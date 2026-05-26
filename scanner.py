"""
scanner.py
Real-Time Stock Scanner — main orchestrator.

Wires all five layers into a single async event loop with
APScheduler for periodic refresh tasks.

Usage
─────
  # Run with the default 23-stock universe
  python scanner.py

  # Custom tickers
  python scanner.py --tickers AAPL,TSLA,AMC,GME,NVDA,PLTR

  # Penny stocks only (pre-filters by current price < $5)
  python scanner.py --bucket penny

  # Mid-cap only ($5–$20)
  python scanner.py --bucket midcap

  # Train the ML breakout model before first run (optional but recommended)
  python -m ai.breakout_model train \
      --tickers AAPL,TSLA,NVDA,AMD,MARA,RIOT,COIN --lookback 365

  # Launch the Dash dashboard in a separate terminal
  python -m output.dashboard

  # Backtest the signal strategy
  python -m output.backtest \
      --tickers AAPL,TSLA,NVDA,AMD --period 1y --top-n 3

Setup
─────
  pip install -e .
  cp .env.example .env        # fill in API keys
  redis-server                # or: docker run -d -p 6379:6379 redis:7-alpine
  python scanner.py

Environment variables  (.env)
─────────────────────────────
  POLYGON_API_KEY          Polygon.io Starter plan (real-time WebSocket)
  ALPACA_API_KEY_ID        Alpaca market data (alternative to Polygon)
  ALPACA_API_SECRET_KEY
  ALPACA_FEED              iex (free) or sip (paid)
  FINNHUB_API_KEY          Free tier: 60 req/min
  UNUSUAL_WHALES_TOKEN     Optional — enables real sweep/dark-pool alerts
  DISCORD_WEBHOOK_URL      Optional — enables Discord alerts
  REDIS_HOST               localhost (default)

Architecture — five layers
──────────────────────────
  Layer 1  Ingestion     price_feed, options_feed, news_feed, short_data
  Layer 2  Signals       volume_signals, composite_signals
  Layer 3  AI            composite_scorer, breakout_model, squeeze_detector
  Layer 4  Output        alerts (SQLite + Discord), dashboard (Dash)
  Layer 5  Orchestration scheduler (APScheduler) + asyncio event loop

Key design decisions
────────────────────
  • Reddit and StockTwits run in daemon threads (synchronous HTTP pollers
    that sleep between requests — blocking the event loop would stall ticks).
  • Options chain refresh runs in an executor for the same reason (yfinance
    adds a 2-second sleep between tickers to respect Yahoo's rate limit).
  • RegSHO (FINRA short volume) is fetched once at startup and then hourly;
    it updates at most once per trading day after market close.
  • The Unusual Whales WebSocket is optional; if the token is absent the
    scanner still runs using the yfinance vol/OI proxy for sweeps.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import os  

import threading
import time
from typing import Optional

import redis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

from config.settings import get_api_config, get_path_config, get_scanner_config

# ── Layer 1: Ingestion ────────────────────────────────────────────────────────
from ingestion.price_feed import (
    PriceFeed,
    fetch_bulk_quotes,
)
from ingestion.options_feed import (
    YFinanceOptionsClient,
    UnusualWhalesFeed,
)
from ingestion.news_feed import (
    FinnhubNewsFeed,
    RedditMentionTracker,
    StockTwitsFeed,
)
from ingestion.short_data import ShortDataClient

# ── Layer 2: Signal computation ───────────────────────────────────────────────
from signals.volume_signals import VolumeSignals, TechnicalSignals
from signals.composite_signals import (
    OptionsSignals,
    RiskSignals,
    CatalystSignals,
    MarketContextSignals,
)

# ── Layer 3: AI scoring ───────────────────────────────────────────────────────
from ai.composite_scorer import CompositeScorer, SignalBundle, classify_bucket
from ai.breakout_model import BreakoutModel
from ai.squeeze_detector import SqueezeDetector, FinBERTScorer

# ── Layer 4: Output ───────────────────────────────────────────────────────────
from output.alerts import (
    init_db,
    save_results,
    send_discord_alert,
    send_squeeze_alert,
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   
from config.settings import get_api_config, get_path_config, get_scanner_config

_api   = get_api_config()
_paths = get_path_config()
_cfg   = get_scanner_config()


# ─────────────────────────────────────────────────────────
#  Default ticker universe
# ─────────────────────────────────────────────────────────

DEFAULT_UNIVERSE: list[str] = [
    # Large caps — high options activity and liquidity
    "AAPL", "TSLA", "NVDA", "AMD", "META", "AMZN", "MSFT", "GOOG",
    # Mid-cap momentum / crypto-adjacent
    "MARA", "RIOT", "CLSK", "COIN", "HOOD", "SOFI", "PLTR",
    # Classic squeeze / penny candidates
    "AMC",  "GME",  "SPCE", "MULN", "NKLA",
]


# ─────────────────────────────────────────────────────────
#  StockScanner
# ─────────────────────────────────────────────────────────

class StockScanner:
    """
    Orchestrates all five layers of the stock scanning pipeline.

    The scanner runs as a long-lived asyncio process:
      •  Price ticks arrive via Polygon/Alpaca WebSocket → Redis
      •  APScheduler fires periodic tasks (signals, news, options, etc.)
      •  Each 30-second signal scan computes scores for every ticker,
         runs ML breakout prediction, checks for squeeze setups,
         persists results to SQLite, and fires Discord alerts.
      •  Reddit and StockTwits are polled in background daemon threads.
    """

    def __init__(self, tickers: list[str]) -> None:
        self.tickers: list[str] = [t.upper() for t in tickers]

        self._redis = redis.Redis(
            host=_api.redis_host,
            port=_api.redis_port,
            db=_api.redis_db,
            decode_responses=True,
        )

        # ── Layer 1 ───────────────────────────────────────
        self._price_feed     = PriceFeed(self.tickers, on_tick=self._on_tick)
        self._options_client = YFinanceOptionsClient()
        self._uw_feed        = UnusualWhalesFeed()
        self._news_feed      = FinnhubNewsFeed()
        self._reddit         = RedditMentionTracker(
            tickers_of_interest=self.tickers,
            subreddits=_cfg.reddit_subreddits,
            poll_interval=_cfg.reddit_refresh_interval,
        )
        self._stwits = StockTwitsFeed(
            poll_interval=_cfg.signal_refresh_interval * 5,   # every ~2.5 min
        )
        self._short_client = ShortDataClient()

        # ── Layer 2 ───────────────────────────────────────
        self._vol_sigs:  dict[str, VolumeSignals]    = {t: VolumeSignals(t)    for t in self.tickers}
        self._tech_sigs: dict[str, TechnicalSignals] = {t: TechnicalSignals(t) for t in self.tickers}
        self._opts_sigs  = OptionsSignals()
        self._risk_sigs  = RiskSignals()
        self._cat_sigs   = CatalystSignals()
        self._mkt_sigs   = MarketContextSignals()

        # ── Layer 3 ───────────────────────────────────────
        self._scorer         = CompositeScorer()
        self._breakout_model = BreakoutModel()
        self._squeeze        = SqueezeDetector()
        self._finbert        = FinBERTScorer()

        # ── Internal state ────────────────────────────────
        self._short_profiles: dict = {}
        self._market_ctx:     dict = {}
        self._last_predictions: dict[str, dict] = {}

    # ── Startup ───────────────────────────────────────────

    async def start(self) -> None:
        """
        Bootstrap the scanner and enter the main event loop.

        Startup sequence
        ────────────────
        1.  Initialise the SQLite results database.
        2.  Seed historical OHLCV into signal modules (yfinance, blocking).
        3.  Fetch initial short profiles (4h cache).
        4.  Compute initial market context (VIX, ES futures).
        5.  Pre-fetch options chains for the full universe.
        6.  Fetch one RegSHO batch to seed short-vol ratios.
        7.  Start APScheduler with all periodic tasks.
        8.  Start Reddit and StockTwits in daemon threads.
        9.  Start Unusual Whales WebSocket (if token configured).
        10. Start the price WebSocket — blocks until scanner is stopped.
        """
        logger.info(f"StockScanner starting — {len(self.tickers)} tickers")
        init_db()

        logger.info("Seeding signal history…")
        await self._seed_history()

        logger.info("Loading initial short profiles…")
        await self._refresh_short_data()

        logger.info("Computing initial market context…")
        self._market_ctx = self._mkt_sigs.compute()

        logger.info("Pre-fetching options chains (yfinance)…")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._options_client.fetch_chain_bulk(
                self.tickers, sleep_between=1.5, max_expiries=3
            ),
        )

        logger.info("Fetching FINRA RegSHO batch…")
        svr_map = await loop.run_in_executor(
            None,
            lambda: self._short_client.fetch_regsho_batch(self.tickers),
        )
        for ticker, svr in svr_map.items():
            if ticker in self._short_profiles and svr > 0:
                self._short_profiles[ticker].short_vol_ratio = svr

        # ── APScheduler ───────────────────────────────────
        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            self._run_signal_scan,
            "interval", seconds=_cfg.signal_refresh_interval, id="scan",
        )
        scheduler.add_job(
            self._refresh_news,
            "interval", seconds=_cfg.news_refresh_interval, id="news",
        )
        scheduler.add_job(
            self._refresh_short_data,
            "interval", seconds=14_400, id="short_data",  # 4 hours
        )
        scheduler.add_job(
            self._refresh_market_ctx,
            "interval", seconds=300, id="market_ctx",     # 5 minutes
        )
        scheduler.add_job(
            self._refresh_options,
            "interval", seconds=180, id="options",        # 3 minutes
        )
        scheduler.add_job(
            self._refresh_regsho,
            "interval", seconds=3_600, id="regsho",       # 1 hour
        )
        scheduler.start()
        logger.info("APScheduler running")

        # ── Daemon threads ────────────────────────────────
        self._start_background_threads()

        # ── Async tasks ───────────────────────────────────
        logger.info("Entering main event loop…")
        await asyncio.gather(
            self._price_feed.run(),   # Polygon/Alpaca WebSocket — blocking
            self._run_uw_feed(),      # Unusual Whales WebSocket — optional
        )

    # ── Daemon thread launchers ───────────────────────────

    def _start_background_threads(self) -> None:
        """
        Launch Reddit JSON poller and StockTwits poller as daemon threads.

        Both use synchronous requests.get with inter-request sleeps.
        Running them inside the asyncio event loop would block tick
        processing during those sleeps, so threads are used instead.
        They are daemon threads so they exit automatically when the
        main process exits (Ctrl-C / SIGTERM).
        """
        threading.Thread(
            target=self._reddit.run_poll_loop,
            name="reddit-poller",
            daemon=True,
        ).start()
        logger.info("Reddit JSON poller thread started")

        threading.Thread(
            target=self._stwits.run_poll_loop,
            args=(self.tickers,),
            name="stwits-poller",
            daemon=True,
        ).start()
        logger.info("StockTwits poller thread started")

    # ── History seeding ───────────────────────────────────

    async def _seed_history(self) -> None:
        """Pull historical OHLCV into every VolumeSignals / TechnicalSignals."""
        for ticker in self.tickers:
            try:
                self._vol_sigs[ticker].refresh_history()
                self._tech_sigs[ticker].refresh_history()
            except Exception as exc:
                logger.warning(f"History seed [{ticker}]: {exc}")
        logger.info("History seeding complete")

    # ── Periodic refresh tasks ────────────────────────────

    async def _refresh_short_data(self) -> None:
        logger.debug("Refreshing short profiles…")
        for ticker in self.tickers:
            try:
                self._short_profiles[ticker] = self._short_client.get_short_profile(
                    ticker, include_regsho=False
                )
            except Exception as exc:
                logger.debug(f"Short data [{ticker}]: {exc}")

    async def _refresh_news(self) -> None:
        logger.debug("Refreshing Finnhub news…")
        for ticker in self.tickers:
            try:
                self._news_feed.fetch_company_news(ticker)
            except Exception as exc:
                logger.debug(f"News [{ticker}]: {exc}")

    async def _refresh_market_ctx(self) -> None:
        self._market_ctx = self._mkt_sigs.compute()
        logger.debug(
            f"Market: VIX={self._market_ctx.get('vix', 0):.1f} "
            f"regime={self._market_ctx.get('vix_regime', '?')}"
        )

    async def _refresh_options(self) -> None:
        """Refresh options chains in an executor (yfinance sleeps between tickers)."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._options_client.fetch_chain_bulk(
                self.tickers, sleep_between=2.0, max_expiries=3
            ),
        )
        logger.debug("Options chains refreshed")

    async def _refresh_regsho(self) -> None:
        """Download today's FINRA RegSHO file and update short_vol_ratio."""
        loop = asyncio.get_event_loop()
        svr_map = await loop.run_in_executor(
            None,
            lambda: self._short_client.fetch_regsho_batch(self.tickers),
        )
        updated = 0
        for ticker, svr in svr_map.items():
            if ticker in self._short_profiles and svr > 0:
                self._short_profiles[ticker].short_vol_ratio = svr
                updated += 1
        logger.debug(f"RegSHO: updated {updated}/{len(self.tickers)} short_vol_ratios")

    # ── Tick callback ─────────────────────────────────────

    def _on_tick(self, ticker: str, payload: dict) -> None:
        """
        Called synchronously on every price tick from PriceFeed.
        Must be lightweight — only write the latest price to Redis.
        All heavy computation is deferred to _run_signal_scan.
        """
        try:
            self._redis.set(f"last_price:{ticker}", str(payload["price"]), ex=300)
        except Exception:
            pass   # never crash the WebSocket loop on a Redis write error

    # ── Main signal scan ──────────────────────────────────

    async def _run_signal_scan(self) -> None:
        """
        Core scan — runs every signal_refresh_interval seconds (default 30s).

        For each ticker builds a SignalBundle from all layer-2 modules,
        then runs ML prediction and squeeze detection.  At the end:
          •  Produces a ranked watchlist.
          •  Persists to SQLite.
          •  Fires Discord alerts for top movers / squeeze setups.
        """
        logger.info("Signal scan…")
        t0 = time.monotonic()

        predictions:    dict[str, dict] = {}
        squeeze_alerts: list            = []

        for ticker in self.tickers:
            try:
                bundle = await self._compute_bundle(ticker)
                if bundle is None:
                    continue

                self._scorer.update(bundle)

                # ML breakout prediction
                pred = self._breakout_model.predict(bundle.to_dict())
                if pred:
                    predictions[ticker] = pred.to_dict()

                # Squeeze detection
                short_profile = self._short_profiles.get(ticker)
                if short_profile:
                    tech_cache = self._redis.hgetall(f"tech:{ticker}") or {}
                    ema21      = float(tech_cache.get("ema21", 0) or 0)
                    uw_alerts  = self._uw_feed.get_ticker_alerts(ticker)

                    sq = self._squeeze.evaluate(
                        ticker=ticker,
                        short_profile=short_profile,
                        rvol=bundle.rvol,
                        uw_alerts=uw_alerts,
                        price=bundle.price,
                        ema21=ema21,
                    )
                    if sq and sq.confidence in ("HIGH", "MEDIUM"):
                        squeeze_alerts.append(sq)
                        send_squeeze_alert(sq.to_dict())

            except Exception as exc:
                logger.warning(f"Bundle error [{ticker}]: {exc}")

        # Ranked watchlist
        ranked = self._scorer.get_ranked(top_n=_cfg.top_n_results, min_score=30)

        # Persist to SQLite
        save_results(ranked, predictions)
        self._last_predictions = predictions

        # Discord alerts
        top_flagged = self._scorer.top_alerts(n=3)
        if top_flagged:
            send_discord_alert(
                results=top_flagged,
                predictions=predictions,
                title="⚡ Top Scanner Alerts",
            )

        elapsed = time.monotonic() - t0
        if ranked:
            top = ranked[0]
            logger.info(
                f"Scan {elapsed:.1f}s — {len(ranked)} scored, "
                f"{len(squeeze_alerts)} squeeze, "
                f"top: {top['ticker']} {top['composite_score']}/100"
            )
        else:
            logger.info(f"Scan {elapsed:.1f}s — no results above threshold")

    # ── Bundle computation ────────────────────────────────

    async def _compute_bundle(self, ticker: str) -> Optional[SignalBundle]:
        """
        Assemble a SignalBundle for one ticker.

        Pulls the latest price from Redis (written by _on_tick), then
        computes all layer-2 signals synchronously.  Returns None if no
        valid price is available (pre-market, halted, or not yet traded).
        """
        # Price resolution: live tick → bar close → skip
        price_raw = self._redis.get(f"last_price:{ticker}")
        if price_raw:
            price = float(price_raw)
        else:
            bar   = self._price_feed.get_latest_bar(ticker)
            price = float(bar.get("c", 0.0))

        if price <= 0:
            return None

        bucket        = classify_bucket(price)
        short_profile = self._short_profiles.get(ticker)
        float_shares  = getattr(short_profile, "float_shares", 10_000_000) or 10_000_000

        # Volume
        tick        = self._price_feed.get_latest_tick(ticker)
        current_vol = int(tick.get("volume", 0))
        vol_sigs    = self._vol_sigs[ticker].compute(
            current_volume=current_vol,
            current_price=price,
            float_shares=float_shares,
        )

        # Technical
        tech_sigs = self._tech_sigs[ticker].compute(current_price=price)
        self._redis.hset(f"tech:{ticker}", mapping={"ema21": str(tech_sigs.get("ema21", 0))})
        self._redis.expire(f"tech:{ticker}", 300)

        # Options
        uw_alerts = self._uw_feed.get_ticker_alerts(ticker)
        avg_dv    = max(vol_sigs.get("avg_volume_10d", 1), 1) * price
        opts_sigs = self._opts_sigs.compute(ticker, avg_dv, uw_alerts)

        # Risk / short
        risk_sigs: dict = {}
        if short_profile:
            risk_sigs = self._risk_sigs.compute(
                ticker=ticker,
                short_profile=short_profile,
                atr_pct=tech_sigs.get("atr_pct", 0.02),
            )

        # Catalyst (news + Reddit + StockTwits)
        cat_sigs          = self._cat_sigs.compute(ticker)
        recent_articles   = self._news_feed.get_cached_news(ticker, n=5)
        finbert_sentiment = self._finbert.score_articles(recent_articles)
        stwits_score      = self._stwits.get_sentiment_score(ticker)
        blended_news      = round(0.70 * finbert_sentiment + 0.30 * stwits_score, 3)

        return SignalBundle(
            ticker=ticker,
            price=price,
            bucket=bucket,
            volume_score=vol_sigs.get("score",    0.0),
            technical_score=tech_sigs.get("score", 0.0),
            risk_score=risk_sigs.get("score",      0.0),
            options_score=opts_sigs.get("score",   0.0),
            catalyst_score=cat_sigs.get("score",   0.0),
            market_score=self._market_ctx.get("score", 0.5),
            rvol=vol_sigs.get("rvol",                      0.0),
            short_float_pct=risk_sigs.get("short_float_pct", 0.0),
            float_shares=float_shares,
            rsi14=tech_sigs.get("rsi14",                   50.0),
            bb_squeeze=bool(tech_sigs.get("bb_squeeze",    False)),
            squeeze_candidate=bool(risk_sigs.get("squeeze_candidate", False)),
            news_sentiment=blended_news,
            vix=self._market_ctx.get("vix",                20.0),
        )

    # ── Async task wrappers ───────────────────────────────

    async def _run_uw_feed(self) -> None:
        """Start Unusual Whales WebSocket (no-op if token not configured)."""
        if _api.unusual_whales_token:
            await self._uw_feed.run()
        else:
            logger.warning("UNUSUAL_WHALES_TOKEN not set — UW feed disabled")
            # Hold the coroutine open so gather() doesn't exit immediately
            await asyncio.Event().wait()


# ─────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────

async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Real-Time Stock Scanner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples
  python scanner.py
  python scanner.py --tickers AAPL,TSLA,GME,AMC,NVDA
  python scanner.py --bucket penny
  python scanner.py --bucket midcap --tickers MARA,RIOT,COIN,HOOD,SOFI
        """,
    )
    parser.add_argument(
        "--tickers",
        default=",".join(DEFAULT_UNIVERSE),
        metavar="T1,T2,...",
        help="Comma-separated ticker list (default: 23-stock universe)",
    )
    parser.add_argument(
        "--bucket",
        choices=["penny", "midcap", "largecap", "all"],
        default="all",
        help=(
            "Pre-filter tickers by current price: "
            "penny <$5 | midcap $5–$20 | largecap >$20 | all (default)"
        ),
    )
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    if args.bucket != "all":
        logger.info(f"Pre-filtering to '{args.bucket}' stocks…")
        bucket_ranges = {
            "penny":    (0.10,  5.00),
            "midcap":   (5.01, 20.00),
            "largecap": (20.01, 1e9),
        }
        lo, hi  = bucket_ranges[args.bucket]
        quotes  = fetch_bulk_quotes(tickers)
        tickers = [t for t, q in quotes.items() if lo <= q.get("price", 0.0) <= hi]
        logger.info(f"{len(tickers)} tickers in '{args.bucket}' bucket")

    if not tickers:
        logger.error("No tickers to scan — exiting")
        sys.exit(1)

    await StockScanner(tickers).start()


if __name__ == "__main__":
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
        colorize=True,
    )
    logger.add(
        _paths.logs / "scanner.log",
        level="DEBUG",
        rotation="1 day",
        retention="7 days",
        compression="gz",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} — {message}",
    )
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        logger.info("Scanner stopped (Ctrl+C)")
