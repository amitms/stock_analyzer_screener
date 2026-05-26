"""
Layer 4 - Plotly Dash live dashboard
dashboard.py

output/dashboard.py
Layer 4 — Plotly Dash live dashboard.

Run:  python -m output.dashboard
      Open browser → http://localhost:8050

The dashboard auto-refreshes every 30 seconds and reads the latest
scan results from Redis + SQLite.
"""

from __future__ import annotations

import json
import sqlite3

import pandas as pd
import plotly.graph_objects as go
import redis
from dash import Dash, Input, Output, callback, dcc, html
import dash_bootstrap_components as dbc
from loguru import logger
import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

from config.settings import get_api_config, get_path_config, get_scanner_config

_api   = get_api_config()
_paths = get_path_config()
_cfg   = get_scanner_config()

DB_PATH = _paths.data / "scan_results.db"

_redis = redis.Redis(
    host=_api.redis_host, port=_api.redis_port,
    db=_api.redis_db, decode_responses=True,
)

# ─────────────────────────────────────────────────────────
#  App initialisation
# ─────────────────────────────────────────────────────────

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    title="Stock Scanner",
    update_title=None,
)

# ─────────────────────────────────────────────────────────
#  Layout
# ─────────────────────────────────────────────────────────

SCORE_COLOR_SCALE = [
    [0.0,  "#555"],
    [0.5,  "#f5a623"],
    [1.0,  "#00ff88"],
]

app.layout = dbc.Container(
    fluid=True,
    style={"fontFamily": "monospace", "backgroundColor": "#111"},
    children=[
        # Header
        dbc.Row([
            dbc.Col(html.H2("⚡ Real-Time Stock Scanner", style={"color": "#00ff88"}), width=8),
            dbc.Col([
                dbc.Switch(id="penny-filter",  label="Penny (<$5)",     value=False),
                dbc.Switch(id="squeeze-filter", label="Squeeze Only",   value=False),
            ], width=4, className="d-flex gap-4 align-items-center justify-content-end"),
        ], className="mt-3 mb-2"),

        # Market context bar
        dbc.Row([
            dbc.Col(html.Div(id="market-bar"), width=12),
        ], className="mb-3"),

        # Refresh interval
        dcc.Interval(id="interval", interval=30_000, n_intervals=0),

        # Watchlist table
        dbc.Row([
            dbc.Col(html.Div(id="watchlist-table"), width=8),
            dbc.Col(html.Div(id="detail-panel"), width=4),
        ]),

        # Score distribution chart
        dbc.Row([
            dbc.Col(dcc.Graph(id="score-chart", style={"height": "320px"}), width=6),
            dbc.Col(dcc.Graph(id="rvol-chart",  style={"height": "320px"}), width=6),
        ], className="mt-3"),

        # Reddit mentions
        dbc.Row([
            dbc.Col(dcc.Graph(id="reddit-chart", style={"height": "280px"}), width=12),
        ], className="mt-2 mb-4"),
    ],
)


# ─────────────────────────────────────────────────────────
#  Data helpers
# ─────────────────────────────────────────────────────────

def load_latest_scan() -> pd.DataFrame:
    """Load most recent scan run from SQLite."""
    if not DB_PATH.exists():
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(DB_PATH)
        # Most recent scan timestamp
        ts_row = conn.execute(
            "SELECT MAX(scan_ts) FROM scan_results"
        ).fetchone()
        if not ts_row or ts_row[0] is None:
            conn.close()
            return pd.DataFrame()
        latest_ts = ts_row[0]
        df = pd.read_sql(
            "SELECT * FROM scan_results WHERE scan_ts = ?",
            conn, params=(latest_ts,)
        )
        conn.close()
        df["flags"] = df["flags"].apply(
            lambda x: json.loads(x) if isinstance(x, str) else []
        )
        return df.sort_values("composite_score", ascending=False)
    except Exception as exc:
        logger.warning(f"Dashboard DB load error: {exc}")
        return pd.DataFrame()


def get_vix() -> float:
    raw = _redis.hgetall("tick:^VIX")
    return float(raw.get("price", 20.0)) if raw else 20.0


def get_reddit_top() -> list[tuple[str, int]]:
    raw = _redis.zrevrange("reddit:mentions", 0, 9, withscores=True)
    return [(t, int(s)) for t, s in raw]


# ─────────────────────────────────────────────────────────
#  Callbacks
# ─────────────────────────────────────────────────────────

@callback(
    Output("market-bar", "children"),
    Input("interval", "n_intervals"),
)
def update_market_bar(_):
    vix = get_vix()
    color = "#ff4444" if vix > 30 else "#f5a623" if vix > 20 else "#00ff88"
    return dbc.Alert(
        f"📊 VIX: {vix:.1f}   |   Auto-refresh: 30s",
        color="secondary",
        style={"backgroundColor": "#1a1a2e", "color": color, "fontWeight": "bold"},
    )


@callback(
    Output("watchlist-table", "children"),
    Input("interval", "n_intervals"),
    Input("penny-filter", "value"),
    Input("squeeze-filter", "value"),
)
def update_table(_, penny_only, squeeze_only):
    df = load_latest_scan()
    if df.empty:
        return dbc.Alert("No scan results yet. Run the scanner to populate.", color="warning")

    if penny_only:
        df = df[df["bucket"] == "penny"]
    if squeeze_only:
        df = df[df["squeeze_candidate"] == 1]

    df = df.head(20)

    header = dbc.Row([
        dbc.Col("Ticker",    width=1, style={"color": "#aaa", "fontSize": "12px"}),
        dbc.Col("Price",     width=1, style={"color": "#aaa", "fontSize": "12px"}),
        dbc.Col("Score",     width=1, style={"color": "#aaa", "fontSize": "12px"}),
        dbc.Col("RVOL",      width=1, style={"color": "#aaa", "fontSize": "12px"}),
        dbc.Col("RSI",       width=1, style={"color": "#aaa", "fontSize": "12px"}),
        dbc.Col("SI%",       width=1, style={"color": "#aaa", "fontSize": "12px"}),
        dbc.Col("Float M",   width=1, style={"color": "#aaa", "fontSize": "12px"}),
        dbc.Col("Flags",     width=4, style={"color": "#aaa", "fontSize": "12px"}),
    ], className="px-2 py-1 mb-1 border-bottom")

    rows = [header]
    for _, row in df.iterrows():
        score = int(row.get("composite_score", 0))
        score_color = (
            "#00ff88" if score >= 70 else
            "#f5a623" if score >= 45 else
            "#888"
        )
        squeeze_mark = "🩳" if row.get("squeeze_candidate") else ""
        flags = row.get("flags", [])
        flag_str = " ".join(flags[:2]) if flags else "-"

        rows.append(dbc.Row([
            dbc.Col(f"{row['ticker']}{squeeze_mark}", width=1,
                    style={"color": "#fff", "fontWeight": "bold"}),
            dbc.Col(f"${row.get('price', 0):.2f}", width=1, style={"color": "#ccc"}),
            dbc.Col(str(score), width=1, style={"color": score_color, "fontWeight": "bold"}),
            dbc.Col(f"{row.get('rvol', 0):.1f}×", width=1,
                    style={"color": "#f5a623" if row.get("rvol", 0) >= 3 else "#aaa"}),
            dbc.Col(f"{row.get('rsi14', 0):.0f}", width=1, style={"color": "#ccc"}),
            dbc.Col(f"{row.get('short_float_pct', 0):.1f}%", width=1, style={"color": "#ccc"}),
            dbc.Col(f"{row.get('float_shares_M', 0):.1f}", width=1, style={"color": "#ccc"}),
            dbc.Col(flag_str, width=4, style={"color": "#f5a623", "fontSize": "11px"}),
        ], className="px-2 py-1 border-bottom",
            style={"backgroundColor": "#1a1a1a" if _ % 2 == 0 else "#111"}))

    return html.Div(rows, style={
        "backgroundColor": "#161616",
        "borderRadius": "8px",
        "padding": "8px",
    })


@callback(
    Output("score-chart", "figure"),
    Input("interval", "n_intervals"),
)
def update_score_chart(_):
    df = load_latest_scan()
    if df.empty:
        return go.Figure()

    top = df.head(15)
    colors = [
        "#00ff88" if s >= 70 else "#f5a623" if s >= 45 else "#888"
        for s in top["composite_score"]
    ]

    fig = go.Figure(go.Bar(
        x=top["ticker"],
        y=top["composite_score"],
        marker_color=colors,
        text=top["composite_score"],
        textposition="outside",
    ))
    fig.update_layout(
        paper_bgcolor="#111", plot_bgcolor="#111",
        font_color="#ccc", title="Composite Scores (top 15)",
        xaxis_tickangle=-45, yaxis_range=[0, 105],
        margin=dict(l=20, r=20, t=40, b=60),
    )
    return fig


@callback(
    Output("rvol-chart", "figure"),
    Input("interval", "n_intervals"),
)
def update_rvol_chart(_):
    df = load_latest_scan()
    if df.empty:
        return go.Figure()

    df_rvol = df.nlargest(15, "rvol")
    fig = go.Figure(go.Bar(
        x=df_rvol["ticker"],
        y=df_rvol["rvol"],
        marker_color=["#ff4444" if v >= 5 else "#f5a623" if v >= 3 else "#888"
                      for v in df_rvol["rvol"]],
        text=[f"{v:.1f}×" for v in df_rvol["rvol"]],
        textposition="outside",
    ))
    fig.update_layout(
        paper_bgcolor="#111", plot_bgcolor="#111",
        font_color="#ccc", title="RVOL — Top 15",
        xaxis_tickangle=-45,
        margin=dict(l=20, r=20, t=40, b=60),
    )
    return fig


@callback(
    Output("reddit-chart", "figure"),
    Input("interval", "n_intervals"),
)
def update_reddit_chart(_):
    mentions = get_reddit_top()
    if not mentions:
        return go.Figure()
    tickers, counts = zip(*mentions)
    fig = go.Figure(go.Bar(
        x=tickers, y=counts,
        marker_color="#7b6cf6",
        text=counts, textposition="outside",
    ))
    fig.update_layout(
        paper_bgcolor="#111", plot_bgcolor="#111",
        font_color="#ccc", title="Reddit Mentions (last hour)",
        margin=dict(l=20, r=20, t=40, b=40),
    )
    return fig


# ─────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Starting Dash dashboard on http://localhost:8050")
    app.run(debug=False, host="0.0.0.0", port=8050)
	
# Main orchestrator - the top-level scanner loop that wires all layers together
# scanner.py

"""
scanner.py
Main orchestrator — wires all five layers into a single
async event loop with APScheduler for timed tasks.

Usage
─────
  # Run live scanner
  python scanner.py

  # Scan a custom ticker list
  python scanner.py --tickers AAPL,TSLA,AMC,GME,BBBY

  # Penny stocks only
  python scanner.py --bucket penny

  # Train the ML model first
  python -m ai.breakout_model train --tickers AAPL,TSLA,NVDA,AMD --lookback 365

Environment
───────────
  Copy .env.example to .env and fill in API keys before running.
"""

# from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from typing import Optional

import redis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger
import sys
import os  
		 
# Layer 1 — ingestion
from ingestion.price_feed import PriceFeed, fetch_bulk_quotes
from ingestion.options_feed import TradierOptionsClient, UnusualWhalesFeed
from ingestion.news_feed import FinnhubNewsFeed, RedditMentionTracker
from ingestion.short_data import ShortDataClient

# Layer 2 — signals
from signals.volume_signals import VolumeSignals, TechnicalSignals
from signals.composite_signals import (
    OptionsSignals, RiskSignals, CatalystSignals, MarketContextSignals,
)

# Layer 3 — AI
from ai.composite_scorer import CompositeScorer, SignalBundle, classify_bucket
from ai.breakout_model import BreakoutModel
from ai.squeeze_detector import SqueezeDetector, FinBERTScorer

# Layer 4 — output
from output.alerts import save_results, send_discord_alert, send_squeeze_alert, init_db

_api   = get_api_config()
_cfg   = get_scanner_config()
_paths = get_path_config()

# ─────────────────────────────────────────────────────────
#  Default universe
# ─────────────────────────────────────────────────────────

DEFAULT_UNIVERSE = [
    # Large caps (high options activity)
    "AAPL", "TSLA", "NVDA", "AMD", "META", "AMZN", "MSFT", "GOOG",
    # Mid cap / momentum
    "MARA", "RIOT", "CLSK", "COIN", "HOOD", "SOFI", "PLTR",
    # Typical short-squeeze / penny candidates
    "AMC",  "GME",  "BBBY", "SPCE", "MULN", "NKLA",
]


# ─────────────────────────────────────────────────────────
#  Scanner engine
# ─────────────────────────────────────────────────────────

class StockScanner:

    def __init__(self, tickers: list[str]):
        self.tickers = [t.upper() for t in tickers]

        # Shared Redis client
        self._redis = redis.Redis(
            host=_api.redis_host, port=_api.redis_port,
            db=_api.redis_db, decode_responses=True,
        )

        # Layer 1
        self._price_feed    = PriceFeed(self.tickers, on_tick=self._on_tick)
        self._options_client = TradierOptionsClient()
        self._uw_feed       = UnusualWhalesFeed()
        self._news_feed     = FinnhubNewsFeed()
        self._reddit        = RedditMentionTracker(self.tickers)
        self._short_client  = ShortDataClient()

        # Layer 2
        self._vol_signals  = {t: VolumeSignals(t) for t in self.tickers}
        self._tech_signals = {t: TechnicalSignals(t) for t in self.tickers}
        self._opts_signals  = OptionsSignals()
        self._risk_signals  = RiskSignals()
        self._cat_signals   = CatalystSignals()
        self._mkt_signals   = MarketContextSignals()

        # Layer 3
        self._scorer          = CompositeScorer()
        self._breakout_model  = BreakoutModel()
        self._squeeze_detect  = SqueezeDetector()
        self._finbert         = FinBERTScorer()

        # State
        self._last_predictions: dict[str, dict] = {}
        self._market_ctx: dict = {}
        self._short_profiles: dict = {}

    # ──────────────────────────────────────────────────────
    #  Startup
    # ──────────────────────────────────────────────────────

    async def start(self):
        logger.info(f"Scanner starting — universe: {len(self.tickers)} tickers")
        init_db()

        # Seed historical data for signal modules
        await self._seed_history()

        # Fetch short profiles (cached 4h)
        await self._refresh_short_data()

        # Initial market context
        self._market_ctx = self._mkt_signals.compute()

        # APScheduler for periodic tasks
        scheduler = AsyncIOScheduler()
        scheduler.add_job(self._run_signal_scan,    "interval",
                          seconds=_cfg.signal_refresh_interval,  id="signals")
        scheduler.add_job(self._refresh_news,       "interval",
                          seconds=_cfg.news_refresh_interval,    id="news")
        scheduler.add_job(self._refresh_short_data, "interval",
                          seconds=14400,                          id="short")  # 4h
        scheduler.add_job(self._refresh_market_ctx, "interval",
                          seconds=300,                            id="market") # 5m
        scheduler.add_job(self._refresh_options,    "interval",
                          seconds=120,                            id="options") # 2m
        scheduler.start()

        # Launch async tasks concurrently
        await asyncio.gather(
            self._price_feed.run(),           # WebSocket (blocking)
            self._run_uw_feed(),              # Unusual Whales WebSocket
            self._run_reddit_stream(),        # Reddit PRAW stream
        )

    # ──────────────────────────────────────────────────────
    #  Seeding
    # ──────────────────────────────────────────────────────

    async def _seed_history(self):
        logger.info("Seeding historical OHLCV for signal modules…")
        for ticker in self.tickers:
            self._vol_signals[ticker].refresh_history()
            self._tech_signals[ticker].refresh_history()
        logger.info("History seeded")

    # ──────────────────────────────────────────────────────
    #  Periodic refresh tasks
    # ──────────────────────────────────────────────────────

    async def _refresh_short_data(self):
        logger.debug("Refreshing short profiles…")
        for ticker in self.tickers:
            try:
                profile = self._short_client.get_short_profile(ticker)
                self._short_profiles[ticker] = profile
            except Exception as exc:
                logger.debug(f"Short data failed {ticker}: {exc}")

    async def _refresh_news(self):
        logger.debug("Refreshing news…")
        for ticker in self.tickers:
            try:
                self._news_feed.fetch_company_news(ticker, lookback_hours=24)
            except Exception as exc:
                logger.debug(f"News refresh {ticker}: {exc}")

    async def _refresh_market_ctx(self):
        self._market_ctx = self._mkt_signals.compute()
        logger.debug(f"Market context: VIX={self._market_ctx.get('vix'):.1f} "
                     f"regime={self._market_ctx.get('vix_regime')}")

    async def _refresh_options(self):
        logger.debug("Refreshing options chains…")
        for ticker in self.tickers:
            try:
                await self._options_client.fetch_chain(ticker)
            except Exception as exc:
                logger.debug(f"Options refresh {ticker}: {exc}")

    # ──────────────────────────────────────────────────────
    #  Tick callback (called by PriceFeed on every trade)
    # ──────────────────────────────────────────────────────

    def _on_tick(self, ticker: str, payload: dict):
        """
        Called on every price tick — lightweight, just update Redis state.
        Heavy computation happens in _run_signal_scan on a schedule.
        """
        self._redis.set(f"last_price:{ticker}", payload["price"], ex=300)

    # ──────────────────────────────────────────────────────
    #  Main signal scan (runs every 30s)
    # ──────────────────────────────────────────────────────

    async def _run_signal_scan(self):
        logger.info("Running signal scan…")
        predictions = {}
        squeeze_alerts = []

        for ticker in self.tickers:
            try:
                bundle = await self._compute_bundle(ticker)
                if bundle is None:
                    continue

                self._scorer.update(bundle)

                # ML prediction
                pred = self._breakout_model.predict(bundle.to_dict())
                if pred:
                    predictions[ticker] = pred.to_dict()

                # Squeeze check
                short_profile = self._short_profiles.get(ticker)
                if short_profile:
                    uw_alerts = self._uw_feed.get_ticker_alerts(ticker)
                    tech = self._redis.hgetall(f"tech:{ticker}") or {}
                    ema21 = float(tech.get("ema21", 0) or 0)

                    sq_alert = self._squeeze_detect.evaluate(
                        ticker=ticker,
                        short_profile=short_profile,
                        rvol=bundle.rvol,
                        uw_alerts=uw_alerts,
                        price=bundle.price,
                        ema21=ema21,
                    )
                    if sq_alert and sq_alert.confidence in ("HIGH", "MEDIUM"):
                        squeeze_alerts.append(sq_alert)
                        send_squeeze_alert(sq_alert.to_dict())

            except Exception as exc:
                logger.warning(f"Bundle computation failed {ticker}: {exc}")

        # Produce ranked output
        ranked = self._scorer.get_ranked(
            top_n=_cfg.top_n_results,
            min_score=30,
        )

        # Save to DB
        save_results(ranked, predictions)
        self._last_predictions = predictions

        # Discord alert for top movers
        top_flagged = self._scorer.top_alerts(n=3)
        if top_flagged:
            send_discord_alert(
                results=top_flagged,
                predictions=predictions,
                title="⚡ Top Scanner Alerts",
            )

        logger.info(
            f"Scan complete — {len(ranked)} stocks scored, "
            f"{len(squeeze_alerts)} squeeze setups, "
            f"top: {ranked[0]['ticker']} ({ranked[0]['composite_score']}/100)"
            if ranked else "Scan complete — no results"
        )

    async def _compute_bundle(self, ticker: str) -> Optional[SignalBundle]:
        """Compute all layer-2 signals for a single ticker."""
        # Get latest price from Redis
        price_raw = self._redis.get(f"last_price:{ticker}")
        if not price_raw:
            # Fallback: use latest bar close
            bar = self._price_feed.get_latest_bar(ticker)
            price = bar.get("c", 0.0)
        else:
            price = float(price_raw)

        if price <= 0:
            return None

        # Filter by price threshold
        bucket = classify_bucket(price)
        short_profile = self._short_profiles.get(ticker)
        float_shares = short_profile.float_shares if short_profile else 10_000_000

        # Volume signals
        tick = self._price_feed.get_latest_tick(ticker)
        current_vol = int(tick.get("volume", 0))
        vol_sigs = self._vol_signals[ticker].compute(
            current_volume=current_vol,
            current_price=price,
            float_shares=float_shares,
        )

        # Technical signals
        tech_sigs = self._tech_signals[ticker].compute(current_price=price)
        # Cache ema21 for squeeze detector
        self._redis.hset(f"tech:{ticker}", mapping={
            "ema21": str(tech_sigs.get("ema21", 0))
        })
        self._redis.expire(f"tech:{ticker}", 300)

        # Options signals
        uw_alerts = self._uw_feed.get_ticker_alerts(ticker)
        avg_dv = vol_sigs.get("avg_volume_10d", 1) * price
        opts_sigs = self._opts_signals.compute(ticker, avg_dv, uw_alerts)

        # Risk / short signals
        risk_sigs = {}
        if short_profile:
            risk_sigs = self._risk_signals.compute(
                ticker=ticker,
                short_profile=short_profile,
                atr_pct=tech_sigs.get("atr_pct", 0.02),
            )

        # Catalyst signals
        cat_sigs = self._cat_signals.compute(ticker)

        # FinBERT on latest news (replaces keyword scores)
        recent_articles = self._news_feed.get_cached_news(ticker, n=5)
        finbert_sentiment = self._finbert.score_articles(recent_articles)

        # Build bundle
        bundle = SignalBundle(
            ticker=ticker,
            price=price,
            bucket=bucket,
            volume_score=vol_sigs.get("score", 0.0),
            technical_score=tech_sigs.get("score", 0.0),
            risk_score=risk_sigs.get("score", 0.0),
            options_score=opts_sigs.get("score", 0.0),
            catalyst_score=cat_sigs.get("score", 0.0),
            market_score=self._market_ctx.get("score", 0.5),
            # Raw metrics
            rvol=vol_sigs.get("rvol", 0.0),
            short_float_pct=risk_sigs.get("short_float_pct", 0.0),
            float_shares=float_shares,
            rsi14=tech_sigs.get("rsi14", 50.0),
            bb_squeeze=tech_sigs.get("bb_squeeze", False),
            squeeze_candidate=risk_sigs.get("squeeze_candidate", False),
            news_sentiment=finbert_sentiment,
            vix=self._market_ctx.get("vix", 20.0),
        )
        return bundle

    # ──────────────────────────────────────────────────────
    #  Background async tasks
    # ──────────────────────────────────────────────────────

    async def _run_uw_feed(self):
        if _api.unusual_whales_token:
            await self._uw_feed.run()
        else:
            logger.warning("No UNUSUAL_WHALES_TOKEN — UW feed disabled")

    async def _run_reddit_stream(self):
        if _api.reddit_client_id and _api.reddit_client_secret:
            loop = asyncio.get_event_loop()
            # PRAW is synchronous — run in thread pool
            await loop.run_in_executor(None, self._reddit.run_stream)
        else:
            logger.warning("No Reddit credentials — Reddit stream disabled")


# ─────────────────────────────────────────────────────────
#  CLI entry point
# ─────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Real-Time Stock Scanner")
    parser.add_argument(
        "--tickers", default=",".join(DEFAULT_UNIVERSE),
        help="Comma-separated ticker list (default: 20-stock universe)",
    )
    parser.add_argument(
        "--bucket", choices=["penny", "midcap", "largecap", "all"],
        default="all", help="Pre-filter universe by price bucket",
    )
    args = parser.parse_args()

    tickers = [t.strip().upper() for t in args.tickers.split(",")]

    # If bucket pre-filter requested, narrow universe by current price
    if args.bucket != "all":
        logger.info(f"Pre-filtering to {args.bucket} stocks…")
        quotes = fetch_bulk_quotes(tickers)
        bucket_map = {"penny": (0, 5), "midcap": (5, 20), "largecap": (20, 1e9)}
        lo, hi = bucket_map[args.bucket]
        tickers = [
            t for t, q in quotes.items()
            if lo < q.get("price", 0) <= hi
        ]
        logger.info(f"{len(tickers)} tickers in {args.bucket} bucket")

    if not tickers:
        logger.error("No tickers to scan — exiting")
        sys.exit(1)

    scanner = StockScanner(tickers)
    await scanner.start()


if __name__ == "__main__":
    logger.add(
        _paths.logs / "scanner.log",
        rotation="1 day",
        retention="7 days",
        level="INFO",
    )
    asyncio.run(main())