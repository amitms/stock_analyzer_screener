"""
ingestion/options_feed.py
Options chain ingestion and metrics computation.

Sources
───────
• yfinance   — options chain (free, no API key, no signup required)
               Data is ~15-min delayed during market hours.
               Provides: strikes, bid/ask, IV, OI, volume, in-the-money flag.
• Unusual Whales WebSocket — curated sweep / dark pool alerts (paid, optional)

yfinance replaces Tradier. The public API surface is identical so
composite_signals.py and scanner.py require no changes.

Polling schedule
────────────────
Call fetch_chain() every 2–5 minutes from the scanner scheduler.
Hammering it faster will trigger Yahoo rate-limiting (HTTP 429).
A 2-second sleep between tickers is added automatically in bulk mode.

Redis schema
────────────
options:{TICKER}  → hash   computed metrics, 5-min TTL
uw:sweeps         → list   Unusual Whales sweep alerts (max 500)
uw:darkpool       → list   Unusual Whales dark pool prints (max 500)
uw:ticker:{T}     → list   per-ticker UW alerts (max 50, 24h TTL)
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

import pandas as pd
import redis
import websockets
import yfinance as yf
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
import sys
import os  

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   
from config.settings import get_api_config

_api = get_api_config()

UW_WS_URL = "wss://phx.unusual-whales.com/socket/websocket"

# How long to cache a chain fetch in Redis before it's considered stale
_CACHE_TTL_SECONDS = 300   # 5 minutes


# ─────────────────────────────────────────────────────────
#  Data classes
# ─────────────────────────────────────────────────────────

@dataclass
class SweepCandidate:
    """
    A contract that looks like a sweep: volume > 500 AND vol/OI > 0.5.
    This is a proxy for real sweep detection — true sweeps require
    L1 tape data (e.g. Unusual Whales).
    """
    ticker:      str
    option_type: str        # "call" | "put"
    strike:      float
    expiry:      str
    volume:      int
    open_interest: int
    vol_oi_ratio:  float
    bid:         float
    ask:         float
    last:        float
    iv:          float      # implied volatility (0–N, e.g. 0.45 = 45%)
    in_the_money: bool

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class OptionsMetrics:
    """
    Computed options chain metrics for a single ticker.
    All fields are safe to serialise as Redis hash values (str/float/int).
    """
    ticker:           str
    # ── Volume ──────────────────────────────────────────
    call_volume:      int   = 0
    put_volume:       int   = 0
    total_volume:     int   = 0
    # ── Open interest ───────────────────────────────────
    call_oi:          int   = 0
    put_oi:           int   = 0
    total_oi:         int   = 0
    # ── Ratios ──────────────────────────────────────────
    pc_volume_ratio:  float = 1.0   # put_vol / call_vol  (< 0.7 = bullish)
    pc_oi_ratio:      float = 1.0   # put_oi  / call_oi
    # ── Implied volatility ──────────────────────────────
    avg_iv:           float = 0.0   # mean IV across all contracts
    call_avg_iv:      float = 0.0
    put_avg_iv:       float = 0.0
    iv_skew:          float = 0.0   # put_avg_iv - call_avg_iv
    # ── Sweep proxy ─────────────────────────────────────
    sweep_count:      int   = 0     # contracts with vol/OI > 0.5 AND vol > 500
    call_sweep_count: int   = 0
    put_sweep_count:  int   = 0
    top_sweeps:       str   = "[]"  # JSON list of SweepCandidate dicts (top 5)
    # ── Expiry coverage ─────────────────────────────────
    nearest_expiry:   str   = ""
    expiry_count:     int   = 0
    # ── Metadata ────────────────────────────────────────
    ts:               int   = field(default_factory=lambda: int(time.time()))
    data_source:      str   = "yfinance"

    def to_redis_mapping(self) -> dict[str, str]:
        """Flatten to string values for Redis HSET."""
        return {k: str(v) for k, v in asdict(self).items()}

    @classmethod
    def from_redis(cls, raw: dict) -> "OptionsMetrics":
        """Reconstruct from Redis HGETALL output."""
        def _cast(k, v):
            int_fields   = {"call_volume","put_volume","total_volume",
                            "call_oi","put_oi","total_oi",
                            "sweep_count","call_sweep_count","put_sweep_count",
                            "expiry_count","ts"}
            float_fields = {"pc_volume_ratio","pc_oi_ratio","avg_iv",
                            "call_avg_iv","put_avg_iv","iv_skew"}
            if k in int_fields:
                return int(float(v))
            if k in float_fields:
                return float(v)
            return v

        kwargs = {k: _cast(k, v) for k, v in raw.items() if k in cls.__dataclass_fields__}
        if "ticker" not in kwargs:
            kwargs["ticker"] = raw.get("ticker", "")
        return cls(**kwargs)


# ─────────────────────────────────────────────────────────
#  yfinance options client
# ─────────────────────────────────────────────────────────

class YFinanceOptionsClient:
    """
    Fetches and caches options chain metrics using yfinance.

    Replaces TradierOptionsClient.  The public API is identical:
      • fetch_chain(ticker)       — pull fresh data, compute metrics, cache
      • get_cached_metrics(ticker) — read latest cached metrics from Redis

    Rate-limiting
    ─────────────
    yfinance hits Yahoo's undocumented API. Typical safe limits:
      • 2 000 req/day,  ~2 req/sec burst
    Use fetch_chain_bulk() for scanning many tickers — it adds a
    configurable sleep between requests.

    Data fields returned per contract
    ──────────────────────────────────
    contractSymbol, strike, bid, ask, lastPrice, volume,
    openInterest, impliedVolatility, inTheMoney,
    lastTradeDate, contractSize, currency
    """

    def __init__(self, poll_interval_seconds: int = 120):
        self._redis = redis.Redis(
            host=_api.redis_host,
            port=_api.redis_port,
            db=_api.redis_db,
            decode_responses=True,
        )
        self.poll_interval = poll_interval_seconds

    # ── Public API ────────────────────────────────────────

    def fetch_chain(
        self,
        ticker: str,
        max_expiries: int = 4,
        force_refresh: bool = False,
    ) -> OptionsMetrics:
        """
        Fetch the options chain for `ticker` and return computed metrics.

        Parameters
        ──────────
        max_expiries    — number of near-term expiries to include (default 4).
                          More expiries = richer data but more Yahoo requests.
        force_refresh   — bypass the Redis cache and pull fresh data.

        Returns
        ───────
        OptionsMetrics dataclass.  Metrics are also written to Redis.
        Returns a zeroed OptionsMetrics on any error so callers never crash.
        """
        ticker = ticker.upper()

        if not force_refresh:
            cached = self._load_cache(ticker)
            if cached:
                return cached

        try:
            metrics = self._fetch_and_compute(ticker, max_expiries)
        except Exception as exc:
            logger.warning(f"YFinanceOptions.fetch_chain({ticker}): {exc}")
            metrics = OptionsMetrics(ticker=ticker)

        self._save_cache(metrics)
        return metrics

    def fetch_chain_bulk(
        self,
        tickers: list[str],
        sleep_between: float = 2.0,
        max_expiries: int = 3,
    ) -> dict[str, OptionsMetrics]:
        """
        Fetch options metrics for multiple tickers with rate-limit protection.

        Parameters
        ──────────
        sleep_between   — seconds to sleep between tickers (default 2.0).
                          Keeps requests well under Yahoo's rate limit.
        max_expiries    — expiries per ticker (kept low to reduce request count).

        Returns
        ───────
        dict[ticker → OptionsMetrics]
        """
        results: dict[str, OptionsMetrics] = {}
        for i, ticker in enumerate(tickers):
            results[ticker.upper()] = self.fetch_chain(
                ticker, max_expiries=max_expiries
            )
            if i < len(tickers) - 1:
                time.sleep(sleep_between)
        return results

    def get_cached_metrics(self, ticker: str) -> dict:
        """
        Return the latest cached metrics as a plain dict (for compatibility
        with composite_signals.py which calls this directly).

        Keys: all OptionsMetrics fields.
        Returns {} if no cache entry exists.
        """
        metrics = self._load_cache(ticker.upper())
        if not metrics:
            return {}
        d = asdict(metrics)
        # Deserialise top_sweeps from JSON string back to list
        try:
            d["top_sweeps"] = json.loads(d.get("top_sweeps", "[]"))
        except (json.JSONDecodeError, TypeError):
            d["top_sweeps"] = []
        return d

    # ── Core computation ──────────────────────────────────

    @retry(
        retry=retry_if_exception_type(Exception),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_and_compute(self, ticker: str, max_expiries: int) -> OptionsMetrics:
        """
        Pull chain data from Yahoo and compute all metrics.
        Retried up to 3 times with exponential backoff on transient errors.
        """
        yf_ticker  = yf.Ticker(ticker)
        expirations = yf_ticker.options          # tuple of "YYYY-MM-DD" strings

        if not expirations:
            logger.warning(f"{ticker}: no options expirations available on yfinance")
            return OptionsMetrics(ticker=ticker)

        # Limit to the nearest N expiries to cap Yahoo request count
        selected_expiries = expirations[:max_expiries]

        all_calls: list[pd.DataFrame] = []
        all_puts:  list[pd.DataFrame] = []

        for expiry in selected_expiries:
            try:
                chain = yf_ticker.option_chain(expiry)
                calls = chain.calls.copy()
                puts  = chain.puts.copy()

                # Tag each row with its expiry so sweeps can report it
                calls["expiry"] = expiry
                puts["expiry"]  = expiry

                calls["option_type"] = "call"
                puts["option_type"]  = "put"

                all_calls.append(calls)
                all_puts.append(puts)

            except Exception as exc:
                logger.debug(f"{ticker} expiry {expiry}: {exc}")
                continue

        if not all_calls and not all_puts:
            return OptionsMetrics(ticker=ticker)

        calls_df = pd.concat(all_calls, ignore_index=True) if all_calls else pd.DataFrame()
        puts_df  = pd.concat(all_puts,  ignore_index=True) if all_puts  else pd.DataFrame()

        return self._compute_metrics(ticker, calls_df, puts_df, selected_expiries)

    def _compute_metrics(
        self,
        ticker: str,
        calls: pd.DataFrame,
        puts:  pd.DataFrame,
        expiries: tuple[str, ...],
    ) -> OptionsMetrics:
        """
        Compute all signal metrics from the raw calls/puts DataFrames.

        yfinance column names
        ─────────────────────
        contractSymbol, lastTradeDate, strike, lastPrice, bid, ask,
        change, percentChange, volume, openInterest, impliedVolatility,
        inTheMoney, contractSize, currency
        """

        def _safe_sum(df: pd.DataFrame, col: str) -> int:
            if df.empty or col not in df.columns:
                return 0
            return int(df[col].fillna(0).clip(lower=0).sum())

        def _safe_mean_iv(df: pd.DataFrame) -> float:
            if df.empty or "impliedVolatility" not in df.columns:
                return 0.0
            vals = df["impliedVolatility"].replace(0, pd.NA).dropna()
            return round(float(vals.mean()), 6) if not vals.empty else 0.0

        # ── Volume ───────────────────────────────────────
        call_vol = _safe_sum(calls, "volume")
        put_vol  = _safe_sum(puts,  "volume")
        tot_vol  = call_vol + put_vol

        # ── Open interest ────────────────────────────────
        call_oi = _safe_sum(calls, "openInterest")
        put_oi  = _safe_sum(puts,  "openInterest")
        tot_oi  = call_oi + put_oi

        # ── P/C ratios ───────────────────────────────────
        pc_vol = round(put_vol / call_vol, 4) if call_vol > 0 else 1.0
        pc_oi  = round(put_oi  / call_oi,  4) if call_oi  > 0 else 1.0

        # ── Implied volatility ───────────────────────────
        call_iv  = _safe_mean_iv(calls)
        put_iv   = _safe_mean_iv(puts)
        all_df   = pd.concat([calls, puts], ignore_index=True)
        avg_iv   = _safe_mean_iv(all_df)
        iv_skew  = round(put_iv - call_iv, 6)   # +ve = put IV > call IV (bearish skew)

        # ── Sweep proxy detection ─────────────────────────
        # A contract qualifies when:
        #   volume > 500  (meaningful size, not noise)
        #   volume / openInterest > 0.5  (large fraction of OI traded today)
        # This proxy misses multi-leg sweeps and time-of-sale data.
        sweeps: list[SweepCandidate] = []
        for df, opt_type in ((calls, "call"), (puts, "put")):
            if df.empty:
                continue
            for _, row in df.iterrows():
                vol = int(row.get("volume", 0) or 0)
                oi  = int(row.get("openInterest", 0) or 0)
                if vol < 500 or oi == 0:
                    continue
                vol_oi = vol / oi
                if vol_oi < 0.5:
                    continue
                sweeps.append(SweepCandidate(
                    ticker       = ticker,
                    option_type  = opt_type,
                    strike       = float(row.get("strike", 0) or 0),
                    expiry       = str(row.get("expiry", "")),
                    volume       = vol,
                    open_interest= oi,
                    vol_oi_ratio = round(vol_oi, 3),
                    bid          = float(row.get("bid", 0) or 0),
                    ask          = float(row.get("ask", 0) or 0),
                    last         = float(row.get("lastPrice", 0) or 0),
                    iv           = float(row.get("impliedVolatility", 0) or 0),
                    in_the_money = bool(row.get("inTheMoney", False)),
                ))

        # Sort by volume descending — highest conviction first
        sweeps.sort(key=lambda s: s.volume, reverse=True)
        call_sweeps = [s for s in sweeps if s.option_type == "call"]
        put_sweeps  = [s for s in sweeps if s.option_type == "put"]

        return OptionsMetrics(
            ticker           = ticker,
            call_volume      = call_vol,
            put_volume       = put_vol,
            total_volume     = tot_vol,
            call_oi          = call_oi,
            put_oi           = put_oi,
            total_oi         = tot_oi,
            pc_volume_ratio  = pc_vol,
            pc_oi_ratio      = pc_oi,
            avg_iv           = avg_iv,
            call_avg_iv      = call_iv,
            put_avg_iv       = put_iv,
            iv_skew          = iv_skew,
            sweep_count      = len(sweeps),
            call_sweep_count = len(call_sweeps),
            put_sweep_count  = len(put_sweeps),
            top_sweeps       = json.dumps([s.to_dict() for s in sweeps[:5]]),
            nearest_expiry   = expiries[0] if expiries else "",
            expiry_count     = len(expiries),
            ts               = int(time.time()),
            data_source      = "yfinance",
        )

    # ── Redis cache ───────────────────────────────────────

    def _save_cache(self, metrics: OptionsMetrics) -> None:
        key = f"options:{metrics.ticker}"
        try:
            self._redis.hset(key, mapping=metrics.to_redis_mapping())
            self._redis.expire(key, _CACHE_TTL_SECONDS)
        except Exception as exc:
            logger.debug(f"Cache write failed for {metrics.ticker}: {exc}")

    def _load_cache(self, ticker: str) -> Optional[OptionsMetrics]:
        key = f"options:{ticker}"
        try:
            raw = self._redis.hgetall(key)
            if not raw:
                return None
            return OptionsMetrics.from_redis(raw)
        except Exception as exc:
            logger.debug(f"Cache read failed for {ticker}: {exc}")
            return None

    # ── Convenience queries ───────────────────────────────

    def get_top_sweeps(self, ticker: str, n: int = 5) -> list[dict]:
        """Return the top sweep candidates from the latest cached chain."""
        metrics = self.get_cached_metrics(ticker)
        sweeps  = metrics.get("top_sweeps", [])
        if isinstance(sweeps, str):
            sweeps = json.loads(sweeps)
        return sweeps[:n]

    def get_iv_surface_summary(self, ticker: str) -> dict:
        """
        Return a compact IV summary useful for display / alerting.
        """
        m = self.get_cached_metrics(ticker)
        if not m:
            return {}
        return {
            "ticker":    ticker,
            "avg_iv":    m.get("avg_iv", 0.0),
            "call_iv":   m.get("call_avg_iv", 0.0),
            "put_iv":    m.get("put_avg_iv", 0.0),
            "iv_skew":   m.get("iv_skew", 0.0),
            "pc_vol":    m.get("pc_volume_ratio", 1.0),
            "pc_oi":     m.get("pc_oi_ratio", 1.0),
        }


# ─────────────────────────────────────────────────────────
#  Backward-compatibility alias
# ─────────────────────────────────────────────────────────
# composite_signals.py imports TradierOptionsClient by name.
# This alias keeps that import working without any changes.
TradierOptionsClient = YFinanceOptionsClient


# ─────────────────────────────────────────────────────────
#  Unusual Whales WebSocket (sweep + dark pool alerts)
# ─────────────────────────────────────────────────────────

class UnusualWhalesFeed:
    """
    Connects to the Unusual Whales Phoenix WebSocket and stores
    real-time sweep / dark pool print alerts in Redis lists.

    This is optional — the scanner works without it.  If
    UNUSUAL_WHALES_TOKEN is not set in .env, the feed simply
    does not start (logged as a warning, not an error).

    Redis schema
    ────────────
    uw:sweeps         → list, each element = JSON alert   (max 500)
    uw:darkpool       → list, dark pool prints             (max 500)
    uw:ticker:{T}     → list, per-ticker alerts            (max 50, 24h TTL)
    """

    CHANNEL = "alerts:lobby"

    def __init__(self):
        self._redis = redis.Redis(
            host=_api.redis_host,
            port=_api.redis_port,
            db=_api.redis_db,
            decode_responses=True,
        )
        self._running = False

    # ── Lifecycle ─────────────────────────────────────────

    async def run(self):
        """Start the WebSocket loop with automatic reconnection."""
        if not _api.unusual_whales_token:
            logger.warning(
                "UNUSUAL_WHALES_TOKEN not configured — UW feed disabled. "
                "Options sweep data will rely on the yfinance volume/OI proxy."
            )
            return

        self._running = True
        while self._running:
            try:
                await self._connect()
            except Exception as exc:
                logger.warning(f"UW feed error: {exc}, retrying in 10s…")
                await asyncio.sleep(10)

    def stop(self):
        self._running = False

    # ── WebSocket ─────────────────────────────────────────

    async def _connect(self):
        headers = {"Authorization": f"Bearer {_api.unusual_whales_token}"}
        try:
            async with websockets.connect(UW_WS_URL, additional_headers=headers, open_timeout=30 ) as ws:
                join_msg = {
                    "topic":   self.CHANNEL,
                    "event":   "phx_join",
                    "payload": {},
                    "ref":     "1",
                }
                await ws.send(json.dumps(join_msg))
                logger.info("Unusual Whales WebSocket connected")

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("event") == "new_alert":
                        self._handle_alert(msg.get("payload", {}))
        except asyncio.TimeoutError:
                print("Handshake timed out again.")
    def _handle_alert(self, payload: dict):
        alert_type = payload.get("type", "")
        ticker     = payload.get("ticker", "").upper()
        if not ticker:
            return

        record = {
            "ticker":    ticker,
            "type":      alert_type,
            "premium":   payload.get("total_premium", 0),
            "side":      payload.get("put_call", ""),
            "strike":    payload.get("strike_price"),
            "expiry":    payload.get("expiry_date"),
            "size":      payload.get("size", 0),
            "spot":      payload.get("underlying_price"),
            "sentiment": payload.get("sentiment"),
            "ts":        int(time.time()),
        }

        is_darkpool = any(
            kw in alert_type.lower() for kw in ("dark_pool", "darkpool", "block")
        )

        if is_darkpool:
            self._redis.lpush("uw:darkpool", json.dumps(record))
            self._redis.ltrim("uw:darkpool", 0, 499)
            logger.info(f"Dark pool: {ticker}  ${record['premium']:,.0f}")
        else:
            self._redis.lpush("uw:sweeps", json.dumps(record))
            self._redis.ltrim("uw:sweeps", 0, 499)
            logger.info(
                f"Sweep: {ticker} {record['side'].upper()}"
                f"  ${record['premium']:,.0f}"
            )

        # Per-ticker store for fast lookup in composite_signals.py
        key = f"uw:ticker:{ticker}"
        self._redis.lpush(key, json.dumps(record))
        self._redis.ltrim(key, 0, 49)
        self._redis.expire(key, 86400)

    # ── Read helpers ──────────────────────────────────────

    def get_ticker_alerts(self, ticker: str, n: int = 10) -> list[dict]:
        """Latest N alerts for a specific ticker (sweeps + dark pool)."""
        raw = self._redis.lrange(f"uw:ticker:{ticker.upper()}", 0, n - 1)
        return [json.loads(r) for r in raw]

    def get_recent_sweeps(self, n: int = 20) -> list[dict]:
        """Latest N sweep alerts across all tickers."""
        raw = self._redis.lrange("uw:sweeps", 0, n - 1)
        return [json.loads(r) for r in raw]

    def get_recent_darkpool(self, n: int = 20) -> list[dict]:
        """Latest N dark pool prints across all tickers."""
        raw = self._redis.lrange("uw:darkpool", 0, n - 1)
        return [json.loads(r) for r in raw]