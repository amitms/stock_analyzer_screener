"""
ingestion/price_feed.py
Real-time OHLCV tick feed via Alpaca.io WebSocket.

Provides BOTH a class (PriceFeed) AND module-level convenience
functions so callers can do either style:

  # Class style (full control):
  feed = PriceFeed(tickers)
  await feed.run()

  # Module-level style (singleton):
  from stock_scanner.ingestion.price_feed import run, stop, get_latest_tick
  asyncio.run(run(["AAPL", "TSLA"]))

Requires:  Alpaca_API_KEY with Starter plan (real-time).
           Free tier delivers 15-min delayed data only.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Callable, Optional

import pandas as pd
import redis
import websockets
import yfinance as yf
from loguru import logger
import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   
from config.settings import get_api_config, get_scanner_config

_api = get_api_config()
_cfg = get_scanner_config()

WS_URL = "wss://stream.data.alpaca.markets/v2/iex"   #or /iex  /sip
# print(_api)
# print(_cfg)
														   

# ─────────────────────────────────────────────────────────
#  PriceFeed class
# ─────────────────────────────────────────────────────────

class PriceFeed:
    """
    Maintains a persistent WebSocket connection to Alpaca.io.
    Publishes tick data to Redis hashes keyed by ticker symbol.

    Redis schema
    ────────────
    tick:{TICKER}   → {price, size, volume, ts}
    bar1m:{TICKER}  → {o, h, l, c, v, vw, n, ts}
															  
    """

    def __init__(
        self,
        tickers: list[str],
        on_tick: Optional[Callable] = None,
    ):
        self.tickers = [t.upper() for t in tickers]
        self.on_tick = on_tick
        self._redis = redis.Redis(
            host=_api.redis_host,
            port=_api.redis_port,
            db=_api.redis_db,
            decode_responses=True,
        )
        self._running = False

														
				 
    # ── Public API ────────────────────────────────────────

    async def run(self):
        """Start the WebSocket loop with automatic reconnection."""
        self._running = True
        while self._running:
            try:
                await self._connect()
            except Exception as exc:
                logger.warning(f"PriceFeed disconnected ({exc}), retrying in 5s…")
                await asyncio.sleep(5)

    def stop(self):
        self._running = False

    def get_latest_tick(self, ticker: str) -> dict:
        data = self._redis.hgetall(f"tick:{ticker.upper()}")
        if not data:
            return {}
        result = {}
        for k, v in data.items():
            try:
                result[k] = int(v) if k == "ts" else float(v)
            except (ValueError, TypeError):
                result[k] = v
        return result

    def get_latest_bar(self, ticker: str) -> dict:
        data = self._redis.hgetall(f"bar1m:{ticker.upper()}")
        if not data:
            return {}
        result = {}
        for k, v in data.items():
            try:
                result[k] = int(v) if k in ("ts", "n") else float(v)
            except (ValueError, TypeError):
                result[k] = v
        return result

														
			   
    # ── Internal ──────────────────────────────────────────

    async def _connect(self):
        async with websockets.connect(WS_URL, ping_interval=20) as ws:
				  
            await ws.send(json.dumps({"action": "auth", "key": _api.apca_api_key_id, "secret": _api.apca_api_secret}))
            resp = json.loads(await ws.recv())
            # if resp[0].get("status") != "auth_success":
            if not any(r.get("status") == "authorized" for r in resp):
                raise ConnectionError(f"Alpaca auth failed: {resp}")
            logger.info("Alpaca WebSocket authenticated")

			# Subscribe														
            # subs = ",".join(
            #     [f"T.{t}" for t in self.tickers] +
            #     [f"AM.{t}" for t in self.tickers]
            # )
            
            await ws.send(json.dumps({"action": "subscribe", "trades": self.tickers, "bars": self.tickers}))
            logger.info(f"Subscribed to {len(self.tickers)} tickers")
            # Stream Loop
            async for raw in ws:
                for msg in json.loads(raw):
									
                    ev = msg.get("T")
                    # TRADE
                    if ev == "t":
                        self._handle_tick(msg)
                    # BAR
                    elif ev == "b":
                        self._handle_bar(msg)
        """
        Polygon
        ev = "T", "AM"
        sym = "AAPL"
        Alpaca
        T = "t" (trade), "b" (bar)
        S = "AAPL"
        """
    def _handle_tick(self, msg: dict):
        ticker = msg["S"]
        payload = {
            "price":  msg.get("p", 0),
            "size":   msg.get("s", 0),
            "volume": msg.get("av", 0), # 0,  # Alpaca trade feed doesn't always give AV
            "ts":     msg.get("t", int(time.time() * 1000)),
        }
        self._redis.hset(f"tick:{ticker}", mapping=payload)
        self._redis.expire(f"tick:{ticker}", 86400)

        if self.on_tick:
            self.on_tick(ticker, payload)

    def _handle_bar(self, msg: dict):
        ticker = msg["S"]
        payload = {
            "o":  msg.get("o", 0),
            "h":  msg.get("h", 0),
            "l":  msg.get("l", 0),
            "c":  msg.get("c", 0),
            "v":  msg.get("v", 0),
            "vw": msg.get("vw", 0),
            "n":  msg.get("n", 0),
            "ts": msg.get("t", int(time.time() * 1000)),
        }
        self._redis.hset(f"bar1m:{ticker}", mapping=payload)
        self._redis.expire(f"bar1m:{ticker}", 86400)
        logger.debug(f"Bar {ticker}: close={payload['c']:.2f} vol={payload['v']:,}")


# ─────────────────────────────────────────────────────────
#  Module-level singleton + convenience wrappers
# ─────────────────────────────────────────────────────────

_feed_instance: Optional[PriceFeed] = None


def _get_redis() -> redis.Redis:
    return redis.Redis(
        host=_api.redis_host, port=_api.redis_port,
        db=_api.redis_db, decode_responses=True,
    )


def _parse_hash(data: dict, int_keys: tuple = ("ts",)) -> dict:
    result = {}
    for k, v in data.items():
        try:
            result[k] = int(v) if k in int_keys else float(v)
        except (ValueError, TypeError):
            result[k] = v
    return result


async def run(
    tickers: list[str],
    on_tick: Optional[Callable] = None,
) -> None:
    """
    Module-level async entry point — creates a singleton PriceFeed
    and starts the Alpaca WebSocket loop (runs until stop() is called).

    Example
    ───────
    import asyncio
    from stock_scanner.ingestion.price_feed import run

    asyncio.run(run(["AAPL", "TSLA", "NVDA"]))
    """
    global _feed_instance
    _feed_instance = PriceFeed(tickers, on_tick=on_tick)
    await _feed_instance.run()


def stop() -> None:
    """Stop the module-level singleton feed."""
    if _feed_instance:
        _feed_instance.stop()


def get_latest_tick(ticker: str) -> dict:
    """
    Return the latest trade tick for a ticker from Redis.
    Works even if the WebSocket feed is not running (reads cache).
    Returns {} if no data is present.

    Fields: price (float), size (float), volume (float), ts (int ms)
    """
    if _feed_instance:
        return _feed_instance.get_latest_tick(ticker)
    data = _get_redis().hgetall(f"tick:{ticker.upper()}")
    return _parse_hash(data) if data else {}


def get_latest_bar(ticker: str) -> dict:
    """
    Return the latest 1-minute OHLCV bar for a ticker from Redis.
    Returns {} if no data is present.

    Fields: o, h, l, c (float), v (float), vw (float), n (int), ts (int ms)
    """
    if _feed_instance:
        return _feed_instance.get_latest_bar(ticker)
    data = _get_redis().hgetall(f"bar1m:{ticker.upper()}")
    return _parse_hash(data, int_keys=("ts", "n")) if data else {}


# ─────────────────────────────────────────────────────────
#  Historical OHLCV helpers (free, yfinance-based)
# ─────────────────────────────────────────────────────────

def fetch_historical_ohlcv(
    ticker: str,
    period: str = "6mo",
    interval: str = "1d",
) -> pd.DataFrame:
    """
    Pull historical OHLCV from yfinance.

    Always returns a flat-column DataFrame with lowercase column names
    (open, high, low, close, volume). Handles both single-ticker and
    multi-ticker yfinance responses, and both flat and MultiIndex columns.

    Returns an empty DataFrame on any error.
    """
    try:
        raw = yf.download(
            ticker,
            period=period,
            interval=interval,
            progress=False,
            auto_adjust=True,
        )
        if raw.empty:
            return pd.DataFrame()

        # yfinance ≥0.2.18 often returns MultiIndex columns even for
        # a single ticker: ("Close", "AAPL"), ("Open", "AAPL"), …
        # Flatten to just the field name.
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)

        raw.columns = [str(c).lower() for c in raw.columns]
        raw.index.name = "date"

        keep = [c for c in ("open", "high", "low", "close", "volume")
                if c in raw.columns]
        return raw[keep].copy()

    except Exception as exc:
        logger.error(f"fetch_historical_ohlcv({ticker}): {exc}")
        return pd.DataFrame()


def fetch_bulk_quotes(tickers: list[str]) -> dict[str, dict]:
    """
    Snapshot quotes for a list of tickers using yfinance fast_info.
    Useful for pre-filtering the universe by price bucket at startup.

    Returns dict[ticker → {price, market_cap, shares_outstanding,
                            52w_high, 52w_low}]
    """
    results: dict[str, dict] = {}
    for ticker in tickers:
        try:
            info  = yf.Ticker(ticker).fast_info
            price = (getattr(info, "last_price", None)
                     or getattr(info, "regularMarketPrice", 0)
                     or 0)
            results[ticker] = {
                "price":              float(price),
                "market_cap":         float(getattr(info, "market_cap", 0) or 0),
                "shares_outstanding": float(getattr(info, "shares", 0) or 0),
                "52w_high":           float(getattr(info, "year_high", 0) or 0),
                "52w_low":            float(getattr(info, "year_low", 0) or 0),
            }
        except Exception as exc:
            logger.debug(f"fetch_bulk_quotes({ticker}): {exc}")
            results[ticker] = {"price": 0.0}
    return results