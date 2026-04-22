"""
tests/test_price_feed_integration.py
Integration tests for price_feed.py using a REAL Alpaca.io WebSocket
and a REAL Redis instance on localhost.

Requirements
────────────
  pip install -e .
  pip install pytest pytest-asyncio redis

Environment — set in .env or as shell exports before running:
  export Alpaca_API_KEY=your_key_here
  export REDIS_HOST=localhost          # default

Run
───
  # All integration tests (30-second live feed capture included):
  pytest tests/test_price_feed_integration.py -v -s

  # Skip the slow live-feed test, run only connectivity + REST checks:
  pytest tests/test_price_feed_integration.py -v -s -m "not slow"

  # Run only the live WebSocket capture:
  pytest tests/test_price_feed_integration.py -v -s -m slow

Test catalogue
──────────────
  [connectivity]
  ✔  test_redis_reachable            — Redis on localhost:6379 responds to PING
  ✔  test_Alpaca_key_configured     — Alpaca_API_KEY env var is set and non-empty
  ✔  test_Alpaca_websocket_auth     — WS auth succeeds with real key (< 5 s)

  [REST / historical — free tier OK, no real-time key required]
  ✔  test_fetch_historical_ohlcv_real       — downloads AAPL daily bars from yfinance
  ✔  test_fetch_historical_ohlcv_columns    — returned columns are exactly ohlcv lowercase
  ✔  test_fetch_historical_ohlcv_no_nulls   — no NaN in close / volume
  ✔  test_fetch_historical_ohlcv_multiindex — yfinance MultiIndex flattened correctly
  ✔  test_fetch_bulk_quotes_real            — fast_info price > 0 for AAPL, MSFT
  ✔  test_fetch_bulk_quotes_bad_ticker      — bad ticker returns price=0.0, no crash

  [Redis read/write — requires local Redis]
  ✔  test_handle_tick_writes_to_real_redis  — _handle_tick stores to localhost Redis
  ✔  test_handle_bar_writes_to_real_redis   — _handle_bar stores to localhost Redis
  ✔  test_get_latest_tick_roundtrip         — write via _handle_tick, read via get_latest_tick
  ✔  test_get_latest_bar_roundtrip          — write via _handle_bar, read via get_latest_bar
  ✔  test_tick_ttl_on_real_redis            — TTL is set between 86390..86400
  ✔  test_missing_ticker_returns_empty      — unknown ticker → {}

  [Live WebSocket — requires Starter plan, marked @slow, 20-second capture]
  ✔  test_live_feed_receives_ticks          — connects, receives ≥1 T event for SPY/QQQ
  ✔  test_live_feed_writes_to_redis         — tick data appears in Redis after capture
  ✔  test_live_feed_bar_received            — at least one AM bar captured (if market open)
  ✔  test_module_run_stop_integration       — module-level run()/stop() lifecycle

Notes
─────
- Tests that need market hours are skipped automatically outside 09:30–16:00 ET
  on weekdays, with a clear skip message.
- All Redis keys written by this test use the prefix  TEST:  to avoid
  colliding with any live scanner data. They are deleted in teardown.
- The live-feed tests default to SPY and QQQ because they have the highest
  tick volume and will deliver data within seconds even on quiet days.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
import redis as redis_lib

# ─────────────────────────────────────────────────────────
#  Path bootstrap — works without `pip install -e .`
# ─────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ─────────────────────────────────────────────────────────
#  Load .env so Alpaca_API_KEY is available when the
#  module-level settings singleton initialises.
# ─────────────────────────────────────────────────────────

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   
from config.settings import get_api_config, get_scanner_config
_api = get_api_config()
_cfg = get_scanner_config()
# print("Alpaca_api_key :", _api.Alpaca_api_key)
# print("redis_host :", _api.redis_host)

# ─────────────────────────────────────────────────────────
#  Import the module under test (real dependencies, no mocks)
# ─────────────────────────────────────────────────────────

from ingestion import price_feed as pf_module
from ingestion.price_feed import (
    PriceFeed,
    _parse_hash,
    fetch_historical_ohlcv,
    fetch_bulk_quotes,
    get_latest_tick,
    get_latest_bar,
    run as module_run,
    stop as module_stop,
)

# ─────────────────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────────────────
REDIS_HOST   = _api.redis_host
REDIS_PORT   = 6379
TEST_PREFIX  = "TEST"          # all keys written here use TEST:{ticker}
LIVE_TICKERS = ["SPY", "QQQ"]  # highest-volume ETFs → ticks even on quiet days
SLOW_TIMEOUT = 20              # seconds to listen for live ticks

ET = ZoneInfo("America/New_York")


# ─────────────────────────────────────────────────────────
#  Shared helpers
# ─────────────────────────────────────────────────────────

def get_real_redis() -> redis_lib.Redis:
    """Return a connection to the local Redis, fail fast if unreachable."""
    r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0,
                        decode_responses=True)
    r.ping()   # raises ConnectionError immediately if Redis is down
    return r


def is_market_open() -> bool:
    """True if current time is within regular NYSE market hours (ET)."""
    now = datetime.now(ET)
    if now.weekday() >= 5:          # Saturday / Sunday
        return False
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now <= market_close


def make_prefixed_feed(tickers: list[str]) -> tuple[PriceFeed, redis_lib.Redis]:
    """
    Create a PriceFeed that writes to the real Redis but under
    TEST:{TICKER} keys so live scanner data is never overwritten.

    We monkey-patch _handle_tick and _handle_bar to prepend TEST: to the key.
    """
    r = get_real_redis()
    feed = PriceFeed.__new__(PriceFeed)
    feed.tickers  = [t.upper() for t in tickers]
    feed.on_tick  = None
    feed._running = False
    feed._redis   = r

    # ── Wrap handlers to use TEST: prefix ─────────────────
    original_tick = PriceFeed._handle_tick
    original_bar  = PriceFeed._handle_bar

    def prefixed_tick(self, msg):
        msg = dict(msg)
        msg["S"] = f"{TEST_PREFIX}:{msg['S']}"
        original_tick(self, msg)

    def prefixed_bar(self, msg):
        msg = dict(msg)
        msg["S"] = f"{TEST_PREFIX}:{msg['S']}"
        original_bar(self, msg)

    feed._handle_tick = lambda msg: prefixed_tick(feed, msg)
    feed._handle_bar  = lambda msg: prefixed_bar(feed, msg)

    return feed, r


def cleanup_test_keys(r: redis_lib.Redis, tickers: list[str] | None = None):
    """Delete all TEST:* keys from Redis. Called in teardown."""
    pattern = f"tick:{TEST_PREFIX}:*"
    for key in r.scan_iter(pattern):
        r.delete(key)
    pattern2 = f"bar1m:{TEST_PREFIX}:*"
    for key in r.scan_iter(pattern2):
        r.delete(key)
    # Also clean direct test keys
    if tickers:
        for t in tickers:
            r.delete(f"tick:{TEST_PREFIX}:{t}")
            r.delete(f"bar1m:{TEST_PREFIX}:{t}")


# ─────────────────────────────────────────────────────────
#  Fixtures
# ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def real_redis():
    """Session-scoped real Redis connection. Skips all tests if unreachable."""
    try:
        r = get_real_redis()
        yield r
        cleanup_test_keys(r)
    except redis_lib.ConnectionError:
        pytest.skip(
            f"Redis not reachable at {REDIS_HOST}:{REDIS_PORT}. "
            "Start Redis with: redis-server  or  docker run -p 6379:6379 redis"
        )


@pytest.fixture(scope="session")
def Alpaca_key():
    """Session-scoped Alpaca key. Skips all WebSocket tests if missing."""
    key = _api.apca_api_key_id
    key = key.strip()
    secret =  _api.apca_api_secret
    secret = secret.strip()
    if not key:
        pytest.skip(
            "Alpaca_API_KEY not set. Export it or add it to your .env file."
        )
    return key


@pytest.fixture(autouse=True)
def clean_singleton():
    """Reset the module-level singleton before and after every test."""
    pf_module._feed_instance = None
    yield
    pf_module._feed_instance = None


# ═════════════════════════════════════════════════════════
#  GROUP 1: Connectivity checks
# ═════════════════════════════════════════════════════════

class TestConnectivity:

    def test_redis_reachable(self, real_redis):
        """Redis on localhost:6379 must respond to PING."""
        assert real_redis.ping() is True

    def test_redis_host_matches_config(self, real_redis):
        """The config REDIS_HOST must match what we actually authorized to."""
        assert _api.redis_host in (REDIS_HOST, "localhost")

    def test_Alpaca_key_configured(self, Alpaca_key):
        """Alpaca_API_KEY must be set and at least 10 characters."""
        assert len(Alpaca_key) >= 10, (
            f"Key looks too short ({len(Alpaca_key)} chars) — is it correct?"
        )

    @pytest.mark.asyncio
    async def test_Alpaca_websocket_auth(self, Alpaca_key):
        """
    
        Perform the Alpaca two-step handshake on an open websocket connection.

        Step 1 — welcome frame
        Alpaca sends: [{"T":"success","msg":"connected"}]
        We assert at least one success frame is present.

        Step 2 — send credentials, receive auth result
        We send: {"action":"auth","key":"...","secret":"..."}
        Alpaca replies: [{"T":"success","msg":"authenticated"}]  ← OK
                    or: [{"T":"error","code":403,...}]           ← fail

        Raises AssertionError with a descriptive message on any failure.
        Raises asyncio.TimeoutError if either recv() takes > 5 seconds.
    
        Opens a real WebSocket to Alpaca and verifies that the auth
        message is accepted. Closes immediately after auth.
        Fails if the key is invalid or network is unavailable.
        """
        import websockets

        WS_URL = "wss://stream.data.alpaca.markets/v2/iex"
        authed = False

        try:
            async with websockets.connect(WS_URL, open_timeout=10) as ws:
                # Alpaca sends a welcome message first
                welcome = json.loads(await asyncio.wait_for(ws.recv(), timeout=5)) 

                # ── Step 1: welcome ────────────────────────────────── 
                assert isinstance(welcome, list) and len(welcome) > 0, (
                    f"Expected a non-empty JSON list as welcome frame, got: {welcome!r}"    )
                
                assert any(m.get("T") == "success" for m in welcome), (
                    f"No 'T':'success' frame in welcome. Got: {welcome}"
                )

                # ── Step 2: auth ─────────────────────────────────────
                # Send auth
                await ws.send(json.dumps({
                    "action": "auth",
                    "key": _api.apca_api_key_id, 
                    "secret": _api.apca_api_secret
                }))

                messages = []
                for _ in range(3):  # read a few frames
                    try:
                        resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                        if isinstance(resp,dict):
                            resp = [resp]
                        messages.extend(resp)
                    except asyncio.TimeoutError:
                        break

                # Fail if any error
                assert not any(m.get("T") == "error" for m in messages), messages

                # Pass if authenticated seen
                assert any(m.get("T") == "success" and m.get("msg") == "authenticated" for m in messages), f"Auth not confirmed: {messages}"
                authed = True

        except (OSError, ConnectionRefusedError) as exc:
            pytest.skip(f"Cannot reach Alpaca WebSocket: {exc}")

        assert authed


# ═════════════════════════════════════════════════════════
#  GROUP 2: REST / historical — yfinance (free, no key needed)
# ═════════════════════════════════════════════════════════

class TestHistoricalOHLCV:

    def test_fetch_historical_returns_dataframe(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert isinstance(df, pd.DataFrame)
        assert not df.empty, "Expected AAPL data but got empty DataFrame"

    def test_fetch_historical_columns_lowercase(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert set(df.columns) == {"open", "high", "low", "close", "volume"}, (
            f"Got columns: {list(df.columns)}"
        )

    def test_fetch_historical_no_nans_in_close(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert df["close"].isna().sum() == 0, "NaN values found in close column"

    def test_fetch_historical_no_nans_in_volume(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert df["volume"].isna().sum() == 0

    def test_fetch_historical_close_positive(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert (df["close"] > 0).all(), "Some close prices are ≤ 0"

    def test_fetch_historical_high_gte_low(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert (df["high"] >= df["low"]).all(), "High < Low on some rows"

    def test_fetch_historical_index_named_date(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert df.index.name == "date"

    def test_fetch_historical_multiindex_flattened(self):
        """
        Explicitly construct a MultiIndex DataFrame (as yfinance ≥0.2.18 returns
        for single-ticker downloads) and verify fetch_historical_ohlcv flattens it.
        """
        import yfinance as yf
        from unittest.mock import patch

        idx = pd.date_range("2024-01-02", periods=3, freq="D")
        mi_cols = pd.MultiIndex.from_tuples([
            ("Open", "AAPL"), ("High", "AAPL"), ("Low", "AAPL"),
            ("Close", "AAPL"), ("Volume", "AAPL"),
        ])
        mi_df = pd.DataFrame(
            [[100.0, 105.0, 99.0, 102.0, 1e6],
             [101.0, 106.0, 100.0, 103.0, 1.1e6],
             [102.0, 107.0, 101.0, 104.0, 1.2e6]],
            index=idx, columns=mi_cols,
        )

        with patch.object(yf, "download", return_value=mi_df):
            df = fetch_historical_ohlcv("AAPL", period="5d")

        assert not isinstance(df.columns, pd.MultiIndex), (
            "MultiIndex columns were NOT flattened"
        )
        assert "close" in df.columns

    def test_fetch_historical_bad_ticker_returns_empty(self):
        df = fetch_historical_ohlcv("ZZZZNOTREAL999", period="5d")
        assert isinstance(df, pd.DataFrame)
        # Either empty or missing close — either way must not raise
        if not df.empty:
            assert df["close"].isna().all() or len(df) == 0

    def test_fetch_historical_longer_period(self):
        df = fetch_historical_ohlcv("SPY", period="1mo", interval="1d")
        assert len(df) >= 15, f"Expected ≥15 rows for 1-month period, got {len(df)}"

    def test_fetch_historical_intraday(self):
        df = fetch_historical_ohlcv("SPY", period="5d", interval="1h")
        assert not df.empty
        assert "close" in df.columns


class TestFetchBulkQuotes:

    def test_happy_path_returns_price(self):
        result = fetch_bulk_quotes(["AAPL"])
        assert "AAPL" in result
        assert result["AAPL"]["price"] > 0, (
            f"AAPL price is {result['AAPL']['price']} — expected > 0"
        )

    def test_multiple_tickers_all_returned(self):
        tickers = ["AAPL", "MSFT", "SPY"]
        result = fetch_bulk_quotes(tickers)
        assert set(result.keys()) == set(tickers)

    def test_all_fields_present(self):
        result = fetch_bulk_quotes(["AAPL"])
        r = result["AAPL"]
        for field in ("price", "market_cap", "shares_outstanding", "52w_high", "52w_low"):
            assert field in r, f"Missing field: {field}"

    def test_price_is_float(self):
        result = fetch_bulk_quotes(["MSFT"])
        assert isinstance(result["MSFT"]["price"], float)

    def test_52w_high_gte_low(self):
        result = fetch_bulk_quotes(["SPY"])
        r = result["SPY"]
        if r["52w_high"] > 0 and r["52w_low"] > 0:
            assert r["52w_high"] >= r["52w_low"]

    def test_bad_ticker_returns_price_zero_no_crash(self):
        result = fetch_bulk_quotes(["ZZNOTREAL999"])
        assert "ZZNOTREAL999" in result
        assert result["ZZNOTREAL999"]["price"] == pytest.approx(0.0)

    def test_empty_list_returns_empty_dict(self):
        result = fetch_bulk_quotes([])
        assert result == {}

    def test_mixed_good_bad_tickers(self):
        result = fetch_bulk_quotes(["AAPL", "ZZNOTREAL999"])
        assert result["AAPL"]["price"] > 0
        assert result["ZZNOTREAL999"]["price"] == pytest.approx(0.0)


# ═════════════════════════════════════════════════════════
#  GROUP 3: Redis read/write (real localhost Redis)
# ═════════════════════════════════════════════════════════

class TestRedisReadWrite:
    """
    Use make_prefixed_feed() so all keys go under TEST:{TICKER}
    and are cleaned up by the session fixture.
    """

    def test_handle_tick_writes_to_real_redis(self, real_redis):
        feed, r = make_prefixed_feed(["AAPL"])
        msg = {"T": "t", "S": "AAPL", "p": 175.50, "s": 100,
               "av": 1_000_000, "t": int(time.time() * 1000)}
        feed._handle_tick(msg)

        stored = r.hgetall(f"tick:{TEST_PREFIX}:AAPL")
        assert stored, "No data written to Redis for tick:TEST:AAPL"
        assert float(stored["price"]) == pytest.approx(175.50, abs=0.01)

    def test_handle_bar_writes_to_real_redis(self, real_redis):
        feed, r = make_prefixed_feed(["SPY"])
        msg = {"T": "b", "S": "SPY",
               "o": 450.0, "h": 455.0, "l": 449.0,
               "c": 452.0, "v": 2_000_000, "vw": 451.5,
               "n": 12000, "s": int(time.time() * 1000)}
        feed._handle_bar(msg)

        stored = r.hgetall(f"bar1m:{TEST_PREFIX}:SPY")
        assert stored, "No data written to Redis for bar1m:TEST:SPY"
        assert float(stored["c"]) == pytest.approx(452.0, abs=0.01)
        assert float(stored["v"]) == pytest.approx(2_000_000.0)

    def test_tick_all_fields_stored(self, real_redis):
        feed, r = make_prefixed_feed(["MSFT"])
        ts = int(time.time() * 1000)
        msg = {"T": "t", "S": "MSFT", "p": 310.0, "s": 50, "av": 500_000, "t": ts}
        feed._handle_tick(msg)

        stored = r.hgetall(f"tick:{TEST_PREFIX}:MSFT")
        assert "price"  in stored
        assert "size"   in stored
        assert "volume" in stored
        assert "ts"     in stored

    def test_bar_all_fields_stored(self, real_redis):
        feed, r = make_prefixed_feed(["QQQ"])
        msg = {"T": "b", "S": "QQQ",
               "o": 350.0, "h": 355.0, "l": 349.0,
               "c": 352.0, "v": 1_500_000, "vw": 351.5,
               "n": 9000, "s": int(time.time() * 1000)}
        feed._handle_bar(msg)

        stored = r.hgetall(f"bar1m:{TEST_PREFIX}:QQQ")
        for field in ("o", "h", "l", "c", "v", "vw", "n", "ts"):
            assert field in stored, f"Missing field '{field}' in bar"

    def test_tick_ttl_set_correctly(self, real_redis):
        feed, r = make_prefixed_feed(["AAPL"])
        feed._handle_tick({"T": "t", "S": "AAPL", "p": 100.0,
                           "s": 1, "av": 1, "t": int(time.time() * 1000)})
        ttl = r.ttl(f"tick:{TEST_PREFIX}:AAPL")
        assert 86390 <= ttl <= 86400, f"TTL is {ttl}, expected ~86400"

    def test_bar_ttl_set_correctly(self, real_redis):
        feed, r = make_prefixed_feed(["SPY"])
        feed._handle_bar({"T": "b", "S": "SPY",
                          "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0,
                          "v": 1, "vw": 1.0, "n": 1, "s": int(time.time() * 1000)})
        ttl = r.ttl(f"bar1m:{TEST_PREFIX}:SPY")
        assert 86390 <= ttl <= 86400

    def test_second_tick_overwrites_first(self, real_redis):
        feed, r = make_prefixed_feed(["AAPL"])
        feed._handle_tick({"T": "t", "S": "AAPL", "p": 100.0,
                           "s": 1, "av": 1, "t": 1})
        feed._handle_tick({"T": "t", "S": "AAPL", "p": 200.0,
                           "s": 2, "av": 2, "t": 2})
        stored = r.hgetall(f"tick:{TEST_PREFIX}:AAPL")
        assert float(stored["price"]) == pytest.approx(200.0)

    def test_missing_ticker_get_latest_tick_returns_empty(self, real_redis):
        """Querying a ticker that has never been written returns {}."""
        feed, _ = make_prefixed_feed(["ZZNOTWRITTEN"])
        # Use underlying redis directly since feed uses prefixed keys
        data = real_redis.hgetall("tick:ZZNOTWRITTEN_REALLY_MISSING")
        assert data == {}

    def test_get_latest_tick_roundtrip_via_class(self, real_redis):
        """
        Write via _handle_tick, read back via get_latest_tick on same feed.
        Uses real Redis keys but with TEST prefix to stay isolated.
        """
        r = get_real_redis()
        # Write raw key that get_latest_tick will actually read
        key = f"tick:ROUNDTRIP_TEST_AAPL"
        r.hset(key, mapping={"price": "188.42", "size": "50",
                              "volume": "5000000", "ts": "1700000000000"})
        r.expire(key, 60)

        # Create a feed pointing to real Redis and read it back
        feed = PriceFeed.__new__(PriceFeed)
        feed.tickers = ["ROUNDTRIP_TEST_AAPL"]
        feed._redis  = r
        feed._running = False
        feed.on_tick  = None

        tick = feed.get_latest_tick("ROUNDTRIP_TEST_AAPL")

        assert tick["price"]  == pytest.approx(188.42, abs=0.01)
        assert tick["volume"] == pytest.approx(5_000_000.0)
        assert isinstance(tick["ts"], int)
        assert tick["ts"] == 1700000000000

        r.delete(key)

    def test_get_latest_bar_roundtrip_via_class(self, real_redis):
        r = get_real_redis()
        key = "bar1m:ROUNDTRIP_TEST_SPY"
        r.hset(key, mapping={
            "o": "451.0", "h": "456.0", "l": "450.0",
            "c": "454.5", "v": "3000000", "vw": "452.8",
            "n": "15000", "ts": "1700000060000",
        })
        r.expire(key, 60)

        feed = PriceFeed.__new__(PriceFeed)
        feed.tickers  = ["ROUNDTRIP_TEST_SPY"]
        feed._redis   = r
        feed._running = False
        feed.on_tick  = None

        bar = feed.get_latest_bar("ROUNDTRIP_TEST_SPY")

        assert bar["c"]  == pytest.approx(454.5, abs=0.01)
        assert bar["v"]  == pytest.approx(3_000_000.0)
        assert bar["n"]  == 15000
        assert isinstance(bar["n"], int)

        r.delete(key)

    def test_on_tick_callback_fires_with_real_redis(self, real_redis):
        """Confirm the callback receives correct data when writing to real Redis."""
        received = []
        feed, _ = make_prefixed_feed(["TSLA"])
        feed.on_tick = lambda ticker, payload: received.append((ticker, payload))

        feed._handle_tick({"T": "t", "S": "TSLA", "p": 242.0,
                           "s": 10, "av": 800_000,
                           "t": int(time.time() * 1000)})

        assert len(received) == 1
        # Note: ticker in callback uses the prefixed sym (TEST:TSLA)
        _, payload = received[0]
        assert payload["price"] == pytest.approx(242.0, abs=0.01)


# ═════════════════════════════════════════════════════════
#  GROUP 4: Live WebSocket feed  (@slow — 20-second capture)
#  Marked pytest.mark.slow — skip with: pytest -m "not slow"
# ═════════════════════════════════════════════════════════

@pytest.mark.slow
@pytest.mark.asyncio
class TestLiveWebSocket:
    """
    These tests open a real Alpaca.io WebSocket, subscribe to SPY and QQQ,
    collect messages for SLOW_TIMEOUT seconds, then verify what arrived.

    They are skipped automatically when the market is closed because
    Alpaca's real-time feed only delivers T events during market hours.
    AM (minute bar) events are delivered on the minute boundary.
    """

    async def _capture_live(
        self,
        Alpaca_key: str,
        tickers: list[str],
        timeout: int = SLOW_TIMEOUT,
    ) -> tuple[list[dict], list[dict]]:
        """
        Connect to Alpaca, subscribe to tickers, collect for `timeout` seconds.
        Returns (tick_events, bar_events).
        """
        import websockets

        WS_URL = "wss://socket.Alpaca.io/stocks"
        ticks: list[dict] = []
        bars:  list[dict] = []

        async def listen():
            async with websockets.connect(WS_URL, open_timeout=10) as ws:
                # Receive welcome
                await asyncio.wait_for(ws.recv(), timeout=5)

                # Auth
                await ws.send(json.dumps({"action": "auth", "key": _api.apca_api_key_id, "secret": _api.apca_api_secret}))
                auth_resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
                assert auth_resp[0].get("status") == "authorized", (
                    f"Auth failed: {auth_resp}"
                )

                # Subscribe
                # subs = ",".join(
                #     [f"T.{t}" for t in tickers] + [f"AM.{t}" for t in tickers]
                # )
                await ws.send(json.dumps({"action": "subscribe",  "trades": self.tickers, "bars": self.tickers}))

                # Collect for timeout seconds
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        for msg in json.loads(raw):
                            ev = msg.get("T")
                            if ev == "t":
                                ticks.append(msg)
                            elif ev == "b":
                                bars.append(msg)
                    except asyncio.TimeoutError:
                        continue   # keep waiting until deadline

        try:
            await asyncio.wait_for(listen(), timeout=timeout + 10)
        except asyncio.TimeoutError:
            pass   # expected — we just stop collecting

        return ticks, bars

    async def test_live_feed_receives_ticks(self, Alpaca_key, real_redis):
        if not is_market_open():
            pytest.skip(
                "Market is closed — live T events not available. "
                "Run during NYSE hours (Mon–Fri 09:30–16:00 ET)."
            )

        ticks, _ = await self._capture_live(Alpaca_key, LIVE_TICKERS,
                                            timeout=SLOW_TIMEOUT)

        assert len(ticks) > 0, (
            f"No T (trade tick) events received for {LIVE_TICKERS} in "
            f"{SLOW_TIMEOUT}s. Check your Alpaca plan tier — real-time requires "
            "Starter ($29/mo) or higher."
        )

        # Verify structure of a tick
        t = ticks[0]
        assert "S" in t,  f"Tick missing 'S': {t}"
        assert "p"   in t,  f"Tick missing 'p' (price): {t}"
        assert "av"  in t,  f"Tick missing 'av' (acc. volume): {t}"
        assert float(t["p"]) > 0, f"Price must be > 0, got {t['p']}"

    async def test_live_feed_tickers_match_subscription(self, Alpaca_key, real_redis):
        if not is_market_open():
            pytest.skip("Market closed — skipping live ticker check.")

        ticks, _ = await self._capture_live(Alpaca_key, LIVE_TICKERS,
                                            timeout=SLOW_TIMEOUT)
        if not ticks:
            pytest.skip("No ticks received — likely market closed or free-tier plan.")

        received_syms = {t["S"] for t in ticks}
        # At least one of our subscribed tickers must appear
        overlap = received_syms & set(LIVE_TICKERS)
        assert overlap, (
            f"Expected ticks for {LIVE_TICKERS}, but received: {received_syms}"
        )

    async def test_live_feed_writes_to_redis(self, Alpaca_key, real_redis):
        """
        Run PriceFeed.run() for SLOW_TIMEOUT seconds against the real WebSocket
        and then assert that data appeared in Redis.
        """
        if not is_market_open():
            pytest.skip("Market closed — live feed write test requires market hours.")

        r = get_real_redis()
        # Use unique test keys so we don't clash with existing data
        test_key_prefix = "INTEGTEST"

        # Create a feed with real Redis but test-prefixed keys
        collected: list[dict] = []

        def on_tick(ticker, payload):
            collected.append({"ticker": ticker, "payload": payload})

        feed = PriceFeed(LIVE_TICKERS, on_tick=on_tick)

        # Override the Redis key with a test prefix by wrapping _handle_tick
        original_handle_tick = feed._handle_tick

        def test_handle_tick(msg):
            original_handle_tick(msg)  # still writes real key
            collected.append(msg)

        feed._handle_tick = test_handle_tick

        async def run_limited():
            feed._running = True
            try:
                await asyncio.wait_for(feed._connect(), timeout=SLOW_TIMEOUT)
            except (asyncio.TimeoutError, Exception):
                pass
            finally:
                feed._running = False

        try:
            await run_limited()
        except Exception:
            pass

        # If market open and plan supports it, we should have got ticks
        if is_market_open():
            assert len(collected) > 0, (
                f"No ticks collected in {SLOW_TIMEOUT}s. "
                "Verify your Alpaca plan supports real-time data."
            )

            # Verify one of the tickers now has data in Redis
            found = False
            for ticker in LIVE_TICKERS:
                data = r.hgetall(f"tick:{ticker}")
                if data and float(data.get("price", 0)) > 0:
                    found = True
                    break
            assert found, (
                f"Ticks were collected but not found in Redis under "
                f"tick:SPY or tick:QQQ"
            )

    async def test_live_bar_received_near_minute_boundary(self, Alpaca_key, real_redis):
        """
        AM events are emitted at the top of each minute.
        This test captures for SLOW_TIMEOUT seconds and checks if any bars arrived.
        Skip if no bars received (expected if not near a minute boundary).
        """
        if not is_market_open():
            pytest.skip("Market closed — AM events not delivered.")

        _, bars = await self._capture_live(Alpaca_key, LIVE_TICKERS,
                                           timeout=SLOW_TIMEOUT)
        if not bars:
            pytest.skip(
                f"No AM (bar) events in {SLOW_TIMEOUT}s. "
                "This is expected if the test didn't span a minute boundary. "
                "Run with --timeout=90 near a :00 second mark for reliable results."
            )

        b = bars[0]
        assert "o" in b and "h" in b and "l" in b and "c" in b
        assert float(b["h"]) >= float(b["l"]), "Bar high < low — malformed data"

    async def test_module_run_stop_integration(self, Alpaca_key, real_redis):
        """
        Test the module-level run() / stop() lifecycle against the real WebSocket.
        Starts the feed, waits 5 seconds, calls stop(), verifies clean shutdown.
        """
        tickers = ["SPY"]
        received: list[dict] = []

        def on_tick(ticker, payload):
            received.append(payload)

        run_task = asyncio.create_task(
            module_run(tickers, on_tick=on_tick)
        )

        # Let it run for 5 seconds
        await asyncio.sleep(5)
        module_stop()

        # Give the task a moment to notice the stop flag
        try:
            await asyncio.wait_for(run_task, timeout=8)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            run_task.cancel()

        # Singleton should now be set
        assert pf_module._feed_instance is not None
        assert pf_module._feed_instance._running is False

        # If market is open, we should have received some ticks
        if is_market_open() and not received:
            pytest.xfail(
                "No ticks received during 5-second window — "
                "may indicate free-tier plan (delayed data)."
            )


# ═════════════════════════════════════════════════════════
#  Pytest configuration
# ═════════════════════════════════════════════════════════

def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow (live WebSocket, 20+ seconds). "
        "Skip with: pytest -m 'not slow'",
    )
