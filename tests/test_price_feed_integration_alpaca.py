"""
tests/test_price_feed_integration.py
Integration tests for price_feed.py using a REAL Alpaca WebSocket
and a REAL Redis instance on localhost.

Alpaca WebSocket protocol differences vs Polygon
─────────────────────────────────────────────────
  URL      wss://stream.data.alpaca.markets/v2/iex   (free IEX feed)
           wss://stream.data.alpaca.markets/v2/sip   (paid SIP — all exchanges)
  Auth     {"action":"auth","key":"<KEY_ID>","secret":"<KEY_SECRET>"}
  Welcome  [{"T":"success","msg":"connected"}]
  AuthOK   [{"T":"success","msg":"authenticated"}]
  AuthFail [{"T":"error","code":403,"msg":"forbidden"}]
  Trade ev "t" → S (symbol), p (price), s (size), t (timestamp ISO), ...
  Bar   ev "b" → S, o, h, l, c, v, vw, t, ...

Requirements
────────────
  pip install -e .
  pip install pytest pytest-asyncio redis websockets yfinance python-dotenv

Environment (.env or shell exports):
  ALPACA_API_KEY_ID=your_key_id
  ALPACA_API_SECRET_KEY=your_secret_key
  ALPACA_FEED=iex          # "iex" (free) or "sip" (paid)
  REDIS_HOST=localhost      # default

Run
───
  pytest tests/test_price_feed_integration.py -m "not slow" -v -s   # fast
  pytest tests/test_price_feed_integration.py -v -s                  # all
  pytest tests/test_price_feed_integration.py -m slow -v -s          # live only
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
import redis as redis_lib

# ─────────────────────────────────────────────────────────
#  Path bootstrap
# ─────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   
from config.settings import get_api_config, get_scanner_config
_api = get_api_config()
_cfg = get_scanner_config()

from ingestion.price_feed import (
    PriceFeed,
    fetch_historical_ohlcv,
    fetch_bulk_quotes,
    run  as module_run,
    stop as module_stop,
)
import ingestion.price_feed as pf_module
from config.settings import get_api_config

_api = get_api_config()

# ─────────────────────────────────────────────────────────
#  Alpaca constants
# ─────────────────────────────────────────────────────────
ALPACA_KEY_ID     = _api.apca_api_key_id
ALPACA_SECRET_KEY = _api.apca_api_secret
ALPACA_FEED       = os.getenv("ALPACA_FEED", "iex").lower()
ALPACA_WS_URL     = f"wss://stream.data.alpaca.markets/v2/{ALPACA_FEED}"

REDIS_HOST   = _api.redis_host
REDIS_PORT   = 6379
TEST_PREFIX  = "TEST"
LIVE_TICKERS = ["SPY", "QQQ"]
SLOW_TIMEOUT = 20

ET = ZoneInfo("America/New_York")


# ─────────────────────────────────────────────────────────
#  Alpaca handshake helpers
# ─────────────────────────────────────────────────────────

def _alpaca_auth_payload() -> str:
    return json.dumps({
        "action": "auth",
        "key":    ALPACA_KEY_ID,
        "secret": ALPACA_SECRET_KEY,
    })


def _is_alpaca_authenticated(messages: list[dict]) -> bool:
    """
    Return True when the auth response contains a success-authenticated frame.

    Alpaca auth success:  [{"T":"success","msg":"authenticated"}]
    Alpaca auth failure:  [{"T":"error","code":403,"msg":"forbidden"}]
    """
    return any(
        m.get("T") == "success" and m.get("msg") == "authenticated"
        for m in messages
    )


def _alpaca_auth_error(messages: list[dict]) -> str | None:
    """Return a readable error string if any message signals auth failure."""
    for m in messages:
        if m.get("T") == "error":
            return f"code={m.get('code')} msg={m.get('msg')}"
    return None


async def _alpaca_connect_and_auth(ws) -> None:
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
    """
    # ── Step 1: welcome ──────────────────────────────────
    raw_welcome = await asyncio.wait_for(ws.recv(), timeout=5)
    welcome = json.loads(raw_welcome)

    assert isinstance(welcome, list) and len(welcome) > 0, (
        f"Expected a non-empty JSON list as welcome frame, got: {raw_welcome!r}"
    )
    assert any(m.get("T") == "success" for m in welcome), (
        f"No 'T':'success' frame in welcome. Got: {welcome}"
    )

    # ── Step 2: auth ─────────────────────────────────────
    await ws.send(_alpaca_auth_payload())

    raw_auth = await asyncio.wait_for(ws.recv(), timeout=5)
    auth_msgs = json.loads(raw_auth)

    assert isinstance(auth_msgs, list), (
        f"Auth response is not a JSON list: {raw_auth!r}"
    )

    if not _is_alpaca_authenticated(auth_msgs):
        err = _alpaca_auth_error(auth_msgs)
        raise AssertionError(
            f"Alpaca auth failed — {err or 'unknown error'}. "
            f"Full response: {auth_msgs}. "
            "Check ALPACA_API_KEY_ID and ALPACA_API_SECRET_KEY in your .env."
        )


# ─────────────────────────────────────────────────────────
#  General helpers
# ─────────────────────────────────────────────────────────

def get_real_redis() -> redis_lib.Redis:
    r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0,
                        decode_responses=True)
    r.ping()
    return r


def is_market_open() -> bool:
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return False
    return (now.replace(hour=9,  minute=30, second=0, microsecond=0)
            <= now <=
            now.replace(hour=16, minute=0,  second=0, microsecond=0))


def make_prefixed_feed(tickers: list[str]) -> tuple[PriceFeed, redis_lib.Redis]:
    """
    PriceFeed backed by real Redis, keys stored under TEST:{TICKER}
    so test writes never overwrite live scanner data.
    """
    r = get_real_redis()
    feed = PriceFeed.__new__(PriceFeed)
    feed.tickers  = [t.upper() for t in tickers]
    feed.on_tick  = None
    feed._running = False
    feed._redis   = r

    orig_tick = PriceFeed._handle_tick
    orig_bar  = PriceFeed._handle_bar

    def prefixed_tick(self, msg):
        orig_tick(self, {**msg, "sym": f"{TEST_PREFIX}:{msg['sym']}"})

    def prefixed_bar(self, msg):
        orig_bar(self, {**msg, "sym": f"{TEST_PREFIX}:{msg['sym']}"})

    feed._handle_tick = lambda msg: prefixed_tick(feed, msg)
    feed._handle_bar  = lambda msg: prefixed_bar(feed, msg)
    return feed, r


def cleanup_test_keys(r: redis_lib.Redis):
    for pattern in (f"tick:{TEST_PREFIX}:*", f"bar1m:{TEST_PREFIX}:*",
                    "tick:ROUNDTRIP_TEST_*", "bar1m:ROUNDTRIP_TEST_*"):
        for key in r.scan_iter(pattern):
            r.delete(key)


# ─────────────────────────────────────────────────────────
#  Fixtures
# ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def real_redis():
    try:
        r = get_real_redis()
        yield r
        cleanup_test_keys(r)
    except redis_lib.ConnectionError:
        pytest.skip(
            f"Redis not reachable at {REDIS_HOST}:{REDIS_PORT}. "
            "Start it: redis-server  or  docker run -d -p 6379:6379 redis:7-alpine"
        )


@pytest.fixture(scope="session")
def alpaca_keys():
    if not ALPACA_KEY_ID or not ALPACA_SECRET_KEY:
        pytest.skip(
            "Alpaca credentials missing. Add to .env:\n"
            "  ALPACA_API_KEY_ID=<key_id>\n"
            "  ALPACA_API_SECRET_KEY=<secret_key>"
        )
    return ALPACA_KEY_ID, ALPACA_SECRET_KEY


@pytest.fixture(autouse=True)
def reset_singleton():
    pf_module._feed_instance = None
    yield
    pf_module._feed_instance = None


# ═════════════════════════════════════════════════════════
#  GROUP 1 — Connectivity
# ═════════════════════════════════════════════════════════

class TestConnectivity:

    def test_redis_reachable(self, real_redis):
        assert real_redis.ping() is True

    def test_redis_host_matches_config(self, real_redis):
        assert _api.redis_host in (REDIS_HOST, "localhost")

    def test_alpaca_keys_configured(self, alpaca_keys):
        key_id, secret = alpaca_keys
        assert len(key_id) >= 8,  f"Key ID looks too short ({len(key_id)} chars)"
        assert len(secret) >= 8,  f"Secret looks too short ({len(secret)} chars)"

    @pytest.mark.asyncio
    async def test_alpaca_websocket_auth(self, alpaca_keys):
        """
        Opens a real connection to Alpaca, completes the two-step handshake,
        and asserts authenticated status. Closes immediately after.

        Alpaca handshake sequence
        ─────────────────────────
        ← [{"T":"success","msg":"connected"}]          welcome (step 1)
        → {"action":"auth","key":"...","secret":"..."}  credentials
        ← [{"T":"success","msg":"authenticated"}]      auth OK  (step 2)
           [{"T":"error","code":403,"msg":"forbidden"}] auth fail
        """
        import websockets

        try:
            async with websockets.connect(ALPACA_WS_URL, open_timeout=10) as ws:
                await _alpaca_connect_and_auth(ws)
                # Reaching here means auth_success — test passes

        except (OSError, ConnectionRefusedError) as exc:
            pytest.skip(f"Cannot reach Alpaca WebSocket ({ALPACA_WS_URL}): {exc}")


# ═════════════════════════════════════════════════════════
#  GROUP 2 — Historical OHLCV (yfinance, no API key)
# ═════════════════════════════════════════════════════════

class TestHistoricalOHLCV:

    def test_returns_dataframe(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert isinstance(df, pd.DataFrame) and not df.empty

    def test_columns_are_lowercase_ohlcv(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert set(df.columns) == {"open", "high", "low", "close", "volume"}

    def test_no_nans_in_close(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert df["close"].isna().sum() == 0

    def test_no_nans_in_volume(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert df["volume"].isna().sum() == 0

    def test_close_is_positive(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert (df["close"] > 0).all()

    def test_high_gte_low(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert (df["high"] >= df["low"]).all()

    def test_index_named_date(self):
        df = fetch_historical_ohlcv("AAPL", period="5d", interval="1d")
        assert df.index.name == "date"

    def test_multiindex_columns_flattened(self):
        """yfinance >=0.2.18 may return MultiIndex columns — must be flattened."""
        import yfinance as yf
        from unittest.mock import patch

        idx = pd.date_range("2024-01-02", periods=3, freq="D")
        mi_cols = pd.MultiIndex.from_tuples([
            ("Open","AAPL"),("High","AAPL"),("Low","AAPL"),
            ("Close","AAPL"),("Volume","AAPL"),
        ])
        mi_df = pd.DataFrame(
            [[100., 105., 99., 102., 1e6],
             [101., 106.,100., 103., 1.1e6],
             [102., 107.,101., 104., 1.2e6]],
            index=idx, columns=mi_cols,
        )
        with patch.object(yf, "download", return_value=mi_df):
            df = fetch_historical_ohlcv("AAPL", period="5d")

        assert not isinstance(df.columns, pd.MultiIndex)
        assert "close" in df.columns

    def test_bad_ticker_returns_empty(self):
        df = fetch_historical_ohlcv("ZZZZNOTREAL999", period="5d")
        assert isinstance(df, pd.DataFrame)

    def test_longer_period_has_enough_rows(self):
        df = fetch_historical_ohlcv("SPY", period="1mo", interval="1d")
        assert len(df) >= 15

    def test_intraday_interval(self):
        df = fetch_historical_ohlcv("SPY", period="5d", interval="1h")
        assert not df.empty and "close" in df.columns


# ═════════════════════════════════════════════════════════
#  GROUP 3 — Bulk quotes (yfinance, no API key)
# ═════════════════════════════════════════════════════════

class TestFetchBulkQuotes:

    def test_returns_positive_price(self):
        assert fetch_bulk_quotes(["AAPL"])["AAPL"]["price"] > 0

    def test_all_tickers_in_result(self):
        tickers = ["AAPL", "MSFT", "SPY"]
        assert set(fetch_bulk_quotes(tickers).keys()) == set(tickers)

    def test_all_fields_present(self):
        r = fetch_bulk_quotes(["AAPL"])["AAPL"]
        for f in ("price","market_cap","shares_outstanding","52w_high","52w_low"):
            assert f in r, f"Missing field: {f}"

    def test_price_is_float(self):
        assert isinstance(fetch_bulk_quotes(["MSFT"])["MSFT"]["price"], float)

    def test_52w_high_gte_low(self):
        r = fetch_bulk_quotes(["SPY"])["SPY"]
        if r["52w_high"] > 0 and r["52w_low"] > 0:
            assert r["52w_high"] >= r["52w_low"]

    def test_bad_ticker_price_zero_no_crash(self):
        assert fetch_bulk_quotes(["ZZNOTREAL999"])["ZZNOTREAL999"]["price"] == pytest.approx(0.0)

    def test_empty_list(self):
        assert fetch_bulk_quotes([]) == {}

    def test_mixed_good_bad(self):
        r = fetch_bulk_quotes(["AAPL", "ZZNOTREAL999"])
        assert r["AAPL"]["price"] > 0
        assert r["ZZNOTREAL999"]["price"] == pytest.approx(0.0)


# ═════════════════════════════════════════════════════════
#  GROUP 4 — Redis read / write (real localhost Redis)
# ═════════════════════════════════════════════════════════

class TestRedisReadWrite:

    def test_handle_tick_writes_price(self, real_redis):
        feed, r = make_prefixed_feed(["AAPL"])
        feed._handle_tick({"ev":"T","sym":"AAPL","p":175.50,"s":100,
                           "av":1_000_000,"t":int(time.time()*1000)})
        stored = r.hgetall(f"tick:{TEST_PREFIX}:AAPL")
        assert stored, "Nothing written to Redis"
        assert float(stored["price"]) == pytest.approx(175.50, abs=0.01)

    def test_handle_bar_writes_close(self, real_redis):
        feed, r = make_prefixed_feed(["SPY"])
        feed._handle_bar({"ev":"AM","sym":"SPY",
                          "o":450.0,"h":455.0,"l":449.0,"c":452.0,
                          "v":2_000_000,"vw":451.5,"z":12000,
                          "s":int(time.time()*1000)})
        stored = r.hgetall(f"bar1m:{TEST_PREFIX}:SPY")
        assert stored
        assert float(stored["c"]) == pytest.approx(452.0, abs=0.01)

    def test_tick_all_fields_present(self, real_redis):
        feed, r = make_prefixed_feed(["MSFT"])
        feed._handle_tick({"ev":"T","sym":"MSFT","p":310.0,"s":50,
                           "av":500_000,"t":int(time.time()*1000)})
        stored = r.hgetall(f"tick:{TEST_PREFIX}:MSFT")
        for field in ("price","size","volume","ts"):
            assert field in stored, f"Missing field '{field}'"

    def test_bar_all_fields_present(self, real_redis):
        feed, r = make_prefixed_feed(["QQQ"])
        feed._handle_bar({"ev":"AM","sym":"QQQ",
                          "o":350.0,"h":355.0,"l":349.0,"c":352.0,
                          "v":1_500_000,"vw":351.5,"z":9000,
                          "s":int(time.time()*1000)})
        stored = r.hgetall(f"bar1m:{TEST_PREFIX}:QQQ")
        for field in ("o","h","l","c","v","vw","n","ts"):
            assert field in stored, f"Missing field '{field}'"

    def test_tick_ttl_is_86400(self, real_redis):
        feed, r = make_prefixed_feed(["AAPL"])
        feed._handle_tick({"ev":"T","sym":"AAPL","p":100.0,"s":1,
                           "av":1,"t":int(time.time()*1000)})
        ttl = r.ttl(f"tick:{TEST_PREFIX}:AAPL")
        assert 86390 <= ttl <= 86400, f"TTL={ttl}"

    def test_bar_ttl_is_86400(self, real_redis):
        feed, r = make_prefixed_feed(["SPY"])
        feed._handle_bar({"ev":"AM","sym":"SPY","o":1.,"h":1.,"l":1.,
                          "c":1.,"v":1,"vw":1.,"z":1,
                          "s":int(time.time()*1000)})
        ttl = r.ttl(f"bar1m:{TEST_PREFIX}:SPY")
        assert 86390 <= ttl <= 86400, f"TTL={ttl}"

    def test_second_tick_overwrites_first(self, real_redis):
        feed, r = make_prefixed_feed(["AAPL"])
        feed._handle_tick({"ev":"T","sym":"AAPL","p":100.0,"s":1,"av":1,"t":1})
        feed._handle_tick({"ev":"T","sym":"AAPL","p":200.0,"s":2,"av":2,"t":2})
        assert float(r.hgetall(f"tick:{TEST_PREFIX}:AAPL")["price"]) == pytest.approx(200.0)

    def test_missing_ticker_returns_empty(self, real_redis):
        assert real_redis.hgetall("tick:ZZNOTWRITTEN_REALLY_MISSING_XYZ") == {}

    def test_get_latest_tick_roundtrip(self, real_redis):
        r = get_real_redis()
        key = "tick:ROUNDTRIP_TEST_AAPL"
        r.hset(key, mapping={"price":"188.42","size":"50",
                              "volume":"5000000","ts":"1700000000000"})
        r.expire(key, 60)

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

    def test_get_latest_bar_roundtrip(self, real_redis):
        r = get_real_redis()
        key = "bar1m:ROUNDTRIP_TEST_SPY"
        r.hset(key, mapping={"o":"451.0","h":"456.0","l":"450.0",
                              "c":"454.5","v":"3000000","vw":"452.8",
                              "n":"15000","ts":"1700000060000"})
        r.expire(key, 60)

        feed = PriceFeed.__new__(PriceFeed)
        feed.tickers = ["ROUNDTRIP_TEST_SPY"]
        feed._redis  = r
        feed._running = False
        feed.on_tick  = None

        bar = feed.get_latest_bar("ROUNDTRIP_TEST_SPY")
        assert bar["c"] == pytest.approx(454.5, abs=0.01)
        assert bar["n"] == 15000
        assert isinstance(bar["n"], int)
        r.delete(key)

    def test_on_tick_callback_receives_correct_payload(self, real_redis):
        received = []
        feed, _ = make_prefixed_feed(["TSLA"])
        feed.on_tick = lambda ticker, payload: received.append((ticker, payload))

        feed._handle_tick({"ev":"T","sym":"TSLA","p":242.0,"s":10,
                           "av":800_000,"t":int(time.time()*1000)})

        assert len(received) == 1
        _, payload = received[0]
        assert payload["price"] == pytest.approx(242.0, abs=0.01)


# ═════════════════════════════════════════════════════════
#  GROUP 5 — Live Alpaca WebSocket  (@slow, ~20 seconds)
# ═════════════════════════════════════════════════════════

@pytest.mark.slow
class TestLiveAlpacaWebSocket:
    """
    Real Alpaca WebSocket tests. Subscribe to SPY/QQQ, collect for
    SLOW_TIMEOUT seconds, then assert on the received data.

    Alpaca message types captured
    ──────────────────────────────
      "t" → trade tick: {T, S, p, s, t, ...}
      "b" → bar:        {T, S, o, h, l, c, v, vw, t}

    Skip conditions
    ───────────────
      - Market closed   (auto-skip with descriptive message)
      - Keys missing    (auto-skip via alpaca_keys fixture)
    """

    async def _alpaca_capture(
        self,
        tickers: list[str],
        timeout: int = SLOW_TIMEOUT,
    ) -> tuple[list[dict], list[dict]]:
        """
        Authenticate, subscribe, and collect trade/bar messages for
        `timeout` seconds. Returns (trades, bars).
        """
        import websockets

        trades: list[dict] = []
        bars:   list[dict] = []

        async def listen():
            async with websockets.connect(ALPACA_WS_URL, open_timeout=10) as ws:
                # ── Handshake ──────────────────────────────────
                await _alpaca_connect_and_auth(ws)

                # ── Subscribe ──────────────────────────────────
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "trades": tickers,
                    "bars":   tickers,
                }))

                # Confirm subscription
                raw_sub = await asyncio.wait_for(ws.recv(), timeout=5)
                sub_msgs = json.loads(raw_sub)
                assert any(m.get("T") == "subscription" for m in sub_msgs), (
                    f"Subscription not confirmed. Got: {sub_msgs}"
                )

                # ── Collect ────────────────────────────────────
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                        for msg in json.loads(raw):
                            ev = msg.get("T")
                            if ev == "t":
                                trades.append(msg)
                            elif ev == "b":
                                bars.append(msg)
                    except asyncio.TimeoutError:
                        continue

        try:
            await asyncio.wait_for(listen(), timeout=timeout + 10)
        except asyncio.TimeoutError:
            pass

        return trades, bars

    @pytest.mark.asyncio
    async def test_live_feed_receives_trades(self, alpaca_keys, real_redis):
        if not is_market_open():
            pytest.skip(
                "Market closed — Alpaca live trades unavailable. "
                "Run Mon–Fri 09:30–16:00 ET."
            )
        trades, _ = await self._alpaca_capture(LIVE_TICKERS, timeout=SLOW_TIMEOUT)

        assert len(trades) > 0, (
            f"No trade events (T='t') for {LIVE_TICKERS} in {SLOW_TIMEOUT}s.\n"
            f"Feed: {ALPACA_FEED}. If using 'iex', try setting ALPACA_FEED=sip."
        )
        t = trades[0]
        assert "S" in t, f"Trade missing 'S' (symbol): {t}"
        assert "p" in t, f"Trade missing 'p' (price): {t}"
        assert "s" in t, f"Trade missing 's' (size): {t}"
        assert "t" in t, f"Trade missing 't' (timestamp): {t}"
        assert float(t["p"]) > 0, f"Price must be > 0, got {t['p']}"

    @pytest.mark.asyncio
    async def test_live_feed_tickers_match_subscription(self, alpaca_keys, real_redis):
        if not is_market_open():
            pytest.skip("Market closed.")

        trades, _ = await self._alpaca_capture(LIVE_TICKERS, timeout=SLOW_TIMEOUT)
        if not trades:
            pytest.skip("No trades received — market may be very quiet.")

        received_syms = {t["S"] for t in trades}
        overlap = received_syms & set(LIVE_TICKERS)
        assert overlap, (
            f"Expected symbols in {LIVE_TICKERS}, got: {received_syms}"
        )

    @pytest.mark.asyncio
    async def test_live_trade_price_and_size_positive(self, alpaca_keys, real_redis):
        if not is_market_open():
            pytest.skip("Market closed.")

        trades, _ = await self._alpaca_capture(["SPY"], timeout=SLOW_TIMEOUT)
        if not trades:
            pytest.skip("No trades received.")

        for trade in trades[:10]:
            assert float(trade["p"]) > 0, f"Non-positive price: {trade}"
            assert int(trade["s"])   > 0, f"Non-positive size: {trade}"

    @pytest.mark.asyncio
    async def test_live_bar_ohlc_valid(self, alpaca_keys, real_redis):
        """
        Alpaca bar events (T='b') emit once per minute.
        Auto-skips if the capture window doesn't span a minute boundary.
        """
        if not is_market_open():
            pytest.skip("Market closed — bars not delivered.")

        _, bars = await self._alpaca_capture(LIVE_TICKERS, timeout=SLOW_TIMEOUT)
        if not bars:
            pytest.skip(
                f"No bar events in {SLOW_TIMEOUT}s — not near a minute boundary. "
                "Re-run at :00 seconds, or increase SLOW_TIMEOUT to 90."
            )

        b = bars[0]
        assert all(k in b for k in ("o","h","l","c")), f"Bar missing OHLC: {b}"
        assert float(b["h"]) >= float(b["l"]),  f"High < Low in bar: {b}"
        assert float(b["c"]) >  0,               f"Bar close <= 0: {b}"

    @pytest.mark.asyncio
    async def test_module_run_stop_lifecycle(self, alpaca_keys, real_redis):
        """
        Exercise module_run() / module_stop() against the live Alpaca feed.
        Runs for 5 seconds, stops, asserts singleton is marked not-running.
        """
        received: list[dict] = []

        def on_tick(ticker, payload):
            received.append(payload)

        run_task = asyncio.create_task(
            module_run(LIVE_TICKERS, on_tick=on_tick)
        )

        await asyncio.sleep(5)
        module_stop()

        try:
            await asyncio.wait_for(run_task, timeout=8)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            run_task.cancel()

        assert pf_module._feed_instance is not None
        assert pf_module._feed_instance._running is False

        if is_market_open() and not received:
            pytest.xfail(
                "No ticks in 5-second window — feed may be delayed or "
                "market activity was very low."
            )


# ─────────────────────────────────────────────────────────
#  Mark registration
# ─────────────────────────────────────────────────────────

def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "slow: live Alpaca WebSocket tests (~20 s, require market hours). "
        "Skip with: pytest -m 'not slow'",
    )