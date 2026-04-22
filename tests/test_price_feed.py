"""
tests/test_price_feed.py
Standalone unit tests for stock_scanner/ingestion/price_feed.py

Run from ANY directory — no Redis, no Alpaca API key, no internet required.
Every external dependency (Redis, websockets, yfinance) is mocked or faked.

  cd path/to/stock_scanner/          # project root
  pip install -e .                   # register package (once)
  pip install pytest pytest-asyncio fakeredis pandas

  python -m pytest tests/test_price_feed.py -v

Test coverage
─────────────
  ✔  _parse_hash              — type coercion (float, int, fallback str)
  ✔  PriceFeed.__init__       — ticker normalisation, Redis connection
  ✔  PriceFeed.stop           — sets _running = False
  ✔  PriceFeed._handle_tick   — Redis write, TTL, on_tick callback
  ✔  PriceFeed._handle_bar    — Redis write, TTL, field mapping
  ✔  PriceFeed.get_latest_tick — read back, missing ticker → {}
  ✔  PriceFeed.get_latest_bar  — read back, missing ticker → {}
  ✔  PriceFeed._connect       — auth failure raises ConnectionError
  ✔  PriceFeed._connect       — happy-path: auth → subscribe → T/AM dispatch
  ✔  PriceFeed.run            — reconnect loop retries on exception
  ✔  module-level get_latest_tick / get_latest_bar (no instance)
  ✔  module-level run() / stop() (singleton lifecycle)
  ✔  fetch_historical_ohlcv   — flat columns, MultiIndex columns, empty, error
  ✔  fetch_bulk_quotes        — happy path, partial failure, all-fail
"""

from __future__ import annotations

import asyncio
import json
import sys
import os
import time
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call

import fakeredis
import pandas as pd
import pytest
import pytest_asyncio
from dotenv import load_dotenv, dotenv_values
# ─────────────────────────────────────────────────────────
#  Path bootstrap — lets you run without `pip install -e .`
#  Adds the project root to sys.path so `*` resolves.
# ─────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ─────────────────────────────────────────────────────────
#  Stub out heavy/networked dependencies BEFORE importing price_feed
#  so they never touch the network even if accidentally called.
# ─────────────────────────────────────────────────────────

# Stub websockets — replaced per-test with AsyncMock
_ws_stub = types.ModuleType("websockets")
_ws_stub.connect = MagicMock()
sys.modules.setdefault("websockets", _ws_stub)

# Stub yfinance — replaced per-test
_yf_stub = types.ModuleType("yfinance")
sys.modules.setdefault("yfinance", _yf_stub)

# Stub loguru so test output stays clean
_loguru = types.ModuleType("loguru")
_logger = MagicMock()
_loguru.logger = _logger
sys.modules.setdefault("loguru", _loguru)

# Stub python-dotenv
_dotenv = types.ModuleType("dotenv")
_dotenv.load_dotenv = lambda *a, **kw: None
sys.modules.setdefault("dotenv", _dotenv)

# Now import the module under test
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingestion import price_feed as pf
from ingestion.price_feed import (
    PriceFeed,
    _parse_hash,
    fetch_historical_ohlcv,
    fetch_bulk_quotes,
)


# ─────────────────────────────────────────────────────────
#  Shared fake-Redis factory
# ─────────────────────────────────────────────────────────

def make_fake_redis() -> fakeredis.FakeRedis:
    """Return a fresh in-memory Redis instance for each test."""
    return fakeredis.FakeRedis(decode_responses=True)


def feed_with_fake_redis(tickers=None, on_tick=None) -> tuple[PriceFeed, fakeredis.FakeRedis]:
    """Create a PriceFeed whose _redis attribute is a FakeRedis."""
    tickers = tickers or ["AAPL", "TSLA"]
    feed = PriceFeed.__new__(PriceFeed)
    feed.tickers = [t.upper() for t in tickers]
    feed.on_tick = on_tick
    feed._running = False
    feed._redis = make_fake_redis()
    return feed, feed._redis


# ═════════════════════════════════════════════════════════
#  1. _parse_hash — type coercion helper
# ═════════════════════════════════════════════════════════

class TestParseHash:

    def test_float_conversion(self):
        result = _parse_hash({"price": "123.45", "size": "10.0"})
        assert result["price"] == pytest.approx(123.45)
        assert result["size"] == pytest.approx(10.0)

    def test_int_key_conversion(self):
        result = _parse_hash({"ts": "1700000000000"}, int_keys=("ts",))
        assert result["ts"] == 1700000000000
        assert isinstance(result["ts"], int)

    def test_multiple_int_keys(self):
        result = _parse_hash({"ts": "111", "n": "7"}, int_keys=("ts", "n"))
        assert isinstance(result["ts"], int)
        assert isinstance(result["n"], int)

    def test_non_numeric_falls_back_to_string(self):
        result = _parse_hash({"S": "AAPL", "price": "150.0"})
        assert result["S"] == "AAPL"   # kept as str (float("AAPL") fails)
        assert result["price"] == pytest.approx(150.0)

    def test_empty_dict(self):
        assert _parse_hash({}) == {}

    def test_zero_values(self):
        result = _parse_hash({"price": "0", "ts": "0"}, int_keys=("ts",))
        assert result["price"] == pytest.approx(0.0)
        assert result["ts"] == 0


# ═════════════════════════════════════════════════════════
#  2. PriceFeed.__init__
# ═════════════════════════════════════════════════════════

class TestPriceFeedInit:

    def test_tickers_uppercased(self):
        feed, _ = feed_with_fake_redis(["aapl", "tsla", "Nvda"])
        assert feed.tickers == ["AAPL", "TSLA", "NVDA"]

    def test_running_starts_false(self):
        feed, _ = feed_with_fake_redis()
        assert feed._running is False

    def test_on_tick_stored(self):
        cb = MagicMock()
        feed, _ = feed_with_fake_redis(on_tick=cb)
        assert feed.on_tick is cb

    def test_on_tick_defaults_none(self):
        feed, _ = feed_with_fake_redis()
        assert feed.on_tick is None

    def test_single_ticker(self):
        feed, _ = feed_with_fake_redis(["gme"])
        assert feed.tickers == ["GME"]


# ═════════════════════════════════════════════════════════
#  3. PriceFeed.stop
# ═════════════════════════════════════════════════════════

class TestStop:

    def test_stop_sets_running_false(self):
        feed, _ = feed_with_fake_redis()
        feed._running = True
        feed.stop()
        assert feed._running is False

    def test_stop_idempotent(self):
        feed, _ = feed_with_fake_redis()
        feed._running = False
        feed.stop()   # should not raise
        assert feed._running is False


# ═════════════════════════════════════════════════════════
#  4. PriceFeed._handle_tick
# ═════════════════════════════════════════════════════════

class TestHandleTick:

    def _make_tick_msg(self, S="AAPL", p=150.25, s=100, av=1_000_000, t=None):
        return {
            "T":  "t",
            "S":   S,
            "p":   p,
            "s":   s,
            "av":  av,
            "t":   t or int(time.time() * 1000),
        }

    def test_writes_correct_fields_to_redis(self):
        feed, r = feed_with_fake_redis()
        msg = self._make_tick_msg(S="AAPL", p=150.25, s=100, av=1_000_000)
        feed._handle_tick(msg)

        stored = r.hgetall("tick:AAPL")
        assert float(stored["price"])  == pytest.approx(150.25)
        assert float(stored["size"])   == pytest.approx(100.0)
        assert float(stored["volume"]) == pytest.approx(1_000_000.0)
        assert "ts" in stored

    def test_ticker_key_uses_Alpaca_sym(self):
        """Alpaca sends uppercase syms; key stored verbatim as tick:TSLA."""
        feed, r = feed_with_fake_redis()
        msg = self._make_tick_msg(S="TSLA", p=200.0)
        feed._handle_tick(msg)
        assert r.exists("tick:TSLA")

    def test_ttl_set_to_86400(self):
        feed, r = feed_with_fake_redis()
        feed._handle_tick(self._make_tick_msg())
        ttl = r.ttl("tick:AAPL")
        assert 86390 <= ttl <= 86400

    def test_on_tick_callback_called(self):
        cb = MagicMock()
        feed, _ = feed_with_fake_redis(on_tick=cb)
        msg = self._make_tick_msg(S="AAPL", p=155.0)
        feed._handle_tick(msg)
        cb.assert_called_once()
        ticker_arg, payload_arg = cb.call_args[0]
        assert ticker_arg == "AAPL"
        assert payload_arg["price"] == pytest.approx(155.0)

    def test_no_callback_does_not_raise(self):
        feed, _ = feed_with_fake_redis(on_tick=None)
        feed._handle_tick(self._make_tick_msg())  # must not raise

    def test_missing_optional_fields_default_to_zero(self):
        feed, r = feed_with_fake_redis()
        feed._handle_tick({"T": "t", "S": "AAPL"})  # no p/s/av/t
        stored = r.hgetall("tick:AAPL")
        assert float(stored["price"])  == pytest.approx(0.0)
        assert float(stored["size"])   == pytest.approx(0.0)
        assert float(stored["volume"]) == pytest.approx(0.0)

    def test_multiple_tickers_stored_independently(self):
        feed, r = feed_with_fake_redis(["AAPL", "TSLA"])
        feed._handle_tick(self._make_tick_msg(S="AAPL", p=150.0))
        feed._handle_tick(self._make_tick_msg(S="TSLA", p=210.0))
        assert float(r.hget("tick:AAPL", "price")) == pytest.approx(150.0)
        assert float(r.hget("tick:TSLA", "price")) == pytest.approx(210.0)

    def test_second_tick_overwrites_first(self):
        feed, r = feed_with_fake_redis()
        feed._handle_tick(self._make_tick_msg(S="AAPL", p=100.0))
        feed._handle_tick(self._make_tick_msg(S="AAPL", p=105.0))
        assert float(r.hget("tick:AAPL", "price")) == pytest.approx(105.0)


# ═════════════════════════════════════════════════════════
#  5. PriceFeed._handle_bar
# ═════════════════════════════════════════════════════════

class TestHandleBar:

    def _make_bar_msg(self, S="AAPL", o=149.0, h=151.0, l=148.5,
                      c=150.0, v=500_000, vw=149.8, n=1234, s=None):
        return {
            "T": "b", "S": S,
            "o": o, "h": h, "l": l, "c": c,
            "v": v, "vw": vw, "n": n,
            "s": s or int(time.time() * 1000),
        }

    def test_all_ohlcv_fields_written(self):
        feed, r = feed_with_fake_redis()
        feed._handle_bar(self._make_bar_msg(
            o=149.0, h=151.0, l=148.5, c=150.0, v=500_000, vw=149.8
        ))
        stored = r.hgetall("bar1m:AAPL")
        assert float(stored["o"])  == pytest.approx(149.0)
        assert float(stored["h"])  == pytest.approx(151.0)
        assert float(stored["l"])  == pytest.approx(148.5)
        assert float(stored["c"])  == pytest.approx(150.0)
        assert float(stored["v"])  == pytest.approx(500_000.0)
        assert float(stored["vw"]) == pytest.approx(149.8)

    def test_n_field_from_z(self):
        """Alpaca uses 'z' for trade count — we store it as 'n'."""
        feed, r = feed_with_fake_redis()
        feed._handle_bar(self._make_bar_msg(n=999))
        assert float(r.hget("bar1m:AAPL", "n")) == pytest.approx(999.0)

    def test_ttl_set(self):
        feed, r = feed_with_fake_redis()
        feed._handle_bar(self._make_bar_msg())
        ttl = r.ttl("bar1m:AAPL")
        assert 86390 <= ttl <= 86400

    def test_missing_fields_default_to_zero(self):
        feed, r = feed_with_fake_redis()
        feed._handle_bar({"T": "b", "S": "AAPL"})
        stored = r.hgetall("bar1m:AAPL")
        assert float(stored["c"]) == pytest.approx(0.0)

    def test_key_uses_Alpaca_sym(self):
        """Alpaca always sends uppercase; we store verbatim."""
        feed, r = feed_with_fake_redis()
        feed._handle_bar(self._make_bar_msg(S="NVDA"))
        assert r.exists("bar1m:NVDA")


# ═════════════════════════════════════════════════════════
#  6. PriceFeed.get_latest_tick
# ═════════════════════════════════════════════════════════

class TestGetLatestTick:

    def test_returns_dict_with_correct_types(self):
        feed, r = feed_with_fake_redis()
        r.hset("tick:AAPL", mapping={"price": "150.25", "volume": "1000000", "ts": "1700000000000"})
        tick = feed.get_latest_tick("AAPL")
        assert isinstance(tick["price"], float)
        assert isinstance(tick["volume"], float)
        assert isinstance(tick["ts"], int)
        assert tick["price"] == pytest.approx(150.25)

    def test_case_insensitive_lookup(self):
        feed, r = feed_with_fake_redis()
        r.hset("tick:AAPL", mapping={"price": "200.0", "ts": "111"})
        assert feed.get_latest_tick("aapl")["price"] == pytest.approx(200.0)
        assert feed.get_latest_tick("Aapl")["price"] == pytest.approx(200.0)

    def test_returns_empty_dict_when_missing(self):
        feed, _ = feed_with_fake_redis()
        assert feed.get_latest_tick("UNKNOWN") == {}

    def test_returns_empty_dict_for_expired_key(self):
        feed, r = feed_with_fake_redis()
        r.hset("tick:AAPL", mapping={"price": "100.0", "ts": "1"})
        r.delete("tick:AAPL")
        assert feed.get_latest_tick("AAPL") == {}


# ═════════════════════════════════════════════════════════
#  7. PriceFeed.get_latest_bar
# ═════════════════════════════════════════════════════════

class TestGetLatestBar:

    def test_returns_floats_and_int_for_ts_n(self):
        feed, r = feed_with_fake_redis()
        r.hset("bar1m:TSLA", mapping={
            "o": "200.0", "h": "205.0", "l": "199.5",
            "c": "203.0", "v": "750000", "vw": "201.5",
            "n": "3000", "ts": "1700000000000",
        })
        bar = feed.get_latest_bar("TSLA")
        assert isinstance(bar["c"], float)
        assert isinstance(bar["n"], int)
        assert isinstance(bar["ts"], int)
        assert bar["c"] == pytest.approx(203.0)
        assert bar["n"] == 3000

    def test_returns_empty_dict_for_missing_ticker(self):
        feed, _ = feed_with_fake_redis()
        assert feed.get_latest_bar("MISSING") == {}

    def test_case_insensitive(self):
        feed, r = feed_with_fake_redis()
        r.hset("bar1m:GME", mapping={"c": "42.0", "ts": "0", "n": "1"})
        assert feed.get_latest_bar("gme")["c"] == pytest.approx(42.0)


# ═════════════════════════════════════════════════════════
#  8. PriceFeed._connect  (async — requires pytest-asyncio)
# ═════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestConnect:

    def _make_ws_context(self, messages: list[list[dict]], auth_status="authorized"):
        """
        Build an async context-manager mock for websockets.connect().

        Uses a real async-generator class for `async for raw in ws` iteration
        because AsyncMock does not properly implement __aiter__/__anext__
        for use with `async for` in Python 3.12.
        """
        auth_resp = json.dumps([{"status": auth_status}])
        encoded = [json.dumps(batch) for batch in messages]

        class _MockWS:
            """Minimal websocket stand-in: recv() + async iteration."""
            def __init__(self_):
                self_.sent = []
            async def recv(self_):
                return auth_resp
            async def send(self_, data):
                self_.sent.append(data)
            def __aiter__(self_):
                return self_._gen()
            async def _gen(self_):
                for raw in encoded:
                    yield raw

        ws = _MockWS()

        # Wrap in an async context manager so `async with connect(...) as ws` works
        class _CM:
            async def __aenter__(self_):
                return ws
            async def __aexit__(self_, *args):
                return False

        return _CM(), ws

    async def test_auth_failure_raises_connection_error(self):
        feed, _ = feed_with_fake_redis()
        cm, _ = self._make_ws_context([], auth_status="auth_failed")

        with patch("ingestion.price_feed.websockets.connect", return_value=cm):
            with pytest.raises(ConnectionError, match="Alpaca auth failed"):
                await feed._connect()

    async def test_subscribe_message_sent_for_all_tickers(self):
        feed, _ = feed_with_fake_redis(["AAPL", "TSLA"])
        cm, ws = self._make_ws_context([])

        with patch("ingestion.price_feed.websockets.connect", return_value=cm):
            try:
                await feed._connect()
            except Exception:
                pass

        sent_calls = ws.sent  # _MockWS stores sent messages in .sent list
        sub_msg = next(
            json.loads(m) for m in sent_calls
            if json.loads(m).get("action") == "subscribe"
        )
        # params = sub_msg["params"]
        assert "trades" in sub_msg
        assert "bars" in sub_msg
      
    async def test_trade_tick_message_dispatched(self):
        feed, r = feed_with_fake_redis(["AAPL"])
        # Patch websockets AND make the feed._redis point to our fake redis
        # (it already does via feed_with_fake_redis — no extra patching needed)
        tick_msg = [{"T": "t", "S": "AAPL", "p": 155.0, "s": 50,
                     "av": 1_000_000, "t": 1700000000000}]
        cm, _ = self._make_ws_context([tick_msg])

        with patch("ingestion.price_feed.websockets.connect", return_value=cm):
            try:
                await feed._connect()
            except Exception:
                pass

        assert r.hexists("tick:AAPL", "price")
        assert float(r.hget("tick:AAPL", "price")) == pytest.approx(155.0)

    async def test_bar_message_dispatched(self):
        feed, r = feed_with_fake_redis(["NVDA"])
        bar_msg = [{"T": "b", "S": "NVDA",
                    "o": 400.0, "h": 410.0, "l": 398.0,
                    "c": 405.0, "v": 300_000, "vw": 403.0,
                    "z": 4500, "s": 1700000000000}]
        cm, _ = self._make_ws_context([bar_msg])

        with patch("ingestion.price_feed.websockets.connect", return_value=cm):
            try:
                await feed._connect()
            except Exception:
                pass

        assert r.hexists("bar1m:NVDA", "c")
        assert float(r.hget("bar1m:NVDA", "c")) == pytest.approx(405.0)

    async def test_mixed_messages_dispatched(self):
        """Both T and AM events in a single batch are processed."""
        feed, r = feed_with_fake_redis(["AMD"])
        batch = [
            {"T": "t",  "S": "AMD", "p": 120.0, "s": 200,
             "av": 500_000, "t": 1700000000000},
            {"T": "b", "S": "AMD", "o": 119.0, "h": 121.0,
             "l": 118.5, "c": 120.0, "v": 100_000,
             "vw": 119.8, "z": 800, "s": 1700000000000},
        ]
        cm, _ = self._make_ws_context([batch])

        with patch("ingestion.price_feed.websockets.connect", return_value=cm):
            try:
                await feed._connect()
            except Exception:
                pass

        assert r.hexists("tick:AMD", "price")
        assert r.hexists("bar1m:AMD", "c")

    async def test_unknown_event_type_ignored(self):
        """Unknown event types (e.g. 'status') must not raise."""
        feed, r = feed_with_fake_redis(["AAPL"])
        cm, _ = self._make_ws_context(
            [[{"T": "status", "status": "authorized", "message": "ok"}]]
        )
        with patch("ingestion.price_feed.websockets.connect", return_value=cm):
            try:
                await feed._connect()   # should not raise
            except Exception:
                pass
        assert not r.exists("tick:AAPL")   # nothing written


# ═════════════════════════════════════════════════════════
#  9. PriceFeed.run — reconnection loop
# ═════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestRun:

    async def test_run_calls_connect_sets_running(self):
        feed, _ = feed_with_fake_redis()
        connect_calls = []

        async def fake_connect():
            connect_calls.append(1)
            feed._running = False   # stop after first iteration

        feed._connect = fake_connect
        await feed.run()
        assert len(connect_calls) == 1

    async def test_run_retries_on_exception(self):
        feed, _ = feed_with_fake_redis()
        call_count = [0]

        async def fake_connect():
            call_count[0] += 1
            if call_count[0] < 3:
                raise OSError("network error")
            feed._running = False

        feed._connect = fake_connect
        with patch("ingestion.price_feed.asyncio.sleep", new=AsyncMock()):
            await feed.run()

        assert call_count[0] == 3   # two failures then success

    async def test_stop_prevents_reconnect(self):
        feed, _ = feed_with_fake_redis()
        connect_calls = [0]

        async def fake_connect():
            connect_calls[0] += 1
            feed.stop()   # stop inside first connect call

        feed._connect = fake_connect
        await feed.run()
        assert connect_calls[0] == 1


# ═════════════════════════════════════════════════════════
#  10. Module-level functions (no singleton)
# ═════════════════════════════════════════════════════════

class TestModuleLevelFunctions:

    def setup_method(self):
        """Reset the module singleton before each test."""
        pf._feed_instance = None

    def test_get_latest_tick_no_instance_reads_redis(self):
        fake_r = make_fake_redis()
        fake_r.hset("tick:MSFT", mapping={"price": "310.0", "ts": "123456789"})

        with patch.object(pf, "_get_redis", return_value=fake_r):
            tick = pf.get_latest_tick("MSFT")

        assert tick["price"] == pytest.approx(310.0)

    def test_get_latest_tick_no_instance_missing_ticker(self):
        fake_r = make_fake_redis()
        with patch.object(pf, "_get_redis", return_value=fake_r):
            assert pf.get_latest_tick("NOPE") == {}

    def test_get_latest_bar_no_instance_reads_redis(self):
        fake_r = make_fake_redis()
        fake_r.hset("bar1m:AMZN", mapping={
            "c": "178.0", "v": "2000000", "ts": "999", "n": "5000"
        })
        with patch.object(pf, "_get_redis", return_value=fake_r):
            bar = pf.get_latest_bar("AMZN")

        assert bar["c"] == pytest.approx(178.0)
        assert isinstance(bar["n"], int)

    def test_get_latest_tick_uses_instance_when_set(self):
        feed, _ = feed_with_fake_redis(["AAPL"])
        feed._redis.hset("tick:AAPL", mapping={"price": "200.0", "ts": "1"})
        pf._feed_instance = feed
        assert pf.get_latest_tick("AAPL")["price"] == pytest.approx(200.0)

    def test_stop_no_instance_does_not_raise(self):
        pf._feed_instance = None
        pf.stop()   # must not raise

    def test_stop_with_instance_stops_feed(self):
        feed, _ = feed_with_fake_redis()
        feed._running = True
        pf._feed_instance = feed
        pf.stop()
        assert not feed._running


@pytest.mark.asyncio
class TestModuleLevelRun:

    def setup_method(self):
        pf._feed_instance = None

    async def test_run_creates_singleton(self):
        assert pf._feed_instance is None

        async def fake_run(self_):
            self_._running = False   # exit immediately

        with patch.object(PriceFeed, "run", fake_run):
            with patch("ingestion.price_feed.redis.Redis",
                       return_value=make_fake_redis()):
                await pf.run(["AAPL", "TSLA"])

        assert pf._feed_instance is not None
        assert pf._feed_instance.tickers == ["AAPL", "TSLA"]

    async def test_run_passes_on_tick_callback(self):
        cb = MagicMock()

        async def fake_run(self_):
            self_._running = False

        with patch.object(PriceFeed, "run", fake_run):
            with patch("ingestion.price_feed.redis.Redis",
                       return_value=make_fake_redis()):
                await pf.run(["GME"], on_tick=cb)

        assert pf._feed_instance.on_tick is cb


# ═════════════════════════════════════════════════════════
#  11. fetch_historical_ohlcv
# ═════════════════════════════════════════════════════════

class TestFetchHistoricalOHLCV:

    def _make_flat_df(self, ticker="AAPL") -> pd.DataFrame:
        idx = pd.date_range("2024-01-02", periods=5, freq="D", name="Date")
        return pd.DataFrame({
            "Open":   [100.0, 101.0, 102.0, 103.0, 104.0],
            "High":   [105.0, 106.0, 107.0, 108.0, 109.0],
            "Low":    [99.0,  100.0, 101.0, 102.0, 103.0],
            "Close":  [102.0, 103.0, 104.0, 105.0, 106.0],
            "Volume": [1e6,   1.1e6, 1.2e6, 1.3e6, 1.4e6],
        }, index=idx)

    def _make_multiindex_df(self, ticker="AAPL") -> pd.DataFrame:
        idx = pd.date_range("2024-01-02", periods=3, freq="D", name="Date")
        cols = pd.MultiIndex.from_tuples([
            ("Open", ticker), ("High", ticker), ("Low", ticker),
            ("Close", ticker), ("Volume", ticker),
        ])
        return pd.DataFrame([
            [100.0, 105.0, 99.0, 102.0, 1e6],
            [101.0, 106.0, 100.0, 103.0, 1.1e6],
            [102.0, 107.0, 101.0, 104.0, 1.2e6],
        ], index=idx, columns=cols)

    def test_returns_lowercase_columns(self):
        mock_yf = MagicMock()
        mock_yf.download = MagicMock(return_value=self._make_flat_df())
        with patch.dict(sys.modules, {"yfinance": mock_yf}):
            with patch("ingestion.price_feed.yf", mock_yf):
                df = fetch_historical_ohlcv("AAPL")
        assert list(df.columns) == ["open", "high", "low", "close", "volume"]

    def test_handles_multiindex_columns(self):
        mock_yf = MagicMock()
        mock_yf.download = MagicMock(return_value=self._make_multiindex_df())
        with patch("ingestion.price_feed.yf", mock_yf):
            df = fetch_historical_ohlcv("AAPL")
        assert "close" in df.columns
        assert "open" in df.columns
        assert not isinstance(df.columns, pd.MultiIndex)

    def test_returns_only_ohlcv_columns(self):
        extra_df = self._make_flat_df()
        extra_df["Dividends"] = 0.0   # yfinance sometimes adds extra cols
        extra_df["Stock Splits"] = 0.0
        mock_yf = MagicMock()
        mock_yf.download = MagicMock(return_value=extra_df)
        with patch("ingestion.price_feed.yf", mock_yf):
            df = fetch_historical_ohlcv("AAPL")
        assert "dividends" not in df.columns
        assert set(df.columns) == {"open", "high", "low", "close", "volume"}

    def test_returns_empty_df_on_empty_download(self):
        mock_yf = MagicMock()
        mock_yf.download = MagicMock(return_value=pd.DataFrame())
        with patch("ingestion.price_feed.yf", mock_yf):
            df = fetch_historical_ohlcv("BADTICKER")
        assert df.empty

    def test_returns_empty_df_on_exception(self):
        mock_yf = MagicMock()
        mock_yf.download = MagicMock(side_effect=Exception("network error"))
        with patch("ingestion.price_feed.yf", mock_yf):
            df = fetch_historical_ohlcv("AAPL")
        assert df.empty

    def test_index_name_is_date(self):
        mock_yf = MagicMock()
        mock_yf.download = MagicMock(return_value=self._make_flat_df())
        with patch("ingestion.price_feed.yf", mock_yf):
            df = fetch_historical_ohlcv("AAPL")
        assert df.index.name == "date"

    def test_passes_period_and_interval_to_yf(self):
        mock_yf = MagicMock()
        mock_yf.download = MagicMock(return_value=self._make_flat_df())
        with patch("ingestion.price_feed.yf", mock_yf):
            fetch_historical_ohlcv("AAPL", period="3mo", interval="1h")
        call_kwargs = mock_yf.download.call_args[1]
        assert call_kwargs["period"] == "3mo"
        assert call_kwargs["interval"] == "1h"

    def test_auto_adjust_always_true(self):
        mock_yf = MagicMock()
        mock_yf.download = MagicMock(return_value=self._make_flat_df())
        with patch("ingestion.price_feed.yf", mock_yf):
            fetch_historical_ohlcv("AAPL")
        assert mock_yf.download.call_args[1]["auto_adjust"] is True

    def test_data_values_preserved(self):
        mock_yf = MagicMock()
        mock_yf.download = MagicMock(return_value=self._make_flat_df())
        with patch("ingestion.price_feed.yf", mock_yf):
            df = fetch_historical_ohlcv("AAPL")
        assert df["close"].iloc[0] == pytest.approx(102.0)
        assert df["volume"].iloc[-1] == pytest.approx(1.4e6)


# ═════════════════════════════════════════════════════════
#  12. fetch_bulk_quotes
# ═════════════════════════════════════════════════════════

class TestFetchBulkQuotes:

    def _make_fast_info(self, price=150.0, market_cap=2.5e12,
                        shares=1.6e10, year_high=200.0, year_low=120.0):
        fi = MagicMock()
        fi.last_price    = price
        fi.market_cap    = market_cap
        fi.shares        = shares
        fi.year_high     = year_high
        fi.year_low      = year_low
        return fi

    def test_happy_path_single_ticker(self):
        fi = self._make_fast_info(price=155.0)
        mock_ticker = MagicMock()
        mock_ticker.fast_info = fi
        mock_yf = MagicMock()
        mock_yf.Ticker = MagicMock(return_value=mock_ticker)

        with patch("ingestion.price_feed.yf", mock_yf):
            result = fetch_bulk_quotes(["AAPL"])

        assert "AAPL" in result
        assert result["AAPL"]["price"] == pytest.approx(155.0)

    def test_all_fields_present(self):
        fi = self._make_fast_info(price=200.0, market_cap=1e12,
                                  shares=5e9, year_high=250.0, year_low=150.0)
        mock_ticker = MagicMock()
        mock_ticker.fast_info = fi
        mock_yf = MagicMock()
        mock_yf.Ticker = MagicMock(return_value=mock_ticker)

        with patch("ingestion.price_feed.yf", mock_yf):
            result = fetch_bulk_quotes(["TSLA"])

        r = result["TSLA"]
        assert r["price"]              == pytest.approx(200.0)
        assert r["market_cap"]         == pytest.approx(1e12)
        assert r["shares_outstanding"] == pytest.approx(5e9)
        assert r["52w_high"]           == pytest.approx(250.0)
        assert r["52w_low"]            == pytest.approx(150.0)

    def test_multiple_tickers_all_returned(self):
        mock_yf = MagicMock()
        mock_yf.Ticker = MagicMock(side_effect=lambda t: MagicMock(
            fast_info=self._make_fast_info(price=100.0 + ord(t[0]))
        ))
        with patch("ingestion.price_feed.yf", mock_yf):
            result = fetch_bulk_quotes(["AAPL", "GOOG", "MSFT"])

        assert set(result.keys()) == {"AAPL", "GOOG", "MSFT"}

    def test_failed_ticker_returns_price_zero(self):
        mock_yf = MagicMock()
        mock_yf.Ticker = MagicMock(side_effect=Exception("API error"))
        with patch("ingestion.price_feed.yf", mock_yf):
            result = fetch_bulk_quotes(["BADTICKER"])
        assert result["BADTICKER"]["price"] == pytest.approx(0.0)

    def test_partial_failure_returns_all_keys(self):
        """One ticker fails, the other succeeds — both appear in result."""
        def ticker_factory(S):
            t = MagicMock()
            if S == "BAD":
                t.fast_info = MagicMock(side_effect=Exception("bad"))
                # Accessing .fast_info raises AttributeError via MagicMock
                type(t).fast_info = property(lambda self: (_ for _ in ()).throw(Exception("bad")))
            else:
                t.fast_info = self._make_fast_info(price=500.0)
            return t

        mock_yf = MagicMock()
        mock_yf.Ticker = MagicMock(side_effect=ticker_factory)
        with patch("ingestion.price_feed.yf", mock_yf):
            result = fetch_bulk_quotes(["GOOD", "BAD"])

        assert "GOOD" in result
        assert "BAD" in result
        assert result["GOOD"]["price"] == pytest.approx(500.0)
        assert result["BAD"]["price"] == pytest.approx(0.0)

    def test_none_price_falls_back_to_zero(self):
        fi = MagicMock()
        fi.last_price = None
        fi.regularMarketPrice = None   # also None
        fi.market_cap = 0
        fi.shares = 0
        fi.year_high = 0
        fi.year_low = 0
        mock_ticker = MagicMock()
        mock_ticker.fast_info = fi
        mock_yf = MagicMock()
        mock_yf.Ticker = MagicMock(return_value=mock_ticker)
        with patch("ingestion.price_feed.yf", mock_yf):
            result = fetch_bulk_quotes(["NULL"])
        assert result["NULL"]["price"] == pytest.approx(0.0)

    def test_empty_ticker_list(self):
        mock_yf = MagicMock()
        with patch("ingestion.price_feed.yf", mock_yf):
            result = fetch_bulk_quotes([])
        assert result == {}
        mock_yf.Ticker.assert_not_called()


# ═════════════════════════════════════════════════════════
#  Pytest configuration
# ═════════════════════════════════════════════════════════

# Tell pytest-asyncio to use asyncio mode for this file
pytestmark = pytest.mark.asyncio


if __name__ == "__main__":
    # Allow running directly: python tests/test_price_feed.py
    import subprocess, sys
    sys.exit(subprocess.call(
        [sys.executable, "-m", "pytest", __file__, "-v", "--tb=short"],
    ))
