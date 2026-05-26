"""
tests/test_short_data.py
Full pytest suite for ingestion/short_data.py

No network, no Redis server, no API keys required.
  - Redis   → replaced by fakeredis.FakeRedis
  - HTTP    → replaced by unittest.mock.patch on requests.get
  - yfinance→ replaced by unittest.mock.patch on yf.Ticker
  - loguru  → silenced via stub

Run
───
  pip install pytest fakeredis pandas
  pytest tests/test_short_data.py -v

Coverage map
────────────
  ShortProfile dataclass
    __init__          default values, all fields populated
    is_squeeze_candidate  all three conditions, boundary values, partial fail
    squeeze_score         formula components, clamping, range 0–1
    to_redis_mapping      all values serialised as str
    from_redis            float fields, int fields, str fields, missing key

  _regsho_date_str
    weekday (Mon–Fri) → returned as-is
    Saturday          → stepped back to Friday
    Sunday            → stepped back to Friday
    offset_days       → subtracts calendar days then weekend-adjusts

  fetch_regsho_short_vol_ratio
    happy path        → ratio = ShortVolume / TotalVolume
    ticker not in file→ returns 0.0
    HTTP 404 first    → falls back to next date
    HTTP 404 all four → returns 0.0
    non-200 status    → returns 0.0
    network exception → returns 0.0
    zero TotalVolume  → returns 0.0
    missing columns   → returns 0.0 (continues to next date then gives up)
    ticker uppercased → match is case-insensitive lookup
    ratio clamped     → valid float between 0 and 1
    whitespace in symbol column → stripped correctly

  ShortDataClient._fetch_yfinance_info
    happy path        → returns info dict
    exception raised  → returns {}
    non-dict return   → returns {}

  ShortDataClient._fetch_finnhub_metrics
    no api key        → returns {} without making HTTP request
    happy path        → returns metric dict
    HTTP non-200      → returns {}
    exception         → returns {}

  ShortDataClient._build_profile
    full yfinance info → all fields populated correctly
    shortRatio from yfinance → used directly
    shortRatio missing → calculated from short_interest / avg_volume
    short_float_pct from yfinance.shortPercentOfFloat → used directly
    short_float_pct calculated → short_interest / float_shares
    price fallback chain → currentPrice → regularMarketPrice → previousClose
    beta fallback → yfinance → finnhub → 1.0
    include_regsho=True  → calls fetch_regsho_short_vol_ratio
    include_regsho=False → short_vol_ratio is 0.0
    data_source label → contains "yfinance", adds "finra_regsho" / "finnhub"
    empty yfinance info → all numeric fields default to 0.0

  ShortDataClient._save_cache / _load_cache
    save then load    → round-trip exact values
    load missing key  → returns None
    load corrupt data → returns None
    TTL               → 14 400 s (4 hours)
    ticker uppercased in key

  ShortDataClient.get_short_profile
    cache hit         → returns cached, no yfinance call
    cache miss        → builds profile, caches it
    force_refresh     → bypasses cache, re-fetches
    ticker uppercased

  ShortDataClient.get_bulk_profiles
    returns dict keyed by UPPER ticker
    each value is a ShortProfile
    include_regsho=False propagated

  ShortDataClient.get_top_short_float
    sorted by short_float_pct descending
    top_n respected
    top_n larger than list → returns all

  ShortDataClient.fetch_regsho_batch
    happy path → all tickers found in one file
    partial match → some tickers found, rest 0.0
    HTTP 404 then success → falls back to next date
    all 404 → all zeros
    zero total_vol row → 0.0 for that ticker
"""

from __future__ import annotations

import sys
import time
import types
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import fakeredis
import pytest
import redis as real_redis
import sys
import os  
from dotenv import load_dotenv, dotenv_values
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

# ─────────────────────────────────────────────────────────
#  Path bootstrap  (works without pip install -e .)
# ─────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ─────────────────────────────────────────────────────────
#  Silence loguru before import so test output stays clean
# ─────────────────────────────────────────────────────────
_loguru_stub        = types.ModuleType("loguru")
_loguru_stub.logger = MagicMock()
sys.modules.setdefault("loguru", _loguru_stub)

_dotenv_stub             = types.ModuleType("dotenv")
_dotenv_stub.load_dotenv = lambda *a, **kw: None
sys.modules.setdefault("dotenv", _dotenv_stub)

# ─────────────────────────────────────────────────────────
#  Module under test
# ─────────────────────────────────────────────────────────
from ingestion.short_data import (
    ShortDataClient,
    ShortProfile,
    _regsho_date_str,
    fetch_regsho_short_vol_ratio,
)


# ─────────────────────────────────────────────────────────
#  Shared test helpers
# ─────────────────────────────────────────────────────────

def make_fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


def make_client(fake_r: fakeredis.FakeRedis) -> ShortDataClient:
    """Create a ShortDataClient wired to an in-memory Redis."""
    client = ShortDataClient.__new__(ShortDataClient)
    client._redis = fake_r
    return client


def full_yf_info(
    ticker: str = "GME",
    float_shares: float = 5_000_000,
    shares_outstanding: float = 76_000_000,
    shares_short: float = 1_500_000,
    short_ratio: float = 3.5,
    short_pct_float: float = 0.30,
    market_cap: float = 800_000_000,
    price: float = 15.50,
    beta: float = 1.8,
    avg_vol: float = 420_000,
    inst_pct: float = 0.35,
    insider_pct: float = 0.12,
    prior_short: float = 1_200_000,
) -> dict:
    """Minimal yfinance .info dict covering every field short_data reads."""
    return {
        "floatShares":              float_shares,
        "sharesOutstanding":        shares_outstanding,
        "sharesShort":              shares_short,
        "shortRatio":               short_ratio,
        "shortPercentOfFloat":      short_pct_float,
        "sharesShortPriorMonth":    prior_short,
        "sharesShortPreviousMonthDate": "2025-03-31",
        "marketCap":                market_cap,
        "currentPrice":             price,
        "beta":                     beta,
        "averageDailyVolume10Day":  avg_vol,
        "averageVolume":            avg_vol,
        "heldPercentInstitutions":  inst_pct,
        "heldPercentInsiders":      insider_pct,
    }


def regsho_csv(rows: list[tuple[str, int, int]]) -> str:
    """
    Build a mock FINRA RegSHO pipe-delimited file body.
    rows: list of (Symbol, ShortVolume, TotalVolume)
    """
    header = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market"
    lines  = [header]
    for sym, sv, tv in rows:
        lines.append(f"20250421|{sym}|{sv}|0|{tv}|CNMS")
    return "\n".join(lines)


def mock_response(status: int, text: str = "") -> MagicMock:
    r            = MagicMock()
    r.status_code = status
    r.text        = text
    return r


# ═════════════════════════════════════════════════════════
#  1. ShortProfile dataclass
# ═════════════════════════════════════════════════════════

class TestShortProfileDefaults:

    def test_ticker_required(self):
        p = ShortProfile(ticker="GME")
        assert p.ticker == "GME"

    def test_numeric_defaults_are_zero(self):
        p = ShortProfile(ticker="X")
        for field in (
            "float_shares", "shares_outstanding", "short_interest",
            "short_float_pct", "short_ratio", "short_vol_ratio",
            "shares_short_prior", "borrow_rate_pct", "inst_ownership_pct",
            "insider_ownership_pct", "market_cap", "price", "avg_volume_10d",
        ):
            assert getattr(p, field) == 0.0, f"{field} should default to 0.0"

    def test_beta_defaults_to_one(self):
        assert ShortProfile(ticker="X").beta == 1.0

    def test_data_source_defaults_to_yfinance(self):
        assert ShortProfile(ticker="X").data_source == "yfinance"

    def test_updated_at_is_recent_epoch(self):
        before = int(time.time())
        p      = ShortProfile(ticker="X")
        after  = int(time.time())
        assert before <= p.updated_at <= after

    def test_all_fields_populated(self):
        p = ShortProfile(
            ticker="GME", float_shares=5e6, shares_outstanding=76e6,
            short_interest=1.5e6, short_float_pct=0.30, short_ratio=3.5,
            short_vol_ratio=0.48, shares_short_prior=1.2e6,
            borrow_rate_pct=0.0, inst_ownership_pct=0.35,
            insider_ownership_pct=0.12, market_cap=8e8, price=15.50,
            beta=1.8, avg_volume_10d=420_000,
            data_source="yfinance+finra_regsho", updated_at=1_700_000_000,
        )
        assert p.float_shares     == pytest.approx(5e6)
        assert p.short_float_pct  == pytest.approx(0.30)
        assert p.data_source      == "yfinance+finra_regsho"
        assert p.updated_at       == 1_700_000_000


class TestIsSqueezeCandidate:
    """All three conditions must be True simultaneously."""

    def _profile(self, sf_pct=0.25, float_sh=10_000_000, dtc=6.0) -> ShortProfile:
        return ShortProfile(
            ticker="X",
            short_float_pct=sf_pct,
            float_shares=float_sh,
            short_ratio=dtc,
        )

    def test_all_conditions_met_returns_true(self):
        assert self._profile().is_squeeze_candidate() is True

    def test_short_float_below_threshold_returns_false(self):
        assert self._profile(sf_pct=0.19).is_squeeze_candidate() is False

    def test_short_float_at_exact_threshold_returns_true(self):
        assert self._profile(sf_pct=0.20).is_squeeze_candidate() is True

    def test_float_shares_above_threshold_returns_false(self):
        assert self._profile(float_sh=20_000_001).is_squeeze_candidate() is False

    def test_float_shares_at_exact_threshold_returns_true(self):
        assert self._profile(float_sh=20_000_000).is_squeeze_candidate() is True

    def test_days_to_cover_below_threshold_returns_false(self):
        assert self._profile(dtc=4.9).is_squeeze_candidate() is False

    def test_days_to_cover_at_exact_threshold_returns_true(self):
        assert self._profile(dtc=5.0).is_squeeze_candidate() is True

    def test_custom_thresholds_respected(self):
        p = self._profile(sf_pct=0.15, float_sh=5_000_000, dtc=3.0)
        assert p.is_squeeze_candidate(
            min_short_float=0.15,
            max_float_shares=5_000_000,
            min_days_to_cover=3.0,
        ) is True

    def test_two_of_three_conditions_false(self):
        """Fails if any single condition is not met."""
        p = self._profile(sf_pct=0.10, float_sh=5_000_000, dtc=10.0)
        assert p.is_squeeze_candidate() is False  # short_float_pct fails

    def test_all_zeros_returns_false(self):
        assert ShortProfile(ticker="X").is_squeeze_candidate() is False


class TestSqueezeScore:

    def test_perfect_setup_close_to_one(self):
        """Max si, near-zero float, high DTC, high SVR → score near 1.0."""
        p = ShortProfile(
            ticker="X",
            short_float_pct=0.50,   # maxed si_component
            float_shares=0,          # maxed float_component (1.0)
            short_ratio=20.0,        # maxed dtc_component
            short_vol_ratio=0.80,    # maxed svr_component
        )
        assert p.squeeze_score() >= 0.95

    def test_zeros_return_zero(self):
        assert ShortProfile(ticker="X").squeeze_score() == pytest.approx(0.0)

    def test_score_range_is_0_to_1(self):
        for sf, fl, dtc, svr in [
            (0.50, 0, 20.0, 0.80),
            (0.10, 50e6, 1.0, 0.30),
            (0.25, 10e6, 5.0, 0.45),
        ]:
            p = ShortProfile(
                ticker="X", short_float_pct=sf, float_shares=fl,
                short_ratio=dtc, short_vol_ratio=svr,
            )
            s = p.squeeze_score()
            assert 0.0 <= s <= 1.0, f"Score {s} out of range for {sf},{fl},{dtc},{svr}"

    def test_si_component_capped_at_50pct(self):
        """short_float_pct above 0.50 should still give same si_component as 0.50."""
        p_high = ShortProfile(ticker="X", short_float_pct=0.90)
        p_max  = ShortProfile(ticker="X", short_float_pct=0.50)
        assert p_high.squeeze_score() == p_max.squeeze_score()

    def test_svr_component_zero_below_40pct(self):
        """short_vol_ratio < 0.40 → svr_component = 0."""
        p = ShortProfile(
            ticker="X",
            short_vol_ratio=0.39,
            short_float_pct=0.0,
            float_shares=20e6,
            short_ratio=0.0,
        )
        # Only float_component could contribute (20M → 0 since 1-20M/20M=0)
        assert p.squeeze_score() == pytest.approx(0.0)

    def test_higher_short_float_gives_higher_score(self):
        p_low  = ShortProfile(ticker="X", short_float_pct=0.10)
        p_high = ShortProfile(ticker="X", short_float_pct=0.40)
        assert p_high.squeeze_score() > p_low.squeeze_score()

    def test_returns_float(self):
        assert isinstance(ShortProfile(ticker="X").squeeze_score(), float)


class TestToRedisMapping:

    def test_all_values_are_strings(self):
        p = ShortProfile(
            ticker="GME", float_shares=5e6, price=15.50, beta=1.8,
            updated_at=1_700_000_000,
        )
        mapping = p.to_redis_mapping()
        assert all(isinstance(v, str) for v in mapping.values()), \
            "Every Redis value must be a str"

    def test_ticker_preserved(self):
        mapping = ShortProfile(ticker="AMC").to_redis_mapping()
        assert mapping["ticker"] == "AMC"

    def test_float_fields_round_trip_via_str(self):
        p = ShortProfile(ticker="X", short_float_pct=0.2345)
        mapping = p.to_redis_mapping()
        assert float(mapping["short_float_pct"]) == pytest.approx(0.2345)

    def test_all_dataclass_fields_present(self):
        p = ShortProfile(ticker="X")
        mapping = p.to_redis_mapping()
        from dataclasses import fields
        for f in fields(ShortProfile):
            assert f.name in mapping, f"Field '{f.name}' missing from Redis mapping"


class TestFromRedis:

    def _roundtrip(self, **kwargs) -> ShortProfile:
        p   = ShortProfile(ticker="GME", **kwargs)
        raw = p.to_redis_mapping()
        return ShortProfile.from_redis(raw)

    def test_ticker_string_preserved(self):
        assert self._roundtrip().ticker == "GME"

    def test_float_fields_typed_as_float(self):
        r = self._roundtrip(float_shares=5_000_000.0, short_float_pct=0.30)
        assert isinstance(r.float_shares, float)
        assert isinstance(r.short_float_pct, float)

    def test_float_values_accurate(self):
        r = self._roundtrip(
            float_shares=5_000_000,
            short_interest=1_500_000,
            short_float_pct=0.3000,
            short_ratio=3.5,
            short_vol_ratio=0.48,
            price=15.50,
            beta=1.8,
        )
        assert r.float_shares    == pytest.approx(5_000_000.0)
        assert r.short_float_pct == pytest.approx(0.3000)
        assert r.short_ratio     == pytest.approx(3.5)
        assert r.price           == pytest.approx(15.50)

    def test_updated_at_typed_as_int(self):
        r = self._roundtrip(updated_at=1_700_000_000)
        assert isinstance(r.updated_at, int)
        assert r.updated_at == 1_700_000_000

    def test_data_source_string_preserved(self):
        r = self._roundtrip(data_source="yfinance+finra_regsho")
        assert r.data_source == "yfinance+finra_regsho"

    def test_unknown_keys_ignored(self):
        """Extra keys in Redis hash should not crash from_redis."""
        raw = ShortProfile(ticker="X").to_redis_mapping()
        raw["unexpected_future_field"] = "42"
        p = ShortProfile.from_redis(raw)  # must not raise
        assert p.ticker == "X"

    def test_missing_ticker_falls_back_to_empty_string(self):
        raw = ShortProfile(ticker="X").to_redis_mapping()
        del raw["ticker"]
        # Should not raise; ticker comes from raw.get("ticker", "")
        p = ShortProfile.from_redis(raw)
        assert p.ticker == ""

    def test_zero_values_round_trip(self):
        r = self._roundtrip(
            float_shares=0.0, short_interest=0.0, short_float_pct=0.0
        )
        assert r.float_shares   == pytest.approx(0.0)
        assert r.short_interest == pytest.approx(0.0)


# ═════════════════════════════════════════════════════════
#  2. _regsho_date_str
# ═════════════════════════════════════════════════════════

class TestRegshoDatStr:

    def _make_date(self, weekday: int, offset: int = 0) -> date:
        """
        Return a date that falls on `weekday` (0=Mon … 6=Sun).
        Start from a known Monday (2025-04-21) and step forward.
        """
        base = date(2025, 4, 21)   # known Monday
        d    = base + timedelta(days=weekday)
        return d - timedelta(days=offset)

    def test_monday_returned_as_is(self):
        d = date(2025, 4, 21)   # Monday
        with patch("ingestion.short_data.date") as mock_date:
            mock_date.today.return_value = d
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            result = _regsho_date_str(0)
        assert result == "20250421"

    def test_friday_returned_as_is(self):
        d = date(2025, 4, 25)   # Friday
        with patch("ingestion.short_data.date") as mock_date:
            mock_date.today.return_value = d
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            result = _regsho_date_str(0)
        assert result == "20250425"

    def test_saturday_steps_back_to_friday(self):
        d = date(2025, 4, 26)   # Saturday
        with patch("ingestion.short_data.date") as mock_date:
            mock_date.today.return_value = d
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            result = _regsho_date_str(0)
        assert result == "20250425"   # Friday

    def test_sunday_steps_back_to_friday(self):
        d = date(2025, 4, 27)   # Sunday
        with patch("ingestion.short_data.date") as mock_date:
            mock_date.today.return_value = d
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            result = _regsho_date_str(0)
        assert result == "20250425"   # Friday

    def test_offset_subtracts_days(self):
        d = date(2025, 4, 23)   # Wednesday
        with patch("ingestion.short_data.date") as mock_date:
            mock_date.today.return_value = d
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            result = _regsho_date_str(2)   # Wednesday minus 2 = Monday
        assert result == "20250421"

    def test_offset_landing_on_weekend_adjusts(self):
        d = date(2025, 4, 28)   # Monday
        with patch("ingestion.short_data.date") as mock_date:
            mock_date.today.return_value = d
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            # offset=2 → Saturday → adjust to Friday
            result = _regsho_date_str(2)
        assert result == "20250425"

    def test_returns_eight_digit_string(self):
        with patch("ingestion.short_data.date") as mock_date:
            mock_date.today.return_value = date(2025, 1, 6)  # Monday
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            result = _regsho_date_str(0)
        assert len(result) == 8
        assert result.isdigit()


# ═════════════════════════════════════════════════════════
#  3. fetch_regsho_short_vol_ratio
# ═════════════════════════════════════════════════════════

class TestFetchRegSHOShortVolRatio:
    """
    All tests patch both `requests.get` and `_regsho_date_str` so there is
    no network access and no calendar dependency.
    """

    _DATE = "20250421"

    def _patch_date(self):
        return patch(
            "ingestion.short_data._regsho_date_str",
            return_value=self._DATE,
        )

    def test_happy_path_correct_ratio(self):
        csv = regsho_csv([("GME", 300_000, 600_000)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert ratio == pytest.approx(0.5, abs=0.0001)

    def test_ticker_uppercased_before_lookup(self):
        csv = regsho_csv([("AAPL", 400_000, 1_000_000)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                ratio = fetch_regsho_short_vol_ratio("aapl")
        assert ratio == pytest.approx(0.4, abs=0.0001)

    def test_ticker_not_in_file_returns_zero(self):
        csv = regsho_csv([("AAPL", 400_000, 1_000_000)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                ratio = fetch_regsho_short_vol_ratio("ZZNOTREAL")
        assert ratio == pytest.approx(0.0)

    def test_http_404_retries_next_date(self):
        """First date returns 404; second date has real data."""
        csv = regsho_csv([("GME", 300_000, 1_000_000)])
        responses = [
            mock_response(404),
            mock_response(200, csv),
        ]
        dates = ["20250421", "20250418"]   # Mon, then prior Fri
        with patch("ingestion.short_data._regsho_date_str",
                   side_effect=dates):
            with patch("requests.get", side_effect=responses):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert ratio == pytest.approx(0.3, abs=0.0001)

    def test_all_four_dates_return_404_gives_zero(self):
        with patch(
            "ingestion.short_data._regsho_date_str",
            side_effect=["20250421", "20250420", "20250418", "20250417"],
        ):
            with patch("requests.get", return_value=mock_response(404)):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert ratio == pytest.approx(0.0)

    def test_non_200_non_404_continues_to_next(self):
        """500 error → try next date."""
        csv = regsho_csv([("GME", 150_000, 500_000)])
        responses = [mock_response(500), mock_response(200, csv)]
        dates = ["20250421", "20250418"]
        with patch("ingestion.short_data._regsho_date_str",
                   side_effect=dates):
            with patch("requests.get", side_effect=responses):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert ratio == pytest.approx(0.3, abs=0.0001)

    def test_network_exception_returns_zero(self):
        with self._patch_date():
            with patch("requests.get", side_effect=Exception("connection refused")):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert ratio == pytest.approx(0.0)

    def test_zero_total_volume_returns_zero(self):
        csv = regsho_csv([("GME", 0, 0)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert ratio == pytest.approx(0.0)

    def test_ratio_is_short_vol_over_total_vol(self):
        """Verify the formula: ShortVolume / TotalVolume."""
        csv = regsho_csv([("SPY", 700_000, 2_000_000)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                ratio = fetch_regsho_short_vol_ratio("SPY")
        assert ratio == pytest.approx(700_000 / 2_000_000, abs=0.0001)

    def test_ratio_between_zero_and_one(self):
        csv = regsho_csv([("QQQ", 450_000, 1_000_000)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                ratio = fetch_regsho_short_vol_ratio("QQQ")
        assert 0.0 <= ratio <= 1.0

    def test_whitespace_in_symbol_column_stripped(self):
        """Symbols sometimes have leading/trailing spaces in the file."""
        raw_csv = (
            "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n"
            "20250421| GME |300000|0|600000|CNMS\n"
        )
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, raw_csv)):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert ratio == pytest.approx(0.5, abs=0.0001)

    def test_missing_columns_falls_through_to_zero(self):
        """If the file has no recognisable symbol/short/total columns, return 0."""
        bad_csv = "ColA|ColB|ColC\nfoo|bar|baz\n"
        # Only 1 date attempt before columns check causes a `continue` then all 404
        with patch(
            "ingestion.short_data._regsho_date_str",
            side_effect=["20250421", "20250418", "20250417", "20250416"],
        ):
            with patch("requests.get", return_value=mock_response(200, bad_csv)):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert ratio == pytest.approx(0.0)

    def test_multiple_tickers_in_file_picks_correct_one(self):
        csv = regsho_csv([
            ("AAPL", 200_000, 1_000_000),
            ("GME",  300_000,   600_000),
            ("AMC",  100_000,   400_000),
        ])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert ratio == pytest.approx(0.5, abs=0.0001)

    def test_returns_float(self):
        csv = regsho_csv([("GME", 300_000, 600_000)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                ratio = fetch_regsho_short_vol_ratio("GME")
        assert isinstance(ratio, float)


# ═════════════════════════════════════════════════════════
#  4. ShortDataClient._fetch_yfinance_info
# ═════════════════════════════════════════════════════════

class TestFetchYFinanceInfo:

    def setup_method(self):
        self.client = make_client(make_fake_redis())

    def _mock_yf(self, info_dict):
        ticker_mock      = MagicMock()
        ticker_mock.info = info_dict
        return patch("ingestion.short_data.yf.Ticker",
                     return_value=ticker_mock)

    def test_happy_path_returns_info_dict(self):
        info = full_yf_info()
        with self._mock_yf(info):
            result = self.client._fetch_yfinance_info("GME")
        assert result["floatShares"] == info["floatShares"]
        assert result["sharesShort"] == info["sharesShort"]

    def test_exception_returns_empty_dict(self):
        with patch("ingestion.short_data.yf.Ticker",
                   side_effect=Exception("rate limited")):
            result = self.client._fetch_yfinance_info("GME")
        assert result == {}

    def test_non_dict_info_returns_empty_dict(self):
        """yfinance occasionally returns None or a string on error."""
        ticker_mock      = MagicMock()
        ticker_mock.info = None
        with patch("ingestion.short_data.yf.Ticker",
                   return_value=ticker_mock):
            result = self.client._fetch_yfinance_info("GME")
        assert result == {}

    def test_ticker_passed_to_yf(self):
        with self._mock_yf({}):
            self.client._fetch_yfinance_info("TSLA")
        import ingestion.short_data as sd
        # Simply verify the call didn't crash — the ticker is passed through yf.Ticker


# ═════════════════════════════════════════════════════════
#  5. ShortDataClient._fetch_finnhub_metrics
# ═════════════════════════════════════════════════════════

class TestFetchFinnhubMetrics:

    def setup_method(self):
        self.client = make_client(make_fake_redis())

    def test_no_api_key_returns_empty_without_http_call(self):
        import ingestion.short_data as sd
        original = sd._api.finnhub_api_key
        sd._api.finnhub_api_key = ""
        try:
            with patch("requests.get") as mock_get:
                result = self.client._fetch_finnhub_metrics("AAPL")
            mock_get.assert_not_called()
            assert result == {}
        finally:
            sd._api.finnhub_api_key = original

    def test_happy_path_returns_metric_dict(self):
        import ingestion.short_data as sd
        sd._api.finnhub_api_key = "test_key"
        payload  = {"metric": {"beta": 1.5, "52WeekHigh": 200.0}}
        resp_mock = mock_response(200)
        resp_mock.json.return_value = payload
        with patch("requests.get", return_value=resp_mock):
            result = self.client._fetch_finnhub_metrics("AAPL")
        assert result["beta"] == pytest.approx(1.5)
        sd._api.finnhub_api_key = ""

    def test_non_200_returns_empty_dict(self):
        import ingestion.short_data as sd
        sd._api.finnhub_api_key = "test_key"
        with patch("requests.get", return_value=mock_response(403)):
            result = self.client._fetch_finnhub_metrics("AAPL")
        assert result == {}
        sd._api.finnhub_api_key = ""

    def test_network_exception_returns_empty_dict(self):
        import ingestion.short_data as sd
        sd._api.finnhub_api_key = "test_key"
        with patch("requests.get", side_effect=Exception("timeout")):
            result = self.client._fetch_finnhub_metrics("AAPL")
        assert result == {}
        sd._api.finnhub_api_key = ""


# ═════════════════════════════════════════════════════════
#  6. ShortDataClient._build_profile
# ═════════════════════════════════════════════════════════

class TestBuildProfile:

    def setup_method(self):
        self.client = make_client(make_fake_redis())
        # By default, no Finnhub key so _fetch_finnhub_metrics returns {}
        import ingestion.short_data as sd
        self._orig_key = sd._api.finnhub_api_key
        sd._api.finnhub_api_key = ""

    def teardown_method(self):
        import ingestion.short_data as sd
        sd._api.finnhub_api_key = self._orig_key

    def _build(self, yf_info: dict, regsho_ratio: float = 0.0,
               include_regsho: bool = False) -> ShortProfile:
        self.client._fetch_yfinance_info    = MagicMock(return_value=yf_info)
        self.client._fetch_finnhub_metrics  = MagicMock(return_value={})
        with patch(
            "ingestion.short_data.fetch_regsho_short_vol_ratio",
            return_value=regsho_ratio,
        ):
            return self.client._build_profile("GME", include_regsho=include_regsho)

    # ── float_shares / shares_outstanding ─────────────────

    def test_float_shares_from_yfinance(self):
        p = self._build(full_yf_info(float_shares=5_000_000))
        assert p.float_shares == pytest.approx(5_000_000.0)

    def test_shares_outstanding_from_yfinance(self):
        p = self._build(full_yf_info(shares_outstanding=76_000_000))
        assert p.shares_outstanding == pytest.approx(76_000_000.0)

    # ── short interest ────────────────────────────────────

    def test_short_interest_from_shares_short(self):
        p = self._build(full_yf_info(shares_short=1_500_000))
        assert p.short_interest == pytest.approx(1_500_000.0)

    def test_short_ratio_used_directly_when_present(self):
        p = self._build(full_yf_info(short_ratio=3.5))
        assert p.short_ratio == pytest.approx(3.5)

    def test_short_ratio_calculated_when_missing(self):
        """When shortRatio is absent, calculate from short_interest / avg_vol."""
        info = full_yf_info(shares_short=420_000, avg_vol=42_000)
        info.pop("shortRatio", None)
        info["shortRatio"] = None   # simulate yfinance returning None
        p = self._build(info)
        # 420_000 / 42_000 = 10.0
        assert p.short_ratio == pytest.approx(10.0)

    def test_short_float_pct_from_yfinance_field(self):
        p = self._build(full_yf_info(short_pct_float=0.30))
        assert p.short_float_pct == pytest.approx(0.30)

    def test_short_float_pct_calculated_when_field_missing(self):
        """Use short_interest / float_shares when shortPercentOfFloat is absent."""
        info = full_yf_info(
            shares_short=1_000_000,
            float_shares=4_000_000,
        )
        info["shortPercentOfFloat"] = None   # simulate absent
        p = self._build(info)
        assert p.short_float_pct == pytest.approx(0.25, abs=0.0001)

    # ── price fallback chain ──────────────────────────────

    def test_price_uses_current_price(self):
        info = full_yf_info(price=15.50)
        p = self._build(info)
        assert p.price == pytest.approx(15.50)

    def test_price_falls_back_to_regular_market_price(self):
        info = full_yf_info()
        info.pop("currentPrice", None)
        info["currentPrice"]       = None
        info["regularMarketPrice"] = 16.00
        p = self._build(info)
        assert p.price == pytest.approx(16.00)

    def test_price_falls_back_to_previous_close(self):
        info = full_yf_info()
        info["currentPrice"]       = None
        info["regularMarketPrice"] = None
        info["previousClose"]      = 14.75
        p = self._build(info)
        assert p.price == pytest.approx(14.75)

    def test_price_defaults_to_zero_when_all_missing(self):
        info = full_yf_info()
        info["currentPrice"]       = None
        info["regularMarketPrice"] = None
        info["previousClose"]      = None
        p = self._build(info)
        assert p.price == pytest.approx(0.0)

    # ── beta fallback ─────────────────────────────────────

    def test_beta_from_yfinance(self):
        p = self._build(full_yf_info(beta=2.1))
        assert p.beta == pytest.approx(2.1)

    def test_beta_falls_back_to_finnhub(self):
        info = full_yf_info()
        info["beta"] = None
        self.client._fetch_yfinance_info   = MagicMock(return_value=info)
        self.client._fetch_finnhub_metrics = MagicMock(return_value={"beta": 1.9})
        with patch(
            "ingestion.short_data.fetch_regsho_short_vol_ratio",
            return_value=0.0,
        ):
            p = self.client._build_profile("GME", include_regsho=False)
        assert p.beta == pytest.approx(1.9)

    def test_beta_defaults_to_one_when_both_missing(self):
        info = full_yf_info()
        info["beta"] = None
        p = self._build(info)
        assert p.beta == pytest.approx(1.0)

    # ── RegSHO ────────────────────────────────────────────

    def test_include_regsho_true_calls_fetch(self):
        with patch(
            "ingestion.short_data.fetch_regsho_short_vol_ratio",
            return_value=0.48,
        ) as mock_regsho:
            self.client._fetch_yfinance_info   = MagicMock(return_value=full_yf_info())
            self.client._fetch_finnhub_metrics = MagicMock(return_value={})
            p = self.client._build_profile("GME", include_regsho=True)
        mock_regsho.assert_called_once_with("GME")
        assert p.short_vol_ratio == pytest.approx(0.48)

    def test_include_regsho_false_skips_fetch(self):
        with patch(
            "ingestion.short_data.fetch_regsho_short_vol_ratio",
        ) as mock_regsho:
            self.client._fetch_yfinance_info   = MagicMock(return_value=full_yf_info())
            self.client._fetch_finnhub_metrics = MagicMock(return_value={})
            p = self.client._build_profile("GME", include_regsho=False)
        mock_regsho.assert_not_called()
        assert p.short_vol_ratio == pytest.approx(0.0)

    # ── data_source label ─────────────────────────────────

    def test_data_source_contains_yfinance(self):
        p = self._build(full_yf_info())
        assert "yfinance" in p.data_source

    def test_data_source_adds_finra_regsho_when_svr_nonzero(self):
        p = self._build(full_yf_info(), regsho_ratio=0.45, include_regsho=True)
        assert "finra_regsho" in p.data_source

    def test_data_source_no_finra_when_svr_zero(self):
        p = self._build(full_yf_info(), regsho_ratio=0.0, include_regsho=True)
        assert "finra_regsho" not in p.data_source

    def test_data_source_adds_finnhub_when_metrics_returned(self):
        self.client._fetch_yfinance_info   = MagicMock(return_value=full_yf_info())
        self.client._fetch_finnhub_metrics = MagicMock(return_value={"beta": 1.5})
        with patch(
            "ingestion.short_data.fetch_regsho_short_vol_ratio",
            return_value=0.0,
        ):
            p = self.client._build_profile("GME", include_regsho=False)
        assert "finnhub" in p.data_source

    # ── empty info dict ───────────────────────────────────

    def test_empty_yfinance_info_all_numerics_zero(self):
        p = self._build({})
        for field in (
            "float_shares", "shares_outstanding", "short_interest",
            "market_cap", "avg_volume_10d",
        ):
            assert getattr(p, field) == pytest.approx(0.0), \
                f"{field} should be 0.0 when yfinance returns {{}}"

    def test_empty_info_borrow_rate_is_zero(self):
        p = self._build({})
        assert p.borrow_rate_pct == pytest.approx(0.0)

    def test_borrow_rate_always_zero_no_free_source(self):
        """borrow_rate_pct has no free data source — always 0.0."""
        p = self._build(full_yf_info())
        assert p.borrow_rate_pct == pytest.approx(0.0)

    # ── ownership ─────────────────────────────────────────

    def test_institutional_ownership_from_yfinance(self):
        p = self._build(full_yf_info(inst_pct=0.35))
        assert p.inst_ownership_pct == pytest.approx(0.35)

    def test_insider_ownership_from_yfinance(self):
        p = self._build(full_yf_info(insider_pct=0.12))
        assert p.insider_ownership_pct == pytest.approx(0.12)

    # ── ticker preserved ──────────────────────────────────

    def test_ticker_stored_in_profile(self):
        self.client._fetch_yfinance_info   = MagicMock(return_value={})
        self.client._fetch_finnhub_metrics = MagicMock(return_value={})
        with patch(
            "ingestion.short_data.fetch_regsho_short_vol_ratio",
            return_value=0.0,
        ):
            p = self.client._build_profile("GME", include_regsho=False)
        assert p.ticker == "GME"


# ═════════════════════════════════════════════════════════
#  7. Redis cache  (_save_cache / _load_cache)
# ═════════════════════════════════════════════════════════

class TestRedisCache:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.client = make_client(self.fake_r)

    def _sample_profile(self, ticker: str = "GME") -> ShortProfile:
        return ShortProfile(
            ticker=ticker,
            float_shares=5_000_000,
            shares_outstanding=76_000_000,
            short_interest=1_500_000,
            short_float_pct=0.30,
            short_ratio=3.5,
            short_vol_ratio=0.48,
            shares_short_prior=1_200_000,
            borrow_rate_pct=0.0,
            inst_ownership_pct=0.35,
            insider_ownership_pct=0.12,
            market_cap=800_000_000,
            price=15.50,
            beta=1.8,
            avg_volume_10d=420_000,
            data_source="yfinance+finra_regsho",
            updated_at=1_700_000_000,
        )

    def test_save_creates_redis_key(self):
        p = self._sample_profile("GME")
        self.client._save_cache(p)
        assert self.fake_r.exists("short:GME")

    def test_load_returns_none_for_missing_key(self):
        assert self.client._load_cache("ZZNOTREAL") is None

    def test_round_trip_ticker(self):
        p = self._sample_profile("AMC")
        self.client._save_cache(p)
        loaded = self.client._load_cache("AMC")
        assert loaded is not None
        assert loaded.ticker == "AMC"

    def test_round_trip_float_fields(self):
        p = self._sample_profile("GME")
        self.client._save_cache(p)
        loaded = self.client._load_cache("GME")
        assert loaded.float_shares    == pytest.approx(5_000_000.0)
        assert loaded.short_float_pct == pytest.approx(0.30)
        assert loaded.short_ratio     == pytest.approx(3.5)
        assert loaded.price           == pytest.approx(15.50)
        assert loaded.beta            == pytest.approx(1.8)

    def test_round_trip_int_fields(self):
        p = self._sample_profile("GME")
        self.client._save_cache(p)
        loaded = self.client._load_cache("GME")
        assert loaded.updated_at == 1_700_000_000
        assert isinstance(loaded.updated_at, int)

    def test_round_trip_string_fields(self):
        p = self._sample_profile("GME")
        self.client._save_cache(p)
        loaded = self.client._load_cache("GME")
        assert loaded.data_source == "yfinance+finra_regsho"

    def test_ttl_is_4_hours(self):
        p = self._sample_profile("GME")
        self.client._save_cache(p)
        ttl = self.fake_r.ttl("short:GME")
        assert 14_390 <= ttl <= 14_400

    def test_key_uses_uppercase_ticker(self):
        p = self._sample_profile("gme")   # lowercase
        self.client._save_cache(p)
        # The profile ticker is whatever was passed; the Redis key uses it directly
        # Load should still work with uppercase
        assert self.fake_r.exists("short:gme")   # key matches profile.ticker

    def test_load_uses_uppercase_key(self):
        """_load_cache always looks up f'short:{ticker.upper()}'."""
        p = self._sample_profile("GME")
        self.client._save_cache(p)
        # Load using lowercase — should still find it because _load_cache uppercases
        loaded = self.client._load_cache("gme")
        assert loaded is not None

    def test_load_corrupt_data_returns_none(self):
        """If Redis contains junk, from_redis may raise ValueError → return None."""
        self.fake_r.hset("short:BAD", mapping={"ticker": "BAD", "float_shares": "not_a_number"})
        result = self.client._load_cache("BAD")
        # Should return None (exception caught internally)
        assert result is None

    def test_overwrite_on_second_save(self):
        p1 = self._sample_profile("GME")
        p1.price = 10.00
        self.client._save_cache(p1)

        p2 = self._sample_profile("GME")
        p2.price = 20.00
        self.client._save_cache(p2)

        loaded = self.client._load_cache("GME")
        assert loaded.price == pytest.approx(20.00)


# ═════════════════════════════════════════════════════════
#  8. ShortDataClient.get_short_profile
# ═════════════════════════════════════════════════════════

class TestGetShortProfile:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.client = make_client(self.fake_r)

    def _stub_build(self, ticker: str = "GME", **kwargs) -> ShortProfile:
        profile = ShortProfile(
            ticker=ticker.upper(),
            float_shares=5_000_000,
            short_float_pct=0.30,
            **kwargs,
        )
        self.client._build_profile = MagicMock(return_value=profile)
        return profile

    def test_cache_miss_calls_build_and_caches_result(self):
        expected = self._stub_build("GME")
        result   = self.client.get_short_profile("GME", include_regsho=False)
        self.client._build_profile.assert_called_once_with("GME", include_regsho=False)
        assert result.ticker == "GME"
        # Should now be in cache
        assert self.fake_r.exists("short:GME")

    def test_cache_hit_returns_cached_without_rebuild(self):
        expected = self._stub_build("GME")
        # Seed cache manually
        self.client._save_cache(expected)
        result = self.client.get_short_profile("GME", include_regsho=False)
        self.client._build_profile.assert_not_called()
        assert result.ticker == "GME"

    def test_force_refresh_bypasses_cache(self):
        expected = self._stub_build("GME")
        # Seed cache
        self.client._save_cache(expected)
        # force_refresh should still call _build_profile
        self.client.get_short_profile("GME", force_refresh=True, include_regsho=False)
        self.client._build_profile.assert_called_once()

    def test_ticker_uppercased(self):
        self._stub_build("GME")
        result = self.client.get_short_profile("gme", include_regsho=False)
        assert result.ticker == "GME"

    def test_returns_short_profile_instance(self):
        self._stub_build("TSLA")
        result = self.client.get_short_profile("TSLA", include_regsho=False)
        assert isinstance(result, ShortProfile)


# ═════════════════════════════════════════════════════════
#  9. ShortDataClient.get_bulk_profiles
# ═════════════════════════════════════════════════════════

class TestGetBulkProfiles:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.client = make_client(self.fake_r)

    def _stub_get_profile(self, profile_map: dict[str, ShortProfile]):
        """Replace get_short_profile with a stub returning from profile_map."""
        def _fake_get(ticker, force_refresh=False, include_regsho=False):
            return profile_map[ticker.upper()]
        self.client.get_short_profile = _fake_get

    def _make_profile(self, ticker: str, sf_pct: float) -> ShortProfile:
        return ShortProfile(ticker=ticker, short_float_pct=sf_pct)

    def test_returns_dict_keyed_by_uppercase_ticker(self):
        pmap = {
            "GME":  self._make_profile("GME",  0.30),
            "AMC":  self._make_profile("AMC",  0.15),
            "TSLA": self._make_profile("TSLA", 0.05),
        }
        self._stub_get_profile(pmap)
        result = self.client.get_bulk_profiles(["gme", "amc", "tsla"])
        assert set(result.keys()) == {"GME", "AMC", "TSLA"}

    def test_each_value_is_short_profile(self):
        pmap = {"AAPL": self._make_profile("AAPL", 0.01)}
        self._stub_get_profile(pmap)
        result = self.client.get_bulk_profiles(["AAPL"])
        assert isinstance(result["AAPL"], ShortProfile)

    def test_include_regsho_false_propagated(self):
        calls = []

        def _fake_get(ticker, force_refresh=False, include_regsho=False):
            calls.append(include_regsho)
            return ShortProfile(ticker=ticker.upper())

        self.client.get_short_profile = _fake_get
        self.client.get_bulk_profiles(["GME", "AMC"], include_regsho=False)
        assert all(c is False for c in calls)

    def test_empty_list_returns_empty_dict(self):
        result = self.client.get_bulk_profiles([])
        assert result == {}


# ═════════════════════════════════════════════════════════
#  10. ShortDataClient.get_top_short_float
# ═════════════════════════════════════════════════════════

class TestGetTopShortFloat:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.client = make_client(self.fake_r)

    def _stub_bulk(self, sf_map: dict[str, float]):
        profiles = {t: ShortProfile(ticker=t, short_float_pct=sf)
                    for t, sf in sf_map.items()}
        self.client.get_bulk_profiles = MagicMock(return_value=profiles)

    def test_sorted_descending_by_short_float_pct(self):
        self._stub_bulk({"GME": 0.30, "AMC": 0.15, "TSLA": 0.05})
        top = self.client.get_top_short_float(["GME", "AMC", "TSLA"], top_n=3)
        assert [p.ticker for p in top] == ["GME", "AMC", "TSLA"]

    def test_top_n_limits_results(self):
        self._stub_bulk({"GME": 0.30, "AMC": 0.15, "TSLA": 0.05})
        top = self.client.get_top_short_float(["GME", "AMC", "TSLA"], top_n=2)
        assert len(top) == 2
        assert top[0].ticker == "GME"
        assert top[1].ticker == "AMC"

    def test_top_n_larger_than_list_returns_all(self):
        self._stub_bulk({"GME": 0.30, "AMC": 0.15})
        top = self.client.get_top_short_float(["GME", "AMC"], top_n=10)
        assert len(top) == 2

    def test_returns_list_of_short_profiles(self):
        self._stub_bulk({"GME": 0.30})
        top = self.client.get_top_short_float(["GME"], top_n=1)
        assert isinstance(top, list)
        assert isinstance(top[0], ShortProfile)

    def test_empty_list_returns_empty(self):
        self._stub_bulk({})
        top = self.client.get_top_short_float([], top_n=5)
        assert top == []


# ═════════════════════════════════════════════════════════
#  11. ShortDataClient.fetch_regsho_batch
# ═════════════════════════════════════════════════════════

class TestFetchRegSHOBatch:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.client = make_client(self.fake_r)

    _DATE = "20250421"

    def _patch_date(self):
        return patch(
            "ingestion.short_data._regsho_date_str",
            return_value=self._DATE,
        )

    def test_happy_path_all_tickers_found(self):
        csv = regsho_csv([
            ("GME",  300_000, 600_000),
            ("AMC",  100_000, 400_000),
            ("TSLA", 500_000, 1_000_000),
        ])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                result = self.client.fetch_regsho_batch(["GME", "AMC", "TSLA"])
        assert result["GME"]  == pytest.approx(0.5, abs=0.0001)
        assert result["AMC"]  == pytest.approx(0.25, abs=0.0001)
        assert result["TSLA"] == pytest.approx(0.5, abs=0.0001)

    def test_partial_match_missing_ticker_is_zero(self):
        csv = regsho_csv([("GME", 300_000, 600_000)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                result = self.client.fetch_regsho_batch(["GME", "ZZNOTREAL"])
        assert result["GME"]      == pytest.approx(0.5, abs=0.0001)
        assert result["ZZNOTREAL"] == pytest.approx(0.0)

    def test_tickers_uppercased_in_result_keys(self):
        csv = regsho_csv([("GME", 300_000, 600_000)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                result = self.client.fetch_regsho_batch(["gme"])
        assert "GME" in result

    def test_http_404_retries_next_date(self):
        csv = regsho_csv([("GME", 200_000, 500_000)])
        responses = [mock_response(404), mock_response(200, csv)]
        dates     = ["20250421", "20250418"]
        with patch("ingestion.short_data._regsho_date_str",
                   side_effect=dates):
            with patch("requests.get", side_effect=responses):
                result = self.client.fetch_regsho_batch(["GME"])
        assert result["GME"] == pytest.approx(0.4, abs=0.0001)

    def test_all_404_returns_all_zeros(self):
        with patch(
            "ingestion.short_data._regsho_date_str",
            side_effect=["20250421", "20250418", "20250417", "20250416"],
        ):
            with patch("requests.get", return_value=mock_response(404)):
                result = self.client.fetch_regsho_batch(["GME", "AMC"])
        assert result["GME"] == pytest.approx(0.0)
        assert result["AMC"] == pytest.approx(0.0)

    def test_zero_total_vol_row_gives_zero_ratio(self):
        csv = regsho_csv([("GME", 0, 0)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                result = self.client.fetch_regsho_batch(["GME"])
        assert result["GME"] == pytest.approx(0.0)

    def test_empty_ticker_list_returns_empty_dict(self):
        with self._patch_date():
            with patch("requests.get") as mock_get:
                result = self.client.fetch_regsho_batch([])
        mock_get.assert_not_called()
        assert result == {}

    def test_returns_all_requested_tickers_as_keys(self):
        csv = regsho_csv([("GME", 300_000, 600_000)])
        tickers = ["GME", "AAPL", "NVDA"]
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                result = self.client.fetch_regsho_batch(tickers)
        assert set(result.keys()) == {"GME", "AAPL", "NVDA"}

    def test_all_values_are_floats(self):
        csv = regsho_csv([("GME", 300_000, 600_000)])
        with self._patch_date():
            with patch("requests.get", return_value=mock_response(200, csv)):
                result = self.client.fetch_regsho_batch(["GME", "ZZNOTREAL"])
        assert all(isinstance(v, float) for v in result.values())


# ─────────────────────────────────────────────────────────
#  Run directly
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Allow running directly: python tests/test_price_feed.py
    import subprocess, sys
    sys.exit(subprocess.call(
        [sys.executable, "-m", "pytest", __file__, "-v", "--tb=short"],
    ))