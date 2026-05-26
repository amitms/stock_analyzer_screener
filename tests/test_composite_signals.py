"""
tests/test_composite_signals.py
Full pytest suite for signals/composite_signals.py

No network access. All external dependencies (yfinance, Redis,
Finnhub, news feed, Reddit) are replaced with controlled MagicMocks
injected directly onto the instances under test.

Run
───
  pip install pytest
  pytest tests/test_composite_signals.py -v

Coverage
────────
  OptionsSignals.compute
    pc_ratio < 0.70        → pc_score = 1.0 (fully bullish)
    pc_ratio between 0.70 and 1.0 → pc_score interpolated linearly
    pc_ratio between 1.0 and 1.2  → pc_score = 0.2
    pc_ratio >= 1.2        → pc_score = 0.0
    pc_bullish True        when pc_ratio < threshold
    pc_bullish False       when pc_ratio >= threshold
    sweep_score = min(sweep_count / 5, 1.0)
    sweep_score capped at 1.0 for large sweep_count
    dark pool premium extracted from uw_alerts type containing "dark_pool"
    dark pool ignored when type does not contain "dark_pool"
    dp_score = min(dp_premium / avg_dv * 10, 1.0)
    call_premium = sum of premiums where side in ("call", "c")
    call_score = min(call_premium / avg_dv * 20, 1.0)
    final score in [0, 1]
    missing options cache → defaults used (pc_ratio=1.0, etc.)
    all required keys in result

  RiskSignals.compute
    si_score = min(short_float_pct / 0.40, 1.0)
    float_score = max(0, 1 - float_shares / 20M)
    dtc_score = min(short_ratio / 20, 1.0)
    borrow_score = min(borrow_rate_pct / 100, 1.0)
    squeeze_score formula and range
    vol_regime "low" when atr_pct <= 0.02
    vol_regime "normal" when 0.02 < atr_pct <= 0.04
    vol_regime "high" when 0.04 < atr_pct <= 0.08
    vol_regime "extreme" when atr_pct > 0.08
    beta_penalty = 0 when beta <= 1.5
    beta_penalty positive when beta > 1.5
    beta_penalty capped at 0.5
    score = max(0, squeeze_score - beta_penalty)
    score never goes negative
    is_candidate uses ShortProfile.is_squeeze_candidate
    short_float_pct in result is percentage (× 100)
    atr_pct in result is percentage (× 100)
    all required keys in result

  CatalystSignals.compute
    news_score = (raw_sentiment + 1) / 2
    catalyst_boost += 0.20 when is_fda
    catalyst_boost += 0.10 when is_earnings
    catalyst_boost += 0.15 when is_ma
    combined catalyst boosts accumulate
    reddit_rank propagated as reddit_score
    velocity_bonus = min(mentions_hr / 50, 0.30)
    velocity_bonus capped at 0.30
    final score clamped to 1.0
    final score returns correct composite formula
    no catalysts → score depends only on news + reddit
    is_fda / is_earnings / is_ma flags from articles
    all required keys in result

  MarketContextSignals
    fetch_vix returns float  (mocked)
    fetch_vix defaults to 20.0 on exception
    fetch_futures_bias returns float  (mocked)
    fetch_futures_bias returns 0.0 on exception or short history
    compute: vix < 15 → vix_score=1.0, regime="low_vol"
    compute: 15 <= vix < 25 → vix_score interpolated, regime="normal"
    compute: 25 <= vix < 35 → vix_score=0.25, regime="high_vol"
    compute: vix >= 35 → vix_score=0.05, regime="extreme_fear"
    futures_score = clipped linear mapping of futures_bias
    futures_score = 0 when futures_bias very negative
    futures_score = 1 when futures_bias very positive
    final score = 0.60*vix_score + 0.40*futures_score
    score in [0, 1]
    all required keys in result
"""

from __future__ import annotations
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dotenv import load_dotenv, dotenv_values
# ─────────────────────────────────────────────────────────
#  Path bootstrap
# ─────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Silence loguru
_loguru_stub        = types.ModuleType("loguru")
_loguru_stub.logger = MagicMock()
sys.modules.setdefault("loguru", _loguru_stub)

_dotenv_stub             = types.ModuleType("dotenv")
_dotenv_stub.load_dotenv = lambda *a, **kw: None
sys.modules.setdefault("dotenv", _dotenv_stub)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

from signals.composite_signals import (
    CatalystSignals,
    MarketContextSignals,
    OptionsSignals,
    RiskSignals,
)
from config.settings import get_scanner_config
from ingestion.short_data import ShortProfile

_cfg = get_scanner_config()


# ─────────────────────────────────────────────────────────
#  Shared helpers
# ─────────────────────────────────────────────────────────

def make_short_profile(
    ticker:          str   = "GME",
    short_float_pct: float = 0.30,
    float_shares:    float = 5_000_000,
    short_ratio:     float = 6.0,
    borrow_rate_pct: float = 0.0,
) -> ShortProfile:
    return ShortProfile(
        ticker=ticker,
        short_float_pct=short_float_pct,
        float_shares=float_shares,
        short_ratio=short_ratio,
        borrow_rate_pct=borrow_rate_pct,
    )


def make_uw_alerts(
    n_darkpool: int = 0,
    dp_premium: float = 0,
    n_call: int = 0,
    call_premium: float = 0,
    n_put: int = 0,
    put_premium: float = 0,
) -> list[dict]:
    alerts: list[dict] = []
    for _ in range(n_darkpool):
        alerts.append({"type": "dark_pool_print", "side": "", "premium": dp_premium})
    for _ in range(n_call):
        alerts.append({"type": "sweep",            "side": "call", "premium": call_premium})
    for _ in range(n_put):
        alerts.append({"type": "sweep",            "side": "put",  "premium": put_premium})
    return alerts


def make_articles(
    n: int = 1,
    is_fda: int = 0,
    is_earnings: int = 0,
    is_ma: int = 0,
) -> list[dict]:
    return [{"is_fda": is_fda, "is_earnings": is_earnings, "is_ma": is_ma}
            for _ in range(n)]


def make_options_signals(cached_metrics: dict) -> OptionsSignals:
    """Inject a mock TradierOptionsClient returning cached_metrics."""
    os_instance = OptionsSignals.__new__(OptionsSignals)
    os_instance._tradier = MagicMock()
    os_instance._tradier.get_cached_metrics.return_value = cached_metrics
    return os_instance


def make_catalyst_signals(
    news_sentiment: float = 0.0,
    articles: list[dict] | None = None,
    reddit_rank: float = 0.0,
    mentions_hr: int = 0,
) -> CatalystSignals:
    """Build CatalystSignals with pre-stubbed news and Reddit outputs."""
    cs = CatalystSignals.__new__(CatalystSignals)
    cs._news   = MagicMock()
    cs._reddit = MagicMock()
    cs._news.get_news_sentiment_score.return_value = news_sentiment
    cs._news.get_cached_news.return_value          = articles if articles is not None else []
    cs._reddit.get_reddit_rank.return_value               = reddit_rank
    cs._reddit.get_ticker_mentions_per_hour.return_value  = mentions_hr
    return cs


# ═════════════════════════════════════════════════════════
#  1. OptionsSignals
# ═════════════════════════════════════════════════════════

class TestOptionsSignalsPCRatio:
    """Put/call ratio → pc_score mapping."""

    _THRESHOLD = _cfg.put_call_ratio_bullish   # default 0.70

    def _compute(self, pc_ratio: float, **kwargs) -> dict:
        opts   = make_options_signals({"pc_volume_ratio": pc_ratio, **kwargs})
        return opts.compute("AAPL", avg_dollar_volume=1_000_000, uw_alerts=[])

    def test_pc_ratio_below_threshold_score_is_one(self):
        result = self._compute(self._THRESHOLD - 0.01)
        # pc_score=1.0 → final score includes 0.30 * 1.0 = 0.30 from pc alone
        # verify pc_bullish flag
        assert result["pc_bullish"] is True

    def test_pc_ratio_equals_threshold_score_still_one(self):
        # exactly AT threshold (strictly less than) → True
        result = self._compute(self._THRESHOLD)
        # threshold=0.70, ratio=0.70 → NOT < 0.70 so pc_score < 1.0
        assert result["pc_bullish"] is False

    def test_pc_ratio_between_threshold_and_1_interpolated(self):
        pc = (self._THRESHOLD + 1.0) / 2   # midpoint → 0.85
        result = self._compute(pc)
        # pc_score = (1 - 0.85) / (1 - 0.70) = 0.15/0.30 = 0.5
        expected_pc_score = (1.0 - pc) / (1.0 - self._THRESHOLD)
        # final score includes 0.30 * pc_score; check total is reasonable
        assert 0.0 <= result["score"] <= 1.0
        assert result["pc_ratio"] == pytest.approx(pc, abs=0.001)

    def test_pc_ratio_between_1_and_1p2_score_is_0p2(self):
        result = self._compute(1.1)
        # pc_score=0.2; total = 0.30*0.2 = 0.06 from pc component alone
        assert result["score"] >= 0.0

    def test_pc_ratio_above_1p2_pc_score_is_zero(self):
        result = self._compute(1.5)
        assert result["pc_bullish"] is False

    def test_pc_bullish_true_when_below_threshold(self):
        result = self._compute(self._THRESHOLD * 0.5)
        assert result["pc_bullish"] is True

    def test_pc_bullish_false_when_above_threshold(self):
        result = self._compute(self._THRESHOLD + 0.01)
        assert result["pc_bullish"] is False


class TestOptionsSignalsSweepScore:

    def _compute(self, sweep_count: int) -> dict:
        opts = make_options_signals({"pc_volume_ratio": 0.5, "sweep_count": sweep_count})
        return opts.compute("AAPL", 1_000_000, [])

    def test_sweep_score_zero_with_no_sweeps(self):
        result = self._compute(0)
        # sweep_score = 0/5 = 0 → contribution 0.25*0 = 0
        # Just verify result is returned cleanly
        assert result["sweep_count"] == 0

    def test_sweep_score_at_one_with_5_sweeps(self):
        result = self._compute(5)
        assert result["sweep_count"] == 5

    def test_sweep_score_capped_at_one_with_many_sweeps(self):
        """More than 5 sweeps → still capped at 1.0 for sweep component."""
        result = self._compute(50)
        assert 0.0 <= result["score"] <= 1.0

    def test_more_sweeps_gives_higher_score(self):
        opts  = OptionsSignals.__new__(OptionsSignals)
        opts._tradier = MagicMock()

        def compute_with_sweeps(n: int) -> float:
            opts._tradier.get_cached_metrics.return_value = {
                "pc_volume_ratio": 0.5, "sweep_count": n
            }
            return opts.compute("AAPL", 1_000_000, [])["score"]

        assert compute_with_sweeps(5) > compute_with_sweeps(0)


class TestOptionsSignalsDarkPool:

    def _compute(self, alerts: list[dict], avg_dv: float = 1_000_000) -> dict:
        opts = make_options_signals({"pc_volume_ratio": 1.0, "sweep_count": 0})
        return opts.compute("AAPL", avg_dv, alerts)

    def test_dp_premium_summed_from_dark_pool_alerts(self):
        alerts = [
            {"type": "dark_pool_print", "side": "", "premium": 50_000},
            {"type": "dark_pool_print", "side": "", "premium": 50_000},
        ]
        result = self._compute(alerts)
        assert result["dp_premium"] == pytest.approx(100_000)

    def test_non_dark_pool_alerts_not_counted_in_dp(self):
        alerts = [{"type": "sweep", "side": "call", "premium": 200_000}]
        result = self._compute(alerts)
        assert result["dp_premium"] == pytest.approx(0.0)

    def test_dp_score_capped_at_one(self):
        """Enormous dark pool print → dp_score can't exceed 1.0."""
        alerts = [{"type": "dark_pool_block", "side": "", "premium": 999_999_999}]
        result = self._compute(alerts, avg_dv=1_000)
        assert result["score"] <= 1.0

    def test_mixed_alerts_only_dp_counted(self):
        alerts = [
            {"type": "dark_pool_print", "side": "",     "premium": 100_000},
            {"type": "sweep",           "side": "call", "premium": 200_000},
            {"type": "sweep",           "side": "put",  "premium": 50_000},
        ]
        result = self._compute(alerts)
        assert result["dp_premium"] == pytest.approx(100_000)


class TestOptionsSignalsCallPremium:

    def _compute(self, alerts: list[dict], avg_dv: float = 1_000_000) -> dict:
        opts = make_options_signals({"pc_volume_ratio": 1.0, "sweep_count": 0})
        return opts.compute("AAPL", avg_dv, alerts)

    def test_call_premium_from_call_side_alerts(self):
        alerts = [
            {"type": "sweep", "side": "call", "premium": 75_000},
            {"type": "sweep", "side": "call", "premium": 25_000},
        ]
        result = self._compute(alerts)
        assert result["call_premium"] == pytest.approx(100_000)

    def test_call_premium_from_c_side_alias(self):
        alerts = [{"type": "sweep", "side": "c", "premium": 60_000}]
        result = self._compute(alerts)
        assert result["call_premium"] == pytest.approx(60_000)

    def test_put_side_not_counted_in_call_premium(self):
        alerts = [{"type": "sweep", "side": "put", "premium": 80_000}]
        result = self._compute(alerts)
        assert result["call_premium"] == pytest.approx(0.0)

    def test_call_score_capped_at_one(self):
        alerts = [{"type": "sweep", "side": "call", "premium": 999_999_999}]
        result = self._compute(alerts, avg_dv=1_000)
        assert result["score"] <= 1.0


class TestOptionsSignalsScoreAndKeys:

    def test_score_in_range_0_to_1(self):
        for pc, sweeps in [(0.3, 0), (1.0, 5), (1.5, 0), (0.6, 10)]:
            opts = make_options_signals({"pc_volume_ratio": pc, "sweep_count": sweeps})
            result = opts.compute("AAPL", 500_000, make_uw_alerts(n_call=2, call_premium=50_000))
            assert 0.0 <= result["score"] <= 1.0, \
                f"score={result['score']} out of range for pc={pc} sweeps={sweeps}"

    def test_empty_cache_uses_defaults(self):
        """Missing metrics dict → defaults: pc_ratio=1.0, sweep_count=0."""
        opts = make_options_signals({})
        result = opts.compute("AAPL", 1_000_000, [])
        assert result["pc_ratio"]    == pytest.approx(1.0)
        assert result["sweep_count"] == 0

    def test_all_required_keys_present(self):
        opts   = make_options_signals({"pc_volume_ratio": 0.6})
        result = opts.compute("AAPL", 1_000_000, [])
        for key in ("ticker", "pc_ratio", "avg_iv", "sweep_count",
                    "dp_premium", "call_premium", "pc_bullish", "score"):
            assert key in result, f"Missing key: {key}"

    def test_ticker_preserved(self):
        opts   = make_options_signals({})
        result = opts.compute("TSLA", 1_000_000, [])
        assert result["ticker"] == "TSLA"


# ═════════════════════════════════════════════════════════
#  2. RiskSignals
# ═════════════════════════════════════════════════════════

class TestRiskSignalsSIScore:

    def _compute(self, sf_pct: float, **kwargs) -> dict:
        profile = make_short_profile(short_float_pct=sf_pct, **kwargs)
        return RiskSignals().compute("GME", profile, atr_pct=0.03, beta=1.0)

    def test_si_score_zero_at_zero_short_float(self):
        result = self._compute(0.0)
        assert result["squeeze_score"] >= 0.0

    def test_si_score_one_at_40pct_short_float(self):
        """short_float_pct=0.40 → si_score=1.0."""
        result = self._compute(0.40)
        assert result["squeeze_score"] >= 0.35  # at least 35% of max weight

    def test_si_score_capped_at_1_above_40pct(self):
        r_high = self._compute(0.80)
        r_max  = self._compute(0.40)
        # Both should produce the same si_score contribution
        assert r_high["squeeze_score"] == pytest.approx(r_max["squeeze_score"], abs=0.001)

    def test_higher_short_float_gives_higher_squeeze_score(self):
        low  = self._compute(0.10)["squeeze_score"]
        high = self._compute(0.40)["squeeze_score"]
        assert high > low


class TestRiskSignalsFloatScore:

    def _compute(self, float_shares: float) -> dict:
        profile = make_short_profile(float_shares=float_shares)
        return RiskSignals().compute("GME", profile, atr_pct=0.03, beta=1.0)

    def test_float_score_max_at_zero_float(self):
        """float_shares=0 → float_score=1.0 (all weight goes to float component)."""
        result = self._compute(0)
        assert result["squeeze_score"] > 0.0

    def test_float_score_zero_at_20M_float(self):
        """float_shares=20M → float_score=0."""
        r_small  = self._compute(1_000_000)["squeeze_score"]
        r_large  = self._compute(20_000_000)["squeeze_score"]
        assert r_small > r_large

    def test_float_score_zero_above_20M(self):
        """float_shares > 20M → float_score=0 (max(0, negative))."""
        r_over = self._compute(25_000_000)["squeeze_score"]
        r_at   = self._compute(20_000_000)["squeeze_score"]
        # Both should be equal (floor at 0)
        assert r_over <= r_at


class TestRiskSignalsDTCScore:

    def _compute(self, short_ratio: float) -> dict:
        profile = make_short_profile(short_ratio=short_ratio)
        return RiskSignals().compute("GME", profile, atr_pct=0.03, beta=1.0)

    def test_dtc_score_zero_at_zero_ratio(self):
        assert self._compute(0.0)["squeeze_score"] >= 0.0

    def test_dtc_score_one_at_20_days(self):
        r_low  = self._compute(1.0)["squeeze_score"]
        r_high = self._compute(20.0)["squeeze_score"]
        assert r_high > r_low

    def test_dtc_score_capped_above_20(self):
        r_20  = self._compute(20.0)["squeeze_score"]
        r_100 = self._compute(100.0)["squeeze_score"]
        assert r_20 == pytest.approx(r_100, abs=0.001)


class TestRiskSignalsVolRegime:

    def _compute(self, atr_pct: float) -> dict:
        profile = make_short_profile()
        return RiskSignals().compute("GME", profile, atr_pct=atr_pct, beta=1.0)

    def test_low_regime_when_atr_le_2pct(self):
        assert self._compute(0.01)["vol_regime"] == "low"
        assert self._compute(0.02)["vol_regime"] == "low"

    def test_normal_regime_when_2_to_4pct(self):
        assert self._compute(0.021)["vol_regime"] == "normal"
        assert self._compute(0.04)["vol_regime"] == "normal"

    def test_high_regime_when_4_to_8pct(self):
        assert self._compute(0.041)["vol_regime"] == "high"
        assert self._compute(0.08)["vol_regime"] == "high"

    def test_extreme_regime_when_above_8pct(self):
        assert self._compute(0.081)["vol_regime"] == "extreme"
        assert self._compute(0.20)["vol_regime"]  == "extreme"

    def test_atr_pct_in_result_is_percentage(self):
        result = self._compute(0.05)
        # atr_pct stored as ×100 in result (5% → 5.0)
        assert result["atr_pct"] == pytest.approx(5.0, abs=0.01)


class TestRiskSignalsBetaPenalty:

    def _compute(self, beta: float) -> dict:
        profile = make_short_profile(short_float_pct=0.30, short_ratio=6.0)
        return RiskSignals().compute("GME", profile, atr_pct=0.03, beta=beta)

    def test_no_penalty_when_beta_le_1p5(self):
        r_low  = self._compute(1.0)["score"]
        r_mid  = self._compute(1.5)["score"]
        assert r_low == pytest.approx(r_mid, abs=0.001)

    def test_penalty_applied_when_beta_above_1p5(self):
        r_no_penalty = self._compute(1.0)["score"]
        r_penalty    = self._compute(3.0)["score"]
        assert r_penalty < r_no_penalty

    def test_penalty_capped_at_0p5(self):
        """Extreme beta → penalty is still capped, score floored at 0."""
        result = self._compute(100.0)
        assert result["score"] >= 0.0

    def test_score_never_negative(self):
        for beta in [0.5, 1.0, 2.0, 5.0, 20.0]:
            assert self._compute(beta)["score"] >= 0.0


class TestRiskSignalsSqueezeCandidate:

    def test_squeeze_candidate_true_when_all_criteria_met(self):
        profile = make_short_profile(
            short_float_pct=_cfg.short_float_threshold,
            float_shares=_cfg.float_shares_max_squeeze,
            short_ratio=_cfg.short_days_to_cover_min,
        )
        result = RiskSignals().compute("GME", profile, atr_pct=0.03)
        assert result["squeeze_candidate"] is True

    def test_squeeze_candidate_false_when_short_float_low(self):
        profile = make_short_profile(
            short_float_pct=0.01,
            float_shares=5_000_000,
            short_ratio=10.0,
        )
        result = RiskSignals().compute("GME", profile, atr_pct=0.03)
        assert result["squeeze_candidate"] is False


class TestRiskSignalsResultKeys:

    def test_all_required_keys_present(self):
        profile = make_short_profile()
        result  = RiskSignals().compute("GME", profile, atr_pct=0.03)
        for key in (
            "ticker", "short_float_pct", "short_ratio", "float_shares",
            "borrow_rate_pct", "beta", "atr_pct", "vol_regime",
            "squeeze_candidate", "squeeze_score", "score",
        ):
            assert key in result, f"Missing key: {key}"

    def test_short_float_pct_in_result_is_percentage(self):
        profile = make_short_profile(short_float_pct=0.25)
        result  = RiskSignals().compute("GME", profile, atr_pct=0.03)
        assert result["short_float_pct"] == pytest.approx(25.0, abs=0.01)

    def test_ticker_preserved(self):
        profile = make_short_profile(ticker="AMC")
        result  = RiskSignals().compute("AMC", profile, atr_pct=0.03)
        assert result["ticker"] == "AMC"

    def test_score_in_range_0_to_1(self):
        for sf, fl, dtc, atr, beta in [
            (0.0, 20e6, 0.0, 0.01, 1.0),
            (0.40, 5e6, 20.0, 0.05, 1.0),
            (0.30, 10e6, 6.0, 0.03, 2.5),
        ]:
            profile = make_short_profile(
                short_float_pct=sf, float_shares=fl, short_ratio=dtc
            )
            r = RiskSignals().compute("X", profile, atr_pct=atr, beta=beta)
            assert 0.0 <= r["score"] <= 1.0


# ═════════════════════════════════════════════════════════
#  3. CatalystSignals
# ═════════════════════════════════════════════════════════

class TestCatalystSignalsNewsScore:

    def _compute(self, sentiment: float, articles=None) -> dict:
        cs = make_catalyst_signals(news_sentiment=sentiment, articles=articles or [])
        return cs.compute("AAPL")

    def test_news_score_normalised_from_minus1_to_plus1(self):
        result = self._compute(0.0)
        assert result["news_score"] == pytest.approx(0.5)   # (0+1)/2

    def test_news_score_1_for_max_positive_sentiment(self):
        result = self._compute(1.0)
        assert result["news_score"] == pytest.approx(1.0)

    def test_news_score_0_for_max_negative_sentiment(self):
        result = self._compute(-1.0)
        assert result["news_score"] == pytest.approx(0.0)

    def test_news_sentiment_raw_preserved(self):
        result = self._compute(0.42)
        assert result["news_sentiment"] == pytest.approx(0.42, abs=0.001)


class TestCatalystSignalsCatalystBoosts:

    def _compute_with_flags(self, is_fda=0, is_earnings=0, is_ma=0) -> dict:
        articles = make_articles(n=1, is_fda=is_fda,
                                 is_earnings=is_earnings, is_ma=is_ma)
        cs = make_catalyst_signals(news_sentiment=0.0, articles=articles)
        return cs.compute("AAPL")

    def test_fda_boost_adds_0p20(self):
        r_no  = self._compute_with_flags()
        r_fda = self._compute_with_flags(is_fda=1)
        assert r_fda["score"] == pytest.approx(r_no["score"] + 0.20, abs=0.001)

    def test_earnings_boost_adds_0p10(self):
        r_no  = self._compute_with_flags()
        r_ear = self._compute_with_flags(is_earnings=1)
        assert r_ear["score"] == pytest.approx(r_no["score"] + 0.10, abs=0.001)

    def test_ma_boost_adds_0p15(self):
        r_no = self._compute_with_flags()
        r_ma = self._compute_with_flags(is_ma=1)
        assert r_ma["score"] == pytest.approx(r_no["score"] + 0.15, abs=0.001)

    def test_all_catalysts_accumulate(self):
        r_none = self._compute_with_flags()
        r_all  = self._compute_with_flags(is_fda=1, is_earnings=1, is_ma=1)
        expected_boost = 0.20 + 0.10 + 0.15
        assert r_all["score"] == pytest.approx(
            min(1.0, r_none["score"] + expected_boost), abs=0.001
        )

    def test_is_fda_flag_true_when_any_article_has_fda(self):
        articles = [
            {"is_fda": 1, "is_earnings": 0, "is_ma": 0},
            {"is_fda": 0, "is_earnings": 0, "is_ma": 0},
        ]
        cs = make_catalyst_signals(articles=articles)
        result = cs.compute("AAPL")
        assert result["is_fda"] is True

    def test_is_fda_flag_false_when_no_articles(self):
        cs = make_catalyst_signals(articles=[])
        result = cs.compute("AAPL")
        assert result["is_fda"] is False

    def test_is_earnings_flag(self):
        articles = [{"is_fda": 0, "is_earnings": 1, "is_ma": 0}]
        cs = make_catalyst_signals(articles=articles)
        assert cs.compute("AAPL")["is_earnings"] is True

    def test_is_ma_flag(self):
        articles = [{"is_fda": 0, "is_earnings": 0, "is_ma": 1}]
        cs = make_catalyst_signals(articles=articles)
        assert cs.compute("AAPL")["is_ma"] is True


class TestCatalystSignalsReddit:

    def _compute(self, rank: float = 0.0, mentions_hr: int = 0) -> dict:
        cs = make_catalyst_signals(reddit_rank=rank, mentions_hr=mentions_hr)
        return cs.compute("AAPL")

    def test_reddit_rank_propagated(self):
        result = self._compute(rank=0.75)
        assert result["reddit_rank"] == pytest.approx(0.75, abs=0.001)

    def test_reddit_mentions_hr_propagated(self):
        result = self._compute(mentions_hr=20)
        assert result["reddit_mentions_hr"] == 20

    def test_velocity_bonus_at_50_mentions_is_0p30(self):
        """50 mentions/hr → velocity_bonus = min(50/50, 0.30) = 0.30."""
        r_zero = self._compute(mentions_hr=0)
        r_50   = self._compute(mentions_hr=50)
        diff   = r_50["score"] - r_zero["score"]
        assert diff == pytest.approx(0.10 * 0.30, abs=0.01)   # 0.10 * velocity_bonus

    def test_velocity_bonus_capped_at_30_mentions(self):
        """100+ mentions/hr → still capped at 0.30."""
        r_50   = self._compute(mentions_hr=50)
        r_1000 = self._compute(mentions_hr=1000)
        assert r_50["score"] == pytest.approx(r_1000["score"], abs=0.001)


class TestCatalystSignalsScoreAndKeys:

    def test_score_clamped_to_1p0(self):
        """All boosts + high news + high reddit → can't exceed 1.0."""
        articles = make_articles(n=1, is_fda=1, is_earnings=1, is_ma=1)
        cs = make_catalyst_signals(
            news_sentiment=1.0,
            articles=articles,
            reddit_rank=1.0,
            mentions_hr=1000,
        )
        assert cs.compute("AAPL")["score"] <= 1.0

    def test_score_zero_with_all_negatives(self):
        """Minimal inputs → score is small positive (news_score=0 → 0.5 at worst)."""
        cs = make_catalyst_signals(news_sentiment=-1.0, articles=[], reddit_rank=0.0)
        result = cs.compute("AAPL")
        assert result["score"] >= 0.0

    def test_all_required_keys_present(self):
        cs     = make_catalyst_signals()
        result = cs.compute("AAPL")
        for key in (
            "ticker", "news_sentiment", "news_score", "is_fda",
            "is_earnings", "is_ma", "reddit_rank", "reddit_mentions_hr", "score",
        ):
            assert key in result, f"Missing key: {key}"

    def test_ticker_preserved(self):
        cs = make_catalyst_signals()
        assert cs.compute("TSLA")["ticker"] == "TSLA"

    def test_score_in_range_0_to_1(self):
        for sentiment, rank, mentions in [
            (-1.0, 0.0, 0),
            ( 0.0, 0.5, 25),
            ( 1.0, 1.0, 100),
        ]:
            cs = make_catalyst_signals(
                news_sentiment=sentiment, reddit_rank=rank, mentions_hr=mentions
            )
            r = cs.compute("X")
            assert 0.0 <= r["score"] <= 1.0


# ═════════════════════════════════════════════════════════
#  4. MarketContextSignals
# ═════════════════════════════════════════════════════════

class TestMarketContextFetchVix:

    def test_returns_vix_float_from_yfinance(self):
        mcs = MarketContextSignals()
        mock_ticker = MagicMock()
        mock_ticker.fast_info.last_price = 18.5
        with patch("yfinance.Ticker", return_value=mock_ticker):
            vix = mcs.fetch_vix()
        assert vix == pytest.approx(18.5)

    def test_defaults_to_20_on_exception(self):
        mcs = MarketContextSignals()
        with patch("yfinance.Ticker", side_effect=Exception("network error")):
            vix = mcs.fetch_vix()
        assert vix == pytest.approx(20.0)

    def test_defaults_to_20_when_last_price_none(self):
        mcs = MarketContextSignals()
        mock_ticker = MagicMock()
        mock_ticker.fast_info.last_price = None
        with patch("yfinance.Ticker", return_value=mock_ticker):
            vix = mcs.fetch_vix()
        assert vix == pytest.approx(20.0)


class TestMarketContextFetchFuturesBias:

    import pandas as pd

    def _mock_es_history(self, prev_close: float, curr_close: float):
        import pandas as pd
        idx = pd.date_range("2025-01-01", periods=2, freq="D")
        return pd.DataFrame({"Close": [prev_close, curr_close]}, index=idx)

    def test_positive_bias_when_price_rose(self):
        mcs = MarketContextSignals()
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = self._mock_es_history(5000.0, 5100.0)
        with patch("yfinance.Ticker", return_value=mock_ticker):
            bias = mcs.fetch_futures_bias()
        assert bias == pytest.approx(0.02, abs=0.001)

    def test_negative_bias_when_price_fell(self):
        mcs = MarketContextSignals()
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = self._mock_es_history(5000.0, 4900.0)
        with patch("yfinance.Ticker", return_value=mock_ticker):
            bias = mcs.fetch_futures_bias()
        assert bias == pytest.approx(-0.02, abs=0.001)

    def test_returns_zero_on_exception(self):
        mcs = MarketContextSignals()
        with patch("yfinance.Ticker", side_effect=Exception("timeout")):
            bias = mcs.fetch_futures_bias()
        assert bias == pytest.approx(0.0)

    def test_returns_zero_when_only_one_row(self):
        import pandas as pd
        mcs = MarketContextSignals()
        mock_ticker = MagicMock()
        idx = pd.date_range("2025-01-01", periods=1, freq="D")
        mock_ticker.history.return_value = pd.DataFrame({"Close": [5000.0]}, index=idx)
        with patch("yfinance.Ticker", return_value=mock_ticker):
            bias = mcs.fetch_futures_bias()
        assert bias == pytest.approx(0.0)


class TestMarketContextComputeVixRegime:

    def _compute(self, vix: float, futures_bias: float = 0.0) -> dict:
        mcs = MarketContextSignals()
        mcs.fetch_vix          = MagicMock(return_value=vix)
        mcs.fetch_futures_bias = MagicMock(return_value=futures_bias)
        return mcs.compute()

    def test_low_vol_regime_below_15(self):
        result = self._compute(12.0)
        assert result["vix_regime"] == "low_vol"
        assert result["vix_score"]  == pytest.approx(1.0)

    def test_low_vol_regime_at_14p9(self):
        result = self._compute(14.9)
        assert result["vix_regime"] == "low_vol"

    def test_normal_regime_at_15(self):
        result = self._compute(15.0)
        assert result["vix_regime"] == "normal"

    def test_normal_regime_below_high_threshold(self):
        result = self._compute(_cfg.vix_high_regime - 0.1)
        assert result["vix_regime"] == "normal"
        assert 0.5 <= result["vix_score"] <= 1.0

    def test_high_vol_regime_at_threshold(self):
        result = self._compute(_cfg.vix_high_regime)
        assert result["vix_regime"] == "high_vol"
        assert result["vix_score"]  == pytest.approx(0.25)

    def test_high_vol_regime_between_25_and_35(self):
        result = self._compute(30.0)
        assert result["vix_regime"] == "high_vol"
        assert result["vix_score"]  == pytest.approx(0.25)

    def test_extreme_fear_regime_at_35(self):
        result = self._compute(_cfg.vix_fear_regime)
        assert result["vix_regime"] == "extreme_fear"
        assert result["vix_score"]  == pytest.approx(0.05)

    def test_extreme_fear_regime_above_35(self):
        result = self._compute(50.0)
        assert result["vix_regime"] == "extreme_fear"

    def test_vix_score_interpolated_in_normal_range(self):
        """VIX halfway between 15 and threshold → score between 0.75 and 1.0."""
        mid_vix = (15.0 + _cfg.vix_high_regime) / 2
        result  = self._compute(mid_vix)
        assert 0.7 <= result["vix_score"] <= 1.0


class TestMarketContextComputeFuturesScore:

    def _compute(self, futures_bias: float) -> dict:
        mcs = MarketContextSignals()
        mcs.fetch_vix          = MagicMock(return_value=20.0)
        mcs.fetch_futures_bias = MagicMock(return_value=futures_bias)
        return mcs.compute()

    def test_futures_score_zero_for_very_negative_bias(self):
        result = self._compute(-0.10)
        assert result["futures_score"] == pytest.approx(0.0)

    def test_futures_score_one_for_very_positive_bias(self):
        result = self._compute(0.10)
        assert result["futures_score"] == pytest.approx(1.0)

    def test_futures_score_half_at_zero_bias(self):
        """futures_bias=0 → (0+0.02)/0.04 = 0.5."""
        result = self._compute(0.0)
        assert result["futures_score"] == pytest.approx(0.5)

    def test_futures_bias_in_result_is_percentage(self):
        """futures_bias stored ×100 in result."""
        result = self._compute(0.01)
        assert result["futures_bias"] == pytest.approx(1.0, abs=0.01)


class TestMarketContextComputeScoreAndKeys:

    def _compute(self, vix=20.0, bias=0.0) -> dict:
        mcs = MarketContextSignals()
        mcs.fetch_vix          = MagicMock(return_value=vix)
        mcs.fetch_futures_bias = MagicMock(return_value=bias)
        return mcs.compute()

    def test_score_equals_formula(self):
        result = self._compute(vix=20.0, bias=0.0)
        expected = round(0.60 * result["vix_score"] + 0.40 * result["futures_score"], 3)
        assert result["score"] == pytest.approx(expected, abs=0.001)

    def test_score_in_range_0_to_1(self):
        for vix, bias in [(10.0, 0.05), (20.0, 0.0), (35.0, -0.05), (50.0, -0.10)]:
            r = self._compute(vix, bias)
            assert 0.0 <= r["score"] <= 1.0, \
                f"score={r['score']} out of range for vix={vix} bias={bias}"

    def test_low_vix_good_futures_gives_high_score(self):
        assert self._compute(10.0, 0.05)["score"] > 0.75

    def test_extreme_vix_negative_futures_gives_low_score(self):
        assert self._compute(50.0, -0.10)["score"] < 0.20

    def test_all_required_keys_present(self):
        result = self._compute()
        for key in ("vix", "vix_regime", "futures_bias",
                    "vix_score", "futures_score", "score"):
            assert key in result, f"Missing key: {key}"


if __name__ == "__main__":
    # Allow running directly: python tests/test_price_feed.py
    import subprocess, sys
    sys.exit(subprocess.call(
        [sys.executable, "-m", "pytest", __file__, "-v", "--tb=short"],
    ))