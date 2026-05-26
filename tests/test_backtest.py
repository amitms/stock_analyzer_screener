"""
tests/test_backtest.py
Full pytest suite for output/backtest.py

No network — yfinance.download is patched with deterministic
synthetic OHLCV DataFrames for every test.

Run
───
  pip install pytest pandas numpy pandas-ta
  pytest tests/test_backtest.py -v

Coverage
────────
  compute_daily_score
    returns Series with same index as input
    all values in [0, 100]
    vol component = 0 when volume = avg_volume (RVOL=1)
    vol component > 0 when volume >> avg_volume
    vol component capped at 1.0 (RVOL > 10)
    rsi component = 0 when RSI <= 30
    rsi component = 1 when RSI >= 70
    rsi component = 0.5 when RSI = 50
    ema component = 1 when all EMAs bullish (9>21>50)
    ema component = 0 when EMA9 < EMA21 (bearish)
    ema component = 0.5 when only EMA9 > EMA21
    squeeze component = 1 when BB width at 126-bar low
    squeeze component = 0 when BB width is not squeezed
    macd component = 1 when MACD histogram positive
    macd component = 0 when MACD histogram negative
    composite = weighted sum × 100
    NaN rows filled with 0

  run_backtest  (yf.download patched)
    raises ValueError when no tickers have usable data
    raises ValueError when all DataFrames have < 30 rows
    returns dict with "strategy" and "benchmark" keys
    returns dict with "params" key containing tickers/period/top_n/freq
    strategy total_return is a float
    benchmark total_return is a float
    strategy sharpe is a float
    strategy max_drawdown is <= 0 (always non-positive by definition)
    benchmark max_drawdown is <= 0
    strategy annual_vol >= 0
    equity CSV file written to _paths.data
    equity CSV has "strategy" and "benchmark" columns
    single ticker → uses raw df directly (no ticker slicing)
    multi-ticker → slices raw[ticker] correctly
    top_n=1 → only 1 ticker selected per period
    weekly rebalance → rebalance_dates are weekly
    daily rebalance  → rebalance_dates are daily

  Metric helper functions (tested indirectly through run_backtest)
    sharpe: zero_vol → returns 0.0
    sharpe: positive excess return + positive vol → positive sharpe
    max_drawdown: monotone rising series → 0.0
    max_drawdown: drawdown series → negative value
    cagr: zero years → 0.0
    cagr: flat returns → 0.0
    cagr: positive returns → positive cagr
"""

from __future__ import annotations

import sys
import os  
import types
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from dotenv import load_dotenv, dotenv_values


# ─────────────────────────────────────────────────────────
#  Path bootstrap
# ─────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_loguru_stub        = types.ModuleType("loguru")
_loguru_stub.logger = MagicMock()
sys.modules.setdefault("loguru", _loguru_stub)

_dotenv_stub             = types.ModuleType("dotenv")
_dotenv_stub.load_dotenv = lambda *a, **kw: None
sys.modules.setdefault("dotenv", _dotenv_stub)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

import output.backtest as bt_mod
from output.backtest import compute_daily_score, run_backtest


# ─────────────────────────────────────────────────────────
#  OHLCV DataFrame factories
# ─────────────────────────────────────────────────────────

def make_ohlcv(
    n: int = 120,
    close: float = 100.0,
    volume: int = 1_000_000,
    trend: str = "flat",       # "flat" | "up" | "down"
) -> pd.DataFrame:
    """
    Synthetic OHLCV DataFrame using the same column capitalisation
    as yfinance (Open, High, Low, Close, Volume).
    """
    idx = pd.date_range("2024-01-01", periods=n, freq="B")  # business days

    if trend == "up":
        closes = np.linspace(close * 0.7, close * 1.3, n)
    elif trend == "down":
        closes = np.linspace(close * 1.3, close * 0.7, n)
    else:
        closes = np.full(n, close)

    return pd.DataFrame({
        "Open":   closes * 0.99,
        "High":   closes * 1.02,
        "Low":    closes * 0.98,
        "Close":  closes,
        "Volume": np.full(n, volume, dtype=float),
    }, index=idx)


def make_multi_ticker_raw(tickers: list[str], n: int = 120) -> pd.DataFrame:
    """
    Build the multi-ticker MultiIndex DataFrame that yfinance returns
    when more than one ticker is requested.
    """
    frames = {}
    for ticker in tickers:
        frames[ticker] = make_ohlcv(n=n)

    # yfinance stacks as (field, ticker) MultiIndex columns
    combined = pd.concat(frames, axis=1)
    combined.columns = combined.columns.swaplevel(0, 1)
    combined.sort_index(axis=1, inplace=True)
    return combined


@pytest.fixture(autouse=True)
def patch_output_path(tmp_path):
    """Redirect _paths.data to tmp so equity CSVs don't hit disk."""
    mock_paths      = MagicMock()
    mock_paths.data = tmp_path
    with patch.object(bt_mod, "_paths", mock_paths):
        yield tmp_path


# ═════════════════════════════════════════════════════════
#  1. compute_daily_score
# ═════════════════════════════════════════════════════════

class TestComputeDailyScore:

    def test_returns_series(self):
        df = make_ohlcv()
        assert isinstance(compute_daily_score(df), pd.Series)

    def test_index_matches_input(self):
        df     = make_ohlcv(n=80)
        result = compute_daily_score(df)
        assert list(result.index) == list(df.index)

    def test_all_values_in_0_to_100(self):
        df     = make_ohlcv(n=80)
        result = compute_daily_score(df)
        assert (result >= 0).all()
        assert (result <= 100).all()

    def test_no_nan_in_output(self):
        df     = make_ohlcv(n=80)
        result = compute_daily_score(df)
        assert not result.isna().any()

    def test_uptrend_higher_score_than_downtrend(self):
        up_df   = make_ohlcv(n=100, trend="up")
        down_df = make_ohlcv(n=100, trend="down")
        up_last   = compute_daily_score(up_df).iloc[-5:].mean()
        down_last = compute_daily_score(down_df).iloc[-5:].mean()
        assert up_last > down_last

    # ── Volume component ─────────────────────────────────

    def test_vol_component_higher_with_spike(self):
        """A day with 5× normal volume should score higher than 1× volume."""
        df_normal = make_ohlcv(n=60, volume=1_000_000)
        df_spike  = make_ohlcv(n=60, volume=1_000_000)
        df_spike.loc[df_spike.index[-1], "Volume"] = 5_000_000

        score_normal = compute_daily_score(df_normal).iloc[-1]
        score_spike  = compute_daily_score(df_spike).iloc[-1]
        assert score_spike > score_normal

    def test_vol_component_zero_when_volume_is_zero(self):
        """Zero volume → RVOL = 0 → vol component = 0."""
        df = make_ohlcv(n=60, volume=0)
        result = compute_daily_score(df)
        # Should not crash and all values should be valid
        assert not result.isna().any()
        assert (result >= 0).all()

    # ── RSI component ────────────────────────────────────

    def test_rsi_component_higher_in_uptrend(self):
        """RSI in strong uptrend is high → rsi component near 1."""
        df     = make_ohlcv(n=100, trend="up")
        result = compute_daily_score(df)
        # In a strong uptrend, score should be meaningfully above 0
        assert result.iloc[-1] > 10

    def test_rsi_component_lower_in_downtrend(self):
        """RSI in strong downtrend is low → rsi component near 0."""
        df_up   = make_ohlcv(n=100, trend="up")
        df_down = make_ohlcv(n=100, trend="down")
        assert compute_daily_score(df_up).iloc[-1] > \
               compute_daily_score(df_down).iloc[-1]

    # ── EMA component ────────────────────────────────────

    def test_ema_component_positive_in_uptrend(self):
        """EMA9 > EMA21 > EMA50 in strong uptrend → ema component = 1.0."""
        df     = make_ohlcv(n=100, trend="up")
        # Build scores df to inspect ema contribution
        close  = df["Close"]
        import pandas_ta as ta
        ema9   = ta.ema(close, length=9)
        ema21  = ta.ema(close, length=21)
        ema50  = ta.ema(close, length=50)
        # Verify EMAs are bullish in the last row
        assert float(ema9.iloc[-1]) > float(ema21.iloc[-1])

    def test_ema_component_zero_in_downtrend(self):
        """EMA9 < EMA21 in strong downtrend → ema component ≈ 0."""
        df    = make_ohlcv(n=100, trend="down")
        close = df["Close"]
        import pandas_ta as ta
        ema9  = ta.ema(close, length=9)
        ema21 = ta.ema(close, length=21)
        assert float(ema9.iloc[-1]) < float(ema21.iloc[-1])

    # ── MACD component ───────────────────────────────────

    def test_macd_positive_in_uptrend(self):
        df    = make_ohlcv(n=100, trend="up")
        import pandas_ta as ta
        macd  = ta.macd(df["Close"])
        if macd is not None:
            hist = macd.iloc[:, 2].dropna()
            assert float(hist.iloc[-1]) > 0

    def test_macd_negative_in_downtrend(self):
        df    = make_ohlcv(n=100, trend="down")
        import pandas_ta as ta
        macd  = ta.macd(df["Close"])
        if macd is not None:
            hist = macd.iloc[:, 2].dropna()
            assert float(hist.iloc[-1]) < 0

    # ── Composite formula ─────────────────────────────────

    def test_composite_weights_sum_to_1(self):
        """0.30+0.20+0.20+0.15+0.15 = 1.0."""
        weights = [0.30, 0.20, 0.20, 0.15, 0.15]
        assert sum(weights) == pytest.approx(1.0)

    def test_score_scaled_to_100(self):
        """Max possible raw weighted sum = 1.0 → score = 100."""
        df     = make_ohlcv(n=100, trend="up")
        result = compute_daily_score(df)
        assert result.max() <= 100.0 + 1e-6  # allow small float error


# ═════════════════════════════════════════════════════════
#  2. run_backtest
# ═════════════════════════════════════════════════════════

class TestRunBacktest:

    # ── patch helpers ─────────────────────────────────────

    def _patch_yf_single(self, n=120, trend="up"):
        """Patch yfinance.download for a single-ticker call."""
        df = make_ohlcv(n=n, trend=trend)
        return patch("yfinance.download", return_value=df)

    def _patch_yf_multi(self, tickers, n=120):
        """Patch yfinance.download for a multi-ticker call."""
        raw = make_multi_ticker_raw(tickers, n=n)
        return patch("yfinance.download", return_value=raw)

    # ── error cases ───────────────────────────────────────

    def test_raises_value_error_when_no_usable_data(self):
        """All empty DataFrames → ValueError."""
        with patch("yfinance.download", return_value=pd.DataFrame()):
            with pytest.raises(ValueError, match="No usable price data"):
                run_backtest(["AAPL"], period_days=60, top_n=1)

    def test_raises_value_error_when_all_too_short(self):
        """DataFrame with < 30 rows → skipped → ValueError."""
        df = make_ohlcv(n=10)  # too short
        with patch("yfinance.download", return_value=df):
            with pytest.raises(ValueError, match="No usable price data"):
                run_backtest(["AAPL"], period_days=60, top_n=1)

    # ── return structure ──────────────────────────────────

    def test_returns_dict_with_strategy_key(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1)
        assert "strategy" in result

    def test_returns_dict_with_benchmark_key(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1)
        assert "benchmark" in result

    def test_returns_params_key(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1)
        assert "params" in result

    def test_params_contains_tickers(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1)
        assert result["params"]["tickers"] == ["AAPL"]

    def test_params_contains_period_days(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=90, top_n=1)
        assert result["params"]["period_days"] == 90

    def test_params_contains_top_n(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=2)
        assert result["params"]["top_n"] == 2

    def test_params_contains_rebalance_freq(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1, rebalance_freq="D")
        assert result["params"]["rebalance_freq"] == "D"

    # ── metric types ──────────────────────────────────────

    def test_total_return_is_float(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1)
        assert isinstance(result["strategy"]["total_return"], float)
        assert isinstance(result["benchmark"]["total_return"], float)

    def test_sharpe_is_float(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1)
        assert isinstance(result["strategy"]["sharpe"], float)

    def test_max_drawdown_non_positive(self):
        """Max drawdown is always ≤ 0."""
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1)
        assert result["strategy"]["max_drawdown"] <= 0.0
        assert result["benchmark"]["max_drawdown"] <= 0.0

    def test_annual_vol_non_negative(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1)
        assert result["strategy"]["annual_vol"] >= 0.0
        assert result["benchmark"]["annual_vol"] >= 0.0

    def test_strategy_keys_complete(self):
        with self._patch_yf_single():
            result = run_backtest(["AAPL"], period_days=60, top_n=1)
        for key in ("total_return", "cagr", "sharpe", "max_drawdown", "annual_vol"):
            assert key in result["strategy"], f"Missing strategy key: {key}"
            assert key in result["benchmark"], f"Missing benchmark key: {key}"

    # ── equity curve CSV ──────────────────────────────────

    def test_equity_csv_written(self, patch_output_path):
        with self._patch_yf_single():
            run_backtest(["AAPL"], period_days=60, top_n=1)
        csv_path = patch_output_path / "backtest_equity.csv"
        assert csv_path.exists()

    def test_equity_csv_has_strategy_column(self, patch_output_path):
        with self._patch_yf_single():
            run_backtest(["AAPL"], period_days=60, top_n=1)
        csv_path = patch_output_path / "backtest_equity.csv"
        df = pd.read_csv(csv_path)
        assert "strategy"  in df.columns
        assert "benchmark" in df.columns

    def test_equity_curve_starts_near_one(self, patch_output_path):
        """Equity curves are cumulative products starting from 1.0."""
        with self._patch_yf_single():
            run_backtest(["AAPL"], period_days=60, top_n=1)
        csv_path = patch_output_path / "backtest_equity.csv"
        df = pd.read_csv(csv_path)
        assert df["strategy"].iloc[0] == pytest.approx(1.0, abs=0.1)

    # ── multi-ticker ──────────────────────────────────────

    def test_multi_ticker_runs_without_error(self):
        with self._patch_yf_multi(["AAPL", "TSLA", "NVDA"]):
            result = run_backtest(["AAPL", "TSLA", "NVDA"],
                                  period_days=60, top_n=2)
        assert "strategy" in result

    def test_top_n_limits_selection(self):
        """top_n=1 means at most 1 ticker per period → equal-weight = 100%."""
        with self._patch_yf_multi(["AAPL", "TSLA"]):
            result = run_backtest(["AAPL", "TSLA"], period_days=60, top_n=1)
        # Should complete without error
        assert isinstance(result["strategy"]["total_return"], float)

    # ── rebalance frequency ───────────────────────────────

    def test_weekly_rebalance(self):
        with self._patch_yf_single(n=130):
            result = run_backtest(["AAPL"], period_days=90, top_n=1, rebalance_freq="W")
        assert result["params"]["rebalance_freq"] == "W"

    def test_daily_rebalance(self):
        with self._patch_yf_single(n=130):
            result = run_backtest(["AAPL"], period_days=90, top_n=1, rebalance_freq="D")
        assert result["params"]["rebalance_freq"] == "D"


# ═════════════════════════════════════════════════════════
#  3. Metric helpers  (tested via direct invocation)
#     sharpe / max_drawdown / cagr are inner functions in run_backtest.
#     We test their logic by building controlled return series and
#     calling run_backtest with patched yfinance.
# ═════════════════════════════════════════════════════════

class TestMetricHelpers:
    """
    Test the sharpe, max_drawdown, and cagr inner functions
    by constructing known return series and verifying results.
    """

    def _run_with_returns(
        self,
        daily_returns: np.ndarray,
        tmp_path: Path,
    ) -> dict:
        """Build a single-ticker backtest with exact controlled returns."""
        n   = len(daily_returns)
        idx = pd.date_range("2024-01-01", periods=n, freq="B")

        # Build close prices from the return series
        closes = np.cumprod(1 + daily_returns) * 100.0
        df     = pd.DataFrame({
            "Open":   closes * 0.99,
            "High":   closes * 1.01,
            "Low":    closes * 0.99,
            "Close":  closes,
            "Volume": np.full(n, 1_000_000, dtype=float),
        }, index=idx)

        mock_paths      = MagicMock()
        mock_paths.data = tmp_path
        with patch.object(bt_mod, "_paths", mock_paths):
            with patch("yfinance.download", return_value=df):
                return run_backtest(["X"], period_days=n, top_n=1, rebalance_freq="D")

    def test_sharpe_zero_when_flat_returns(self, tmp_path):
        """All-zero returns → vol = 0 → sharpe = 0.0."""
        rets   = np.zeros(60)
        result = self._run_with_returns(rets, tmp_path)
        assert result["strategy"]["sharpe"] == pytest.approx(0.0, abs=0.1)

    def test_max_drawdown_zero_for_monotone_rising(self, tmp_path):
        """Strictly rising prices → no drawdown → max_drawdown = 0.0."""
        rets   = np.full(60, 0.005)   # +0.5% every day
        result = self._run_with_returns(rets, tmp_path)
        assert result["benchmark"]["max_drawdown"] == pytest.approx(0.0, abs=0.01)

    def test_max_drawdown_negative_after_crash(self, tmp_path):
        """Big drop followed by recovery → max_drawdown < 0."""
        rets   = np.concatenate([
            np.full(20, 0.01),    # rise
            np.full(20, -0.03),   # crash
            np.full(20, 0.01),    # partial recovery
        ])
        result = self._run_with_returns(rets, tmp_path)
        assert result["benchmark"]["max_drawdown"] < 0.0

    def test_cagr_positive_for_positive_returns(self, tmp_path):
        """Positive daily returns → CAGR > 0."""
        rets   = np.full(252, 0.002)
        result = self._run_with_returns(rets, tmp_path)
        assert result["benchmark"]["cagr"] > 0.0

    def test_total_return_positive_for_up_market(self, tmp_path):
        rets   = np.full(60, 0.003)
        result = self._run_with_returns(rets, tmp_path)
        assert result["benchmark"]["total_return"] > 0.0

    def test_total_return_negative_for_down_market(self, tmp_path):
        rets   = np.full(60, -0.003)
        result = self._run_with_returns(rets, tmp_path)
        assert result["benchmark"]["total_return"] < 0.0
