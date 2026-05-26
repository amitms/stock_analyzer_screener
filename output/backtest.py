"""
Backtesting module using vectorbt
backtest.py

output/backtest.py
Backtest the composite signal score as a ranking strategy using vectorbt.

Usage
─────
  python -m output.backtest --tickers AAPL,TSLA,NVDA,AMD --period 1y
  python -m output.backtest --penny --period 6mo
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
from loguru import logger

try:
    import vectorbt as vbt
    HAVE_VBT = True
except ImportError:
    HAVE_VBT = False
    logger.warning("vectorbt not installed — using manual backtest")

import pandas_ta as ta
import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

from config.settings import get_path_config, get_scanner_config

_cfg   = get_scanner_config()
_paths = get_path_config()


# ─────────────────────────────────────────────────────────
#  Signal reconstruction from OHLCV
# ─────────────────────────────────────────────────────────

def compute_daily_score(df: pd.DataFrame) -> pd.Series:
    """
    Reconstruct a simplified composite score from daily OHLCV.
    Returns a daily score Series (0–100).
    Mirrors the live computation but uses only OHLCV-derivable signals.
    """
    close  = df["Close"]
    high   = df["High"]
    low    = df["Low"]
    volume = df["Volume"]

    scores = pd.DataFrame(index=df.index)

    # ── Volume component ─────────────────────────────────
    avg_vol = volume.rolling(10).mean().replace(0, np.nan)
    rvol    = volume / avg_vol
    scores["vol"] = (rvol.clip(0, 10) / 10).fillna(0)

    # ── RSI component ─────────────────────────────────────
    rsi = ta.rsi(close, length=14).fillna(50)
    scores["rsi"] = ((rsi - 30) / 40).clip(0, 1)

    # ── EMA trend ─────────────────────────────────────────
    ema9  = ta.ema(close, length=9)
    ema21 = ta.ema(close, length=21)
    ema50 = ta.ema(close, length=50)
    scores["ema"] = (
        ((ema9 > ema21).astype(float) * 0.5) +
        ((ema21 > ema50).astype(float) * 0.5)
    ).fillna(0)

    # ── Bollinger squeeze ─────────────────────────────────
    bb = ta.bbands(close, length=20, std=2.0)
    if bb is not None:
        bw = (bb.iloc[:, 0] - bb.iloc[:, 2]) / bb.iloc[:, 1].replace(0, np.nan)
        bw_10pct = bw.rolling(126).quantile(0.10)
        scores["squeeze"] = (bw <= bw_10pct).astype(float).fillna(0)
    else:
        scores["squeeze"] = 0.0

    # ── MACD ──────────────────────────────────────────────
    macd_df = ta.macd(close)
    if macd_df is not None:
        macd_hist = macd_df.iloc[:, 2].fillna(0)
        scores["macd"] = (macd_hist > 0).astype(float)
    else:
        scores["macd"] = 0.5

    # Composite (weights mirror penny bucket weights approximately)
    composite = (
        0.30 * scores["vol"] +
        0.20 * scores["ema"] +
        0.20 * scores["rsi"] +
        0.15 * scores["squeeze"] +
        0.15 * scores["macd"]
    ) * 100

    return composite.fillna(0)


# ─────────────────────────────────────────────────────────
#  Backtest engine
# ─────────────────────────────────────────────────────────

def run_backtest(
    tickers: list[str],
    period_days: int = 252,
    top_n: int = 5,
    rebalance_freq: str = "W",   # "D" daily, "W" weekly
) -> dict:
    """
    Long-only ranking strategy:
    1. Compute daily composite score for each ticker
    2. Rebalance to top-N tickers at each rebalance date
    3. Equal-weight allocation

    Returns a dict with performance metrics.
    """
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=period_days)

    logger.info(f"Downloading {len(tickers)} tickers for backtest…")
    raw = yf.download(
        tickers, start=start_date, end=end_date,
        auto_adjust=True, progress=False, group_by="ticker",
    )

    # Build price matrix and score matrix
    close_prices: dict[str, pd.Series] = {}
    score_series: dict[str, pd.Series] = {}

    for ticker in tickers:
        try:
            if len(tickers) == 1:
                df = raw
            else:
                df = raw[ticker]
            if df.empty or len(df) < 30:
                continue
            close_prices[ticker] = df["Close"]
            score_series[ticker] = compute_daily_score(df)
        except Exception as exc:
            logger.debug(f"Backtest skip {ticker}: {exc}")

    if not close_prices:
        raise ValueError("No usable price data for backtest")

    prices_df = pd.DataFrame(close_prices).dropna(how="all")
    scores_df = pd.DataFrame(score_series).reindex(prices_df.index).fillna(0)

    # ── Compute daily returns ─────────────────────────────
    rets = prices_df.pct_change().fillna(0)

    # ── Rebalance logic ───────────────────────────────────
    rebalance_dates = scores_df.resample(rebalance_freq).last().index
    portfolio_rets  = pd.Series(0.0, index=rets.index)

    prev_weights: dict[str, float] = {}
    for i, date in enumerate(rebalance_dates):
        if date not in scores_df.index:
            continue

        scores_today = scores_df.loc[date].dropna()
        top_tickers  = scores_today.nlargest(top_n).index.tolist()
        weight       = 1.0 / len(top_tickers)
        weights      = {t: weight for t in top_tickers}

        # Apply weights to next rebalance period
        next_date = (rebalance_dates[i + 1]
                     if i + 1 < len(rebalance_dates)
                     else rets.index[-1])

        period_rets = rets.loc[date:next_date]
        for t, w in weights.items():
            if t in period_rets.columns:
                portfolio_rets.loc[date:next_date] += period_rets[t] * w

        prev_weights = weights

    # ── Benchmark: equal-weight buy & hold ────────────────
    bh_rets = rets.mean(axis=1)

    # ── Performance metrics ───────────────────────────────
    def sharpe(r: pd.Series, rf: float = 0.05) -> float:
        excess = r.mean() * 252 - rf
        vol    = r.std() * np.sqrt(252)
        return round(excess / vol, 3) if vol > 0 else 0.0

    def max_drawdown(r: pd.Series) -> float:
        cum = (1 + r).cumprod()
        peak = cum.cummax()
        dd = (cum - peak) / peak
        return round(float(dd.min()), 4)

    def cagr(r: pd.Series, years: float) -> float:
        total = float((1 + r).prod())
        return round(total ** (1 / years) - 1, 4) if years > 0 else 0.0

    years = period_days / 365

    metrics = {
        "strategy": {
            "total_return":  round(float((1 + portfolio_rets).prod() - 1), 4),
            "cagr":          cagr(portfolio_rets, years),
            "sharpe":        sharpe(portfolio_rets),
            "max_drawdown":  max_drawdown(portfolio_rets),
            "annual_vol":    round(float(portfolio_rets.std() * np.sqrt(252)), 4),
        },
        "benchmark": {
            "total_return":  round(float((1 + bh_rets).prod() - 1), 4),
            "cagr":          cagr(bh_rets, years),
            "sharpe":        sharpe(bh_rets),
            "max_drawdown":  max_drawdown(bh_rets),
            "annual_vol":    round(float(bh_rets.std() * np.sqrt(252)), 4),
        },
        "params": {
            "tickers":        tickers,
            "period_days":    period_days,
            "top_n":          top_n,
            "rebalance_freq": rebalance_freq,
        },
    }

    # ── Save equity curve ──────────────────────────────────
    equity_df = pd.DataFrame({
        "strategy":  (1 + portfolio_rets).cumprod(),
        "benchmark": (1 + bh_rets).cumprod(),
    })
    out_path = _paths.data / "backtest_equity.csv"
    equity_df.to_csv(out_path)
    logger.info(f"Equity curve saved → {out_path}")

    return metrics


# ─────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    parser = argparse.ArgumentParser(description="Backtest scanner signal strategy")
    parser.add_argument("--tickers", default="AAPL,TSLA,NVDA,AMD,MARA,RIOT,COIN",
                        help="Comma-separated tickers")
    parser.add_argument("--period", default="1y",
                        choices=["1mo","3mo","6mo","1y","2y"],
                        help="Lookback period")
    parser.add_argument("--top-n", type=int, default=3)
    parser.add_argument("--freq",  default="W", choices=["D","W","M"])
    args = parser.parse_args()

    period_map = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 252, "2y": 504}
    tickers = [t.strip().upper() for t in args.tickers.split(",")]

    results = run_backtest(
        tickers=tickers,
        period_days=period_map[args.period],
        top_n=args.top_n,
        rebalance_freq=args.freq,
    )

    print("\n" + "="*50)
    print("BACKTEST RESULTS")
    print("="*50)
    print(json.dumps(results, indent=2))