
"""
Layer 2 - Volume and technical signal computation
volume_signals.py

signals/volume_signals.py
Layer 2 — Volume and technical indicator computation.

Computes RVOL, float turnover, VWAP deviation, RSI, MACD,
Bollinger squeeze, EMA crossovers, and ATR from a combination
of historical OHLCV (seeded from yfinance) and live 1-min bars.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pandas_ta as ta
from loguru import logger

import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   
from ingestion.price_feed import fetch_historical_ohlcv


def _safe_last(series: pd.Series, default: float = 0.0) -> float:
    """Return last non-NaN value in a Series, or default."""
    vals = series.dropna()
    return float(vals.iloc[-1]) if not vals.empty else default


# ─────────────────────────────────────────────────────────
#  Volume signals
# ─────────────────────────────────────────────────────────

class VolumeSignals:
    """
    All volume-based indicators for a single ticker.
    Designed to be called every 30 seconds with a refreshed DataFrame.
    """

    def __init__(self, ticker: str, lookback_days: int = 10):
        self.ticker = ticker.upper()
        self.lookback_days = lookback_days
        self._hist: pd.DataFrame = pd.DataFrame()

    def refresh_history(self):
        """Re-pull historical OHLCV. Call once at startup or daily."""
        self._hist = fetch_historical_ohlcv(
            self.ticker, period=f"{self.lookback_days + 30}d"
        )

    def compute(
        self,
        current_volume: int,
        current_price: float,
        float_shares: float,
    ) -> dict:
        """
        Compute all volume metrics given today's running volume.

        Parameters
        ──────────
        current_volume  — shares traded so far today (from live feed)
        current_price   — latest trade price
        float_shares    — public float (from ShortDataClient)
        """
        if self._hist.empty:
            self.refresh_history()

        df = self._hist.copy()

        # ── Average volume (10-day) ──────────────────────────
        avg_vol_10 = float(df["volume"].tail(self.lookback_days).mean()) if not df.empty else 1
        avg_vol_10 = max(avg_vol_10, 1)

        # ── RVOL ─────────────────────────────────────────────
        # Intraday RVOL: compare current vol to *expected* vol at this time of day.
        # Simplified: compare to full-day average (replace with time-of-day
        # adjustment for production).
        rvol = current_volume / avg_vol_10

        # ── Float turnover ───────────────────────────────────
        float_turnover = (current_volume / float_shares
                          if float_shares > 0 else 0.0)

        # ── Dollar volume ────────────────────────────────────
        dollar_volume = current_volume * current_price

        # ── Volume trend: today vs 5-day avg ─────────────────
        avg_vol_5 = float(df["volume"].tail(5).mean()) if not df.empty else 1
        vol_trend = current_volume / max(avg_vol_5, 1)

        return {
            "ticker":          self.ticker,
            "current_volume":  current_volume,
            "avg_volume_10d":  round(avg_vol_10, 0),
            "avg_volume_5d":   round(avg_vol_5, 0),
            "rvol":            round(rvol, 2),
            "float_turnover":  round(float_turnover, 4),
            "dollar_volume":   round(dollar_volume, 0),
            "vol_trend_5d":    round(vol_trend, 2),
            # Flags
            "is_unusual_vol":  rvol >= 3.0,
            "is_extreme_vol":  rvol >= 5.0,
            "is_float_play":   float_turnover >= 0.10,  # >10% of float traded
            # Score component (0–1)
            "score": min(1.0, round(
                0.50 * min(rvol / 10.0, 1.0) +
                0.30 * min(float_turnover * 5, 1.0) +
                0.20 * min(vol_trend / 5.0, 1.0),
                3,
            )),
        }


# ─────────────────────────────────────────────────────────
#  Technical signals
# ─────────────────────────────────────────────────────────

class TechnicalSignals:
    """
    VWAP, EMA, RSI, MACD, Bollinger Bands, ATR, and
    breakout-level detection for a single ticker.
    """

    def __init__(self, ticker: str):
        self.ticker = ticker.upper()
        self._hist: pd.DataFrame = pd.DataFrame()

    def refresh_history(self):
        self._hist = fetch_historical_ohlcv(self.ticker, period="3mo")

    def compute(self, current_price: float, intraday_df: pd.DataFrame | None = None) -> dict:
        """
        Compute technical indicators.

        Parameters
        ──────────
        current_price   — latest price from live feed
        intraday_df     — optional 1-min bars DataFrame for intraday VWAP
                          columns: open, high, low, close, volume
        """
        if self._hist.empty:
            self.refresh_history()

        df = self._hist.copy()
        if df.empty:
            return self._empty_result(current_price)

        close = df["close"]

        # ── EMAs ─────────────────────────────────────────────
        ema9  = ta.ema(close, length=9)
        ema21 = ta.ema(close, length=21)
        ema50 = ta.ema(close, length=50)

        ema9_val  = _safe_last(ema9)
        ema21_val = _safe_last(ema21)
        ema50_val = _safe_last(ema50)

        ema_bull = ema9_val > ema21_val > ema50_val
        ema_xover_fresh = (
            len(ema9) >= 2 and len(ema21) >= 2
            and ema9.iloc[-1] > ema21.iloc[-1]
            and ema9.iloc[-2] <= ema21.iloc[-2]
        )

        # ── RSI ──────────────────────────────────────────────
        rsi = ta.rsi(close, length=14)
        rsi_val = _safe_last(rsi, 50.0)

        # ── MACD ─────────────────────────────────────────────
        macd_df = ta.macd(close)
        if macd_df is not None and not macd_df.empty:
            macd_hist = _safe_last(macd_df.iloc[:, 2])   # histogram
            macd_sig  = _safe_last(macd_df.iloc[:, 1])   # signal
            macd_line = _safe_last(macd_df.iloc[:, 0])
        else:
            macd_hist = macd_sig = macd_line = 0.0

        # ── Bollinger Bands ───────────────────────────────────
        bbands = ta.bbands(close, length=20, std=2.0)
        bb_width = 0.0
        bb_pct_b = 0.5
        bb_squeeze = False
        if bbands is not None and not bbands.empty:
            bb_upper = _safe_last(bbands.iloc[:, 0])
            bb_mid   = _safe_last(bbands.iloc[:, 1])
            bb_lower = _safe_last(bbands.iloc[:, 2])
            bb_width = (bb_upper - bb_lower) / bb_mid if bb_mid else 0.0
            bb_pct_b = ((current_price - bb_lower) / (bb_upper - bb_lower)
                        if (bb_upper - bb_lower) > 0 else 0.5)

            # Squeeze: bandwidth at 6-month low
            all_widths = (bbands.iloc[:, 0] - bbands.iloc[:, 2]) / bbands.iloc[:, 1]
            bb_squeeze = bb_width <= float(all_widths.quantile(0.10))

        # ── ATR (volatility) ─────────────────────────────────
        atr = ta.atr(df["high"], df["low"], close, length=14)
        atr_val = _safe_last(atr)
        atr_pct = atr_val / current_price if current_price else 0.0

        # ── Support / resistance (pivot) ─────────────────────
        pivot = (df["high"].iloc[-1] + df["low"].iloc[-1] + df["close"].iloc[-1]) / 3
        r1    = 2 * pivot - df["low"].iloc[-1]
        s1    = 2 * pivot - df["high"].iloc[-1]

        # ── 52-week metrics ───────────────────────────────────
        high_52w = float(df["high"].max())
        low_52w  = float(df["low"].min())
        pct_from_high = (current_price - high_52w) / high_52w if high_52w else 0.0
        near_52w_high = pct_from_high >= -0.05   # within 5% of 52-week high

        # ── Intraday VWAP ─────────────────────────────────────
        vwap_dev = 0.0
        if intraday_df is not None and not intraday_df.empty:
            idf = intraday_df.copy()
            idf["typical"] = (idf["high"] + idf["low"] + idf["close"]) / 3
            idf["tp_vol"]  = idf["typical"] * idf["volume"]
            cum_vol = idf["volume"].cumsum()
            cum_tp  = idf["tp_vol"].cumsum()
            vwap = (cum_tp / cum_vol).iloc[-1]
            vwap_dev = (current_price - vwap) / vwap if vwap else 0.0

        # ── Composite score (0–1) ─────────────────────────────
        score_components = [
            0.25 * (1.0 if ema_bull else 0.0),
            0.20 * min(max((rsi_val - 50) / 30, 0.0), 1.0),   # RSI above 50
            0.20 * (1.0 if macd_hist > 0 else 0.0),
            0.15 * (1.0 if bb_squeeze else 0.0),
            0.10 * (1.0 if near_52w_high else 0.0),
            0.10 * min(max(vwap_dev + 0.5, 0.0), 1.0),
        ]
        score = round(sum(score_components), 3)

        return {
            "ticker":          self.ticker,
            "price":           current_price,
            "ema9":            round(ema9_val, 3),
            "ema21":           round(ema21_val, 3),
            "ema50":           round(ema50_val, 3),
            "ema_bullish":     ema_bull,
            "ema_xover_fresh": ema_xover_fresh,
            "rsi14":           round(rsi_val, 2),
            "macd_hist":       round(macd_hist, 4),
            "macd_signal":     round(macd_sig, 4),
            "bb_width":        round(bb_width, 4),
            "bb_pct_b":        round(bb_pct_b, 3),
            "bb_squeeze":      bb_squeeze,
            "atr":             round(atr_val, 3),
            "atr_pct":         round(atr_pct, 4),
            "vwap_deviation":  round(vwap_dev, 4),
            "pivot":           round(pivot, 3),
            "resistance_r1":   round(r1, 3),
            "support_s1":      round(s1, 3),
            "high_52w":        round(high_52w, 3),
            "low_52w":         round(low_52w, 3),
            "near_52w_high":   near_52w_high,
            "score":           score,
        }

    def _empty_result(self, price: float) -> dict:
        return {"ticker": self.ticker, "price": price, "score": 0.0}