"""		
Layer 2 - Options, risk, catalyst and market context signals
composite_signals.py
signals/composite_signals.py
Layer 2 — Options, risk, catalyst, and market-context signal modules.

Each class returns a normalised score (0–1) plus raw metrics
for the composite scorer in Layer 3 to consume.
"""

from __future__ import annotations

import time

import requests
from loguru import logger
import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

from config.settings import get_api_config, get_scanner_config
from ingestion.options_feed import TradierOptionsClient
from ingestion.news_feed import FinnhubNewsFeed, RedditMentionTracker
from ingestion.short_data import ShortProfile

_api = get_api_config()
_cfg = get_scanner_config()


# ─────────────────────────────────────────────────────────
#  Options flow signals
# ─────────────────────────────────────────────────────────

class OptionsSignals:
    """
    Put/call ratio, IV rank, sweep activity, and dark pool
    premium relative to average daily dollar volume.
    """

    def __init__(self):
        self._tradier = TradierOptionsClient()

    def compute(
        self,
        ticker: str,
        avg_dollar_volume: float,
        uw_alerts: list[dict],
    ) -> dict:
        """
        Parameters
        ──────────
        ticker            — symbol
        avg_dollar_volume — 10-day average dollar volume (for dark pool sizing)
        uw_alerts         — recent Unusual Whales alerts for this ticker
        """
        opts = self._tradier.get_cached_metrics(ticker)

        pc_ratio    = opts.get("pc_volume_ratio", 1.0)
        avg_iv      = opts.get("avg_iv", 0.0)
        sweep_count = opts.get("sweep_count", 0)

        # Put/call interpretation: < 0.70 bullish, > 1.20 bearish
        if pc_ratio < _cfg.put_call_ratio_bullish:
            pc_score = 1.0
        elif pc_ratio < 1.0:
            pc_score = (1.0 - pc_ratio) / (1.0 - _cfg.put_call_ratio_bullish)
        elif pc_ratio < 1.2:
            pc_score = 0.2
        else:
            pc_score = 0.0

        # Sweep score: more sweeps = stronger conviction signal
        sweep_score = min(sweep_count / 5.0, 1.0)

        # Dark pool premium from UW alerts
        dp_premium = sum(
            a.get("premium", 0)
            for a in uw_alerts
            if "dark_pool" in a.get("type", "").lower()
        )
        dp_score = min(dp_premium / max(avg_dollar_volume, 1) * 10, 1.0)

        # Call sweep only (bullish directional)
        call_sweeps = [
            a for a in uw_alerts
            if a.get("side", "").lower() in ("call", "c")
        ]
        call_premium = sum(a.get("premium", 0) for a in call_sweeps)
        call_score = min(call_premium / max(avg_dollar_volume, 1) * 20, 1.0)

        score = round(
            0.30 * pc_score +
            0.25 * sweep_score +
            0.25 * call_score +
            0.20 * dp_score,
            3,
        )

        return {
            "ticker":        ticker,
            "pc_ratio":      round(pc_ratio, 3),
            "avg_iv":        round(avg_iv, 4),
            "sweep_count":   sweep_count,
            "dp_premium":    round(dp_premium, 0),
            "call_premium":  round(call_premium, 0),
            "pc_bullish":    pc_ratio < _cfg.put_call_ratio_bullish,
            "score":         score,
        }


# ─────────────────────────────────────────────────────────
#  Risk / short squeeze signals
# ─────────────────────────────────────────────────────────

class RiskSignals:
    """
    Beta, ATR-based volatility regime, and short squeeze
    readiness score from the short interest profile.
    """

    def compute(
        self,
        ticker: str,
        short_profile: ShortProfile,
        atr_pct: float,
        beta: float = 1.0,
    ) -> dict:
        """
        Parameters
        ──────────
        short_profile   — from ShortDataClient
        atr_pct         — ATR as % of price (from TechnicalSignals)
        beta            — stock beta vs S&P 500
        """
        # Short squeeze score:
        # High short float + low float + high days-to-cover + high borrow rate
        si_score     = min(short_profile.short_float_pct / 0.40, 1.0)
        float_score  = max(0.0, 1.0 - short_profile.float_shares / 20_000_000)
        dtc_score    = min(short_profile.short_ratio / 20.0, 1.0)
        borrow_score = min(short_profile.borrow_rate_pct / 100.0, 1.0)

        squeeze_score = round(
            0.35 * si_score +
            0.30 * float_score +
            0.25 * dtc_score +
            0.10 * borrow_score,
            3,
        )

        # Volatility regime
        # High ATR = large potential moves = both opportunity and risk
        vol_regime = (
            "extreme" if atr_pct > 0.08 else
            "high"    if atr_pct > 0.04 else
            "normal"  if atr_pct > 0.02 else
            "low"
        )

        # Beta-adjusted risk: penalise extremely high beta in high-VIX regimes
        beta_penalty = min(max(beta - 1.5, 0) / 3.0, 0.5)

        # Overall risk score: higher = more squeeze potential
        score = round(max(0.0, squeeze_score - beta_penalty), 3)

        is_candidate = short_profile.is_squeeze_candidate(
            min_short_float=_cfg.short_float_threshold,
            max_float_shares=_cfg.float_shares_max_squeeze,
            min_days_to_cover=_cfg.short_days_to_cover_min,
        )

        return {
            "ticker":           ticker,
            "short_float_pct":  round(short_profile.short_float_pct * 100, 2),
            "short_ratio":      round(short_profile.short_ratio, 2),
            "float_shares":     short_profile.float_shares,
            "borrow_rate_pct":  short_profile.borrow_rate_pct,
            "beta":             round(beta, 2),
            "atr_pct":          round(atr_pct * 100, 2),
            "vol_regime":       vol_regime,
            "squeeze_candidate": is_candidate,
            "squeeze_score":    squeeze_score,
            "score":            score,
        }


# ─────────────────────────────────────────────────────────
#  Catalyst signals (news + Reddit)
# ─────────────────────────────────────────────────────────

class CatalystSignals:
    """
    News sentiment (FinBERT in AI layer; keyword-based here),
    Reddit mention velocity, and catalyst-type flags.
    """

    def __init__(self):
        self._news = FinnhubNewsFeed()
        self._reddit = RedditMentionTracker()

    def compute(self, ticker: str) -> dict:
        # ── News sentiment ────────────────────────────────────
        news_score_raw = self._news.get_news_sentiment_score(ticker)
        recent_articles = self._news.get_cached_news(ticker, n=5)

        is_fda      = any(a.get("is_fda")      for a in recent_articles)
        is_earnings = any(a.get("is_earnings")  for a in recent_articles)
        is_ma       = any(a.get("is_ma")        for a in recent_articles)

        # Normalize -1..+1 to 0..1
        news_score = (news_score_raw + 1.0) / 2.0

        # Catalyst multiplier: major catalysts boost the score
        catalyst_boost = 0.0
        if is_fda:
            catalyst_boost += 0.20
        if is_earnings:
            catalyst_boost += 0.10
        if is_ma:
            catalyst_boost += 0.15

        # ── Reddit ───────────────────────────────────────────
        reddit_rank     = self._reddit.get_reddit_rank(ticker)
        mentions_hr     = self._reddit.get_ticker_mentions_per_hour(ticker)
        reddit_score    = reddit_rank   # already 0–1

        # Velocity bonus: rapid mention growth
        velocity_bonus = min(mentions_hr / 50.0, 0.30)

        # ── Composite ────────────────────────────────────────
        score = round(
            min(1.0,
                0.50 * news_score +
                0.30 * reddit_score +
                0.10 * velocity_bonus +
                catalyst_boost),
            3,
        )

        return {
            "ticker":          ticker,
            "news_sentiment":  round(news_score_raw, 3),
            "news_score":      round(news_score, 3),
            "is_fda":          is_fda,
            "is_earnings":     is_earnings,
            "is_ma":           is_ma,
            "reddit_rank":     round(reddit_rank, 3),
            "reddit_mentions_hr": mentions_hr,
            "score":           score,
        }


# ─────────────────────────────────────────────────────────
#  Market context signals (VIX, futures, sector)
# ─────────────────────────────────────────────────────────

class MarketContextSignals:
    """
    Reads VIX, S&P 500 futures bias, and sector ETF relative
    strength to produce a market-tailwind/headwind score.
    """

    SECTOR_ETFS = {
        "XLK": "technology",  "XLF": "financials", "XLV": "healthcare",
        "XLE": "energy",      "XLI": "industrials", "XLY": "consumer_disc",
        "XLP": "consumer_st", "XLU": "utilities",  "XLB": "materials",
        "XLRE": "real_estate",
    }

    def fetch_vix(self) -> float:
        """Fetch VIX from yfinance (^VIX)."""
        import yfinance as yf
        try:
            vix_data = yf.Ticker("^VIX").fast_info
            return float(vix_data.last_price or 20.0)
        except Exception:
            return 20.0

    def fetch_futures_bias(self) -> float:
        """
        ES futures bias: % change from prior close.
        Positive = market expects gap-up open.
        """
        import yfinance as yf
        try:
            es = yf.Ticker("ES=F").history(period="2d", interval="1d")
            if len(es) >= 2:
                prev = es["Close"].iloc[-2]
                curr = es["Close"].iloc[-1]
                return float((curr - prev) / prev) if prev else 0.0
        except Exception:
            pass
        return 0.0

    def compute(self, ticker_sector: str | None = None) -> dict:
        vix = self.fetch_vix()
        futures_bias = self.fetch_futures_bias()

        # VIX regime scoring
        if vix < 15:
            vix_score = 1.0    # low fear = risk-on
        elif vix < _cfg.vix_high_regime:
            vix_score = 1.0 - (vix - 15) / (_cfg.vix_high_regime - 15) * 0.5
        elif vix < _cfg.vix_fear_regime:
            vix_score = 0.25   # high fear = only strong setups
        else:
            vix_score = 0.05   # extreme fear = mostly avoid

        # Futures bias score
        futures_score = min(max((futures_bias + 0.02) / 0.04, 0.0), 1.0)

        vix_regime = (
            "extreme_fear" if vix >= _cfg.vix_fear_regime else
            "high_vol"     if vix >= _cfg.vix_high_regime else
            "normal"       if vix >= 15 else
            "low_vol"
        )

        score = round(
            0.60 * vix_score +
            0.40 * futures_score,
            3,
        )

        return {
            "vix":            round(vix, 2),
            "vix_regime":     vix_regime,
            "futures_bias":   round(futures_bias * 100, 3),
            "vix_score":      round(vix_score, 3),
            "futures_score":  round(futures_score, 3),
            "score":          score,
        }