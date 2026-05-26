"""
Layer 3 - Composite scorer that fuses all signal modules
composite_scorer.py
ai/composite_scorer.py
Layer 3a — Weighted signal fusion into a single composite score.

Produces a 0–100 score per ticker, categorised into
penny / mid-cap / large-cap buckets with different weight sets.
Also produces a ranked watchlist with squeeze flags.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from loguru import logger
import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   
from config.settings import get_scanner_config

_cfg = get_scanner_config()

StockBucket = Literal["penny", "midcap", "largecap"]


@dataclass
class SignalBundle:
    """All normalised sub-scores (0–1) for a single ticker."""
    ticker: str
    price: float
    bucket: StockBucket

    # Layer-2 sub-scores (0–1 each)
    volume_score:    float = 0.0
    technical_score: float = 0.0
    risk_score:      float = 0.0
    options_score:   float = 0.0
    catalyst_score:  float = 0.0
    market_score:    float = 0.0

    # Raw metrics (for dashboard / logging)
    rvol:              float = 0.0
    short_float_pct:   float = 0.0
    float_shares:      float = 0.0
    rsi14:             float = 50.0
    bb_squeeze:        bool  = False
    squeeze_candidate: bool  = False
    news_sentiment:    float = 0.0
    vix:               float = 20.0

    # Computed fields
    composite_score: float = field(init=False, default=0.0)
    composite_score_100: int = field(init=False, default=0)
    alert_flags: list[str] = field(default_factory=list)

    def __post_init__(self):
        self._compute()

    def _compute(self):
        weights = (
            _cfg.weights_penny
            if self.bucket == "penny"
            else _cfg.weights_midcap
        )
        self.composite_score = round(
            weights["volume"]    * self.volume_score +
            weights["technical"] * self.technical_score +
            weights["risk"]      * self.risk_score +
            weights["options"]   * self.options_score +
            weights["catalyst"]  * self.catalyst_score +
            weights["market"]    * self.market_score,
            4,
        )
        self.composite_score_100 = int(self.composite_score * 100)
        self._build_flags()

    def _build_flags(self):
        flags = []
        if self.rvol >= 5.0:
            flags.append("🔥 EXTREME RVOL")
        elif self.rvol >= 3.0:
            flags.append("📈 HIGH RVOL")

        if self.squeeze_candidate:
            flags.append("🩳 SQUEEZE SETUP")

        if self.bb_squeeze:
            flags.append("💥 BB SQUEEZE")

        if self.rsi14 > 70:
            flags.append("⚠️ OVERBOUGHT RSI")
        elif self.rsi14 < 35:
            flags.append("💚 OVERSOLD RSI")

        if self.news_sentiment > 0.3:
            flags.append("📰 POSITIVE NEWS")
        elif self.news_sentiment < -0.3:
            flags.append("📰 NEGATIVE NEWS")

        if self.options_score > 0.7:
            flags.append("🦅 UNUSUAL OPTIONS")

        self.alert_flags = flags

    def to_dict(self) -> dict:
        return {
            "ticker":              self.ticker,
            "price":               self.price,
            "bucket":              self.bucket,
            "composite_score":     self.composite_score_100,
            "volume_score":        round(self.volume_score * 100),
            "technical_score":     round(self.technical_score * 100),
            "risk_score":          round(self.risk_score * 100),
            "options_score":       round(self.options_score * 100),
            "catalyst_score":      round(self.catalyst_score * 100),
            "market_score":        round(self.market_score * 100),
            "rvol":                round(self.rvol, 2),
            "rsi14":               round(self.rsi14, 1),
            "short_float_pct":     round(self.short_float_pct, 1),
            "float_shares_M":      round(self.float_shares / 1e6, 2),
            "bb_squeeze":          self.bb_squeeze,
            "squeeze_candidate":   self.squeeze_candidate,
            "news_sentiment":      self.news_sentiment,
            "vix":                 self.vix,
            "flags":               self.alert_flags,
        }


def classify_bucket(price: float) -> StockBucket:
    if price <= _cfg.max_price_penny:
        return "penny"
    elif price <= _cfg.max_price_midcap:
        return "midcap"
    return "largecap"


class CompositeScorer:
    """
    Aggregates all signal bundles and produces a ranked watchlist.
    """

    def __init__(self):
        self._bundles: dict[str, SignalBundle] = {}

    def update(self, bundle: SignalBundle):
        self._bundles[bundle.ticker] = bundle

    def get_ranked(
        self,
        bucket_filter: StockBucket | None = None,
        top_n: int | None = None,
        min_score: int = 0,
        squeeze_only: bool = False,
    ) -> list[dict]:
        """
        Return ranked list of scored stocks.

        Parameters
        ──────────
        bucket_filter  — "penny" | "midcap" | "largecap" | None (all)
        top_n          — limit results
        min_score      — minimum composite score (0–100)
        squeeze_only   — filter to squeeze candidates only
        """
        bundles = list(self._bundles.values())

        if bucket_filter:
            bundles = [b for b in bundles if b.bucket == bucket_filter]

        if squeeze_only:
            bundles = [b for b in bundles if b.squeeze_candidate]

        bundles = [b for b in bundles if b.composite_score_100 >= min_score]

        bundles.sort(key=lambda b: b.composite_score, reverse=True)

        if top_n:
            bundles = bundles[:top_n]

        return [b.to_dict() for b in bundles]

    def get_ticker(self, ticker: str) -> dict | None:
        b = self._bundles.get(ticker.upper())
        return b.to_dict() if b else None

    def top_alerts(self, n: int = 5) -> list[dict]:
        """Top-N highest scoring stocks with any alert flags."""
        ranked = self.get_ranked(top_n=n * 3)
        flagged = [r for r in ranked if r["flags"]]
        return flagged[:n]