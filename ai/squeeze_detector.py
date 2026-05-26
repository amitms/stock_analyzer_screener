"""
Layer 3 - Short squeeze detector and FinBERT NLP sentiment
squeeze_detector.py

ai/squeeze_detector.py
Layer 3c — Rules-based short squeeze detector + FinBERT NLP sentiment scorer.

The squeeze detector is intentionally kept rule-based for interpretability.
FinBERT replaces the keyword scoring in news_feed.py for higher accuracy.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from loguru import logger
import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

from config.settings import get_scanner_config
from ingestion.short_data import ShortProfile

_cfg = get_scanner_config()

# ─────────────────────────────────────────────────────────
#  Squeeze detector
# ─────────────────────────────────────────────────────────

@dataclass
class SqueezeAlert:
    ticker: str
    score: float                   # 0–1
    confidence: str                # "HIGH" | "MEDIUM" | "LOW"
    triggers: list[str]            # human-readable trigger list
    short_float_pct: float
    short_ratio: float
    float_shares: float
    rvol: float
    has_call_sweeps: bool
    has_dark_pool: bool
    borrow_rate: float
    ts: int

    def to_dict(self) -> dict:
        return {
            "ticker":          self.ticker,
            "squeeze_score":   round(self.score * 100),
            "confidence":      self.confidence,
            "triggers":        self.triggers,
            "short_float_pct": round(self.short_float_pct, 2),
            "short_ratio":     round(self.short_ratio, 2),
            "float_shares_M":  round(self.float_shares / 1e6, 2),
            "rvol":            round(self.rvol, 2),
            "has_call_sweeps": self.has_call_sweeps,
            "has_dark_pool":   self.has_dark_pool,
            "borrow_rate":     round(self.borrow_rate, 2),
        }


class SqueezeDetector:
    """
    Multi-factor squeeze scoring:
    1. High short float (≥ 20%)
    2. Low float shares (< 20M)
    3. High days-to-cover (≥ 5)
    4. RVOL spike (≥ 3×)
    5. Unusual call option sweeps
    6. Dark pool confirmation prints
    7. High borrow rate (>10% annualised)
    8. Price above key moving average (momentum confirmation)
    """

    def evaluate(
        self,
        ticker: str,
        short_profile: ShortProfile,
        rvol: float,
        uw_alerts: list[dict],
        price: float,
        ema21: float,
    ) -> SqueezeAlert | None:
        """
        Returns a SqueezeAlert if any squeeze criteria are met,
        else returns None.
        """
        triggers   = []
        score_pts  = 0.0
        max_pts    = 8.0

        # ── Criterion 1: Short float ─────────────────────────
        sf = short_profile.short_float_pct
        if sf >= 0.40:
            score_pts += 2.0
            triggers.append(f"Very high short float: {sf*100:.1f}%")
        elif sf >= _cfg.short_float_threshold:
            score_pts += 1.0
            triggers.append(f"High short float: {sf*100:.1f}%")

        # ── Criterion 2: Float size ───────────────────────────
        fs = short_profile.float_shares
        if fs <= 5_000_000:
            score_pts += 2.0
            triggers.append(f"Micro float: {fs/1e6:.1f}M shares")
        elif fs <= _cfg.float_shares_max_squeeze:
            score_pts += 1.0
            triggers.append(f"Low float: {fs/1e6:.1f}M shares")

        # ── Criterion 3: Days-to-cover ────────────────────────
        dtc = short_profile.short_ratio
        if dtc >= 10.0:
            score_pts += 1.5
            triggers.append(f"High days-to-cover: {dtc:.1f}")
        elif dtc >= _cfg.short_days_to_cover_min:
            score_pts += 0.75
            triggers.append(f"Elevated days-to-cover: {dtc:.1f}")

        # ── Criterion 4: RVOL spike ───────────────────────────
        if rvol >= _cfg.rvol_squeeze_threshold:
            score_pts += 1.5
            triggers.append(f"Volume spike: {rvol:.1f}× avg")
        elif rvol >= _cfg.rvol_alert_threshold:
            score_pts += 0.75
            triggers.append(f"Elevated RVOL: {rvol:.1f}×")

        # ── Criterion 5: Call sweeps ──────────────────────────
        call_sweeps = [
            a for a in uw_alerts
            if a.get("side", "").lower() in ("call", "c")
            and a.get("premium", 0) >= 50_000
        ]
        has_calls = len(call_sweeps) > 0
        if has_calls:
            score_pts += 1.0
            total_call_prem = sum(a.get("premium", 0) for a in call_sweeps)
            triggers.append(f"Call sweeps: ${total_call_prem:,.0f} premium")

        # ── Criterion 6: Dark pool confirmation ───────────────
        dp_prints = [
            a for a in uw_alerts
            if "dark_pool" in a.get("type", "").lower()
        ]
        has_dp = len(dp_prints) > 0
        if has_dp:
            score_pts += 1.0
            triggers.append(f"Dark pool prints: {len(dp_prints)}")

        # ── Criterion 7: High borrow rate ─────────────────────
        borrow = short_profile.borrow_rate_pct
        if borrow >= 50.0:
            score_pts += 1.0
            triggers.append(f"Hard-to-borrow: {borrow:.1f}% fee")
        elif borrow >= 10.0:
            score_pts += 0.5
            triggers.append(f"Elevated borrow rate: {borrow:.1f}%")

        # ── Criterion 8: Price momentum confirmation ──────────
        if price > ema21 and ema21 > 0:
            score_pts += 0.5
            triggers.append(f"Price above EMA21 (momentum)")

        # Skip if not enough criteria met
        if score_pts < 2.0 or not triggers:
            return None

        score = min(score_pts / max_pts, 1.0)
        confidence = (
            "HIGH"   if score >= 0.70 else
            "MEDIUM" if score >= 0.45 else
            "LOW"
        )

        return SqueezeAlert(
            ticker=ticker,
            score=round(score, 3),
            confidence=confidence,
            triggers=triggers,
            short_float_pct=sf * 100,
            short_ratio=dtc,
            float_shares=fs,
            rvol=rvol,
            has_call_sweeps=has_calls,
            has_dark_pool=has_dp,
            borrow_rate=borrow,
            ts=int(time.time()),
        )


# ─────────────────────────────────────────────────────────
#  FinBERT NLP sentiment scorer
# ─────────────────────────────────────────────────────────

class FinBERTScorer:
    """
    Uses ProsusAI/finbert to score news headlines and summaries
    with financial-domain sentiment (positive / negative / neutral).

    Requires: pip install transformers torch
    Falls back to keyword scoring if model not available.
    """

    MODEL_NAME = "ProsusAI/finbert"

    def __init__(self):
        self._pipeline = None
        self._load()

    def _load(self):
        try:
            from transformers import pipeline
            self._pipeline = pipeline(
                "text-classification",
                model=self.MODEL_NAME,
                truncation=True,
                max_length=512,
            )
            logger.info("FinBERT loaded successfully")
        except Exception as exc:
            logger.warning(f"FinBERT unavailable, using keyword fallback: {exc}")

    def score_headline(self, text: str) -> float:
        """
        Returns a sentiment score in [-1, +1]:
          +1 = strongly positive (bullish)
          -1 = strongly negative (bearish)
           0 = neutral
        """
        if not self._pipeline:
            return self._keyword_fallback(text)

        try:
            result = self._pipeline(text[:512])[0]
            label  = result["label"].lower()
            score  = result["score"]

            if label == "positive":
                return round(score, 3)
            elif label == "negative":
                return round(-score, 3)
            else:
                return 0.0
        except Exception as exc:
            logger.debug(f"FinBERT inference error: {exc}")
            return self._keyword_fallback(text)

    def score_articles(self, articles: list[dict]) -> float:
        """
        Score a list of news articles (with 'headline' and 'summary' fields).
        Returns a recency-weighted aggregate score [-1, +1].
        """
        if not articles:
            return 0.0

        total, weight_sum = 0.0, 0.0
        for i, art in enumerate(articles):
            text   = f"{art.get('headline', '')} {art.get('summary', '')}".strip()
            weight = 1.0 / (i + 1)
            total += self.score_headline(text) * weight
            weight_sum += weight

        return round(total / weight_sum, 3) if weight_sum else 0.0

    @staticmethod
    def _keyword_fallback(text: str) -> float:
        text_lower = text.lower()
        pos = sum(1 for w in [
            "beat", "beats", "record", "upgrade", "approval", "surge",
            "strong", "positive", "raises", "guidance", "buyback", "deal",
        ] if w in text_lower)
        neg = sum(1 for w in [
            "miss", "misses", "downgrade", "lawsuit", "decline", "warning",
            "loss", "cut", "fraud", "recall", "rejected", "delay",
        ] if w in text_lower)
        raw = pos - neg
        return round(max(-1.0, min(1.0, raw / max(abs(raw), 1))), 3)