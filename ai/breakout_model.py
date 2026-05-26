

"""
Layer 3 - ML breakout prediction model (XGBoost + LightGBM)
breakout_model.py
ai/breakout_model.py
Layer 3b — XGBoost / LightGBM model that predicts next-day
price breakout direction and magnitude.

Training
────────
  python -m ai.breakout_model train --universe sp500 --lookback 365
  python -m ai.breakout_model train --tickers AAPL,TSLA --lookback 180

Inference
─────────
  model.predict(signal_bundle) → BreakoutPrediction
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import yfinance as yf
from loguru import logger

try:
    import xgboost as xgb
    import lightgbm as lgb
    HAVE_BOOSTERS = True
except ImportError:
    HAVE_BOOSTERS = False
    logger.warning("xgboost / lightgbm not installed — ML predictions disabled")

try:
    import optuna
    HAVE_OPTUNA = True
except ImportError:
    HAVE_OPTUNA = False

from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

from config.settings import get_path_config, get_scanner_config
import pandas_ta as ta

_cfg = get_scanner_config()
_paths = get_path_config()


@dataclass
class BreakoutPrediction:
    ticker: str
    direction_prob: float      # P(price > +5% next day)
    bearish_prob:   float      # P(price < -5% next day)
    neutral_prob:   float      # remainder
    predicted_direction: str   # "bullish" | "bearish" | "neutral"
    confidence: str            # "high" | "medium" | "low"
    price_target_low: float    # 10th-percentile next-day return estimate
    price_target_mid: float    # median
    price_target_high: float   # 90th-percentile
    current_price: float
    model_version: str
    ts: int

    def to_dict(self) -> dict:
        return {
            "ticker":              self.ticker,
            "direction":           self.predicted_direction,
            "bullish_prob":        round(self.direction_prob * 100, 1),
            "bearish_prob":        round(self.bearish_prob * 100, 1),
            "neutral_prob":        round(self.neutral_prob * 100, 1),
            "confidence":          self.confidence,
            "price_target_low":    round(self.price_target_low, 2),
            "price_target_mid":    round(self.price_target_mid, 2),
            "price_target_high":   round(self.price_target_high, 2),
            "current_price":       self.current_price,
            "model_version":       self.model_version,
        }


# ─────────────────────────────────────────────────────────
#  Feature engineering
# ─────────────────────────────────────────────────────────

FEATURE_COLS = [
    # Volume
    "rvol", "avg_vol_10d", "float_turnover", "vol_trend_5d",
    # Technical
    "rsi14", "macd_hist", "bb_width", "bb_pct_b",
    "ema9_vs_21", "ema21_vs_50", "atr_pct", "vwap_deviation",
    "pct_from_52w_high",
    # Risk / short
    "short_float_pct", "short_ratio", "borrow_rate",
    # Options / flow
    "pc_ratio", "avg_iv", "sweep_count",
    # Catalyst
    "news_sentiment",
    # Market
    "vix", "futures_bias",
]


def build_features_from_history(ticker: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all features from raw OHLCV history.
    Returns a DataFrame with FEATURE_COLS and a 'label' column.
    label = 1 if next-day return > breakout_pct_target, else 0.
    """
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # Labels: next-day return > threshold
    df["next_ret"] = df["close"].shift(-1) / df["close"] - 1
    df["label"]    = (df["next_ret"] > _cfg.breakout_pct_target).astype(int)

    # Volume features
    df["avg_vol_10d"]    = df["volume"].rolling(10).mean()
    df["avg_vol_5d"]     = df["volume"].rolling(5).mean()
    df["rvol"]           = df["volume"] / df["avg_vol_10d"].replace(0, np.nan)
    df["float_turnover"] = 0.0   # placeholder — filled from short data in live use
    df["vol_trend_5d"]   = df["volume"] / df["avg_vol_5d"].replace(0, np.nan)

    # Technical features
    df["rsi14"]    = ta.rsi(df["close"], length=14)
    macd           = ta.macd(df["close"])
    if macd is not None:
        df["macd_hist"] = macd.iloc[:, 2]
    else:
        df["macd_hist"] = 0.0

    bbands = ta.bbands(df["close"], length=20, std=2.0)
    if bbands is not None:
        df["bb_upper"] = bbands.iloc[:, 0]
        df["bb_mid"]   = bbands.iloc[:, 1]
        df["bb_lower"] = bbands.iloc[:, 2]
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]
        df["bb_pct_b"] = ((df["close"] - df["bb_lower"]) /
                          (df["bb_upper"] - df["bb_lower"]).replace(0, np.nan))
    else:
        df["bb_width"] = 0.0
        df["bb_pct_b"] = 0.5

    ema9     = ta.ema(df["close"], length=9)
    ema21    = ta.ema(df["close"], length=21)
    ema50    = ta.ema(df["close"], length=50)
    df["ema9_vs_21"]  = (ema9 / ema21.replace(0, np.nan)) - 1
    df["ema21_vs_50"] = (ema21 / ema50.replace(0, np.nan)) - 1

    atr = ta.atr(df["high"], df["low"], df["close"], length=14)
    df["atr_pct"]  = atr / df["close"].replace(0, np.nan)

    df["rolling_high_52w"]  = df["high"].rolling(252).max()
    df["pct_from_52w_high"] = df["close"] / df["rolling_high_52w"].replace(0, np.nan) - 1
    df["vwap_deviation"]    = 0.0   # approximated as 0 for daily data

    # Placeholder columns filled from live signals in production
    for col in ["short_float_pct", "short_ratio", "borrow_rate",
                "pc_ratio", "avg_iv", "sweep_count",
                "news_sentiment", "futures_bias"]:
        if col not in df.columns:
            df[col] = 0.0

    # VIX from yfinance (daily)
    try:
        vix = yf.download("^VIX", start=df.index[0], end=df.index[-1],
                          progress=False)["Close"]
        vix.index = pd.to_datetime(vix.index)
        df["vix"] = vix.reindex(df.index, method="ffill")
    except Exception:
        df["vix"] = 20.0

    df = df.dropna(subset=["label"])
    available = [c for c in FEATURE_COLS if c in df.columns]
    return df[available + ["label", "next_ret"]].dropna()


def _live_bundle_to_features(bundle: dict) -> pd.DataFrame:
    """Convert a live SignalBundle.to_dict() into a feature row."""
    row = {
        "rvol":            bundle.get("rvol", 1.0),
        "avg_vol_10d":     1.0,   # normalised — not used raw in live mode
        "float_turnover":  0.0,
        "vol_trend_5d":    1.0,
        "rsi14":           bundle.get("rsi14", 50.0),
        "macd_hist":       0.0,
        "bb_width":        0.0,
        "bb_pct_b":        0.5,
        "ema9_vs_21":      0.0,
        "ema21_vs_50":     0.0,
        "atr_pct":         0.0,
        "vwap_deviation":  0.0,
        "pct_from_52w_high": 0.0,
        "short_float_pct": bundle.get("short_float_pct", 0) / 100,
        "short_ratio":     0.0,
        "borrow_rate":     0.0,
        "pc_ratio":        1.0,
        "avg_iv":          0.0,
        "sweep_count":     0,
        "news_sentiment":  bundle.get("news_sentiment", 0.0),
        "vix":             bundle.get("vix", 20.0),
        "futures_bias":    0.0,
    }
    return pd.DataFrame([row])[FEATURE_COLS]


# ─────────────────────────────────────────────────────────
#  Model class
# ─────────────────────────────────────────────────────────

class BreakoutModel:
    """
    Trains and serves a calibrated XGBoost breakout-direction classifier.
    Optionally tunes hyperparameters with Optuna.
    """

    MODEL_FILE   = _paths.models / "breakout_xgb.pkl"
    SCALER_FILE  = _paths.models / "breakout_scaler.pkl"
    META_FILE    = _paths.models / "breakout_meta.json"

    def __init__(self):
        self._model: CalibratedClassifierCV | None = None
        self._scaler: StandardScaler | None = None
        self._meta: dict = {}
        self._try_load()

    # ──────────────────────────────────────────────────────
    #  Training
    # ──────────────────────────────────────────────────────

    def train(
        self,
        tickers: list[str],
        lookback_days: int = 365,
        tune: bool = False,
        n_trials: int = 50,
    ):
        """
        Train on historical data for the given ticker universe.

        Parameters
        ──────────
        tune     — run Optuna hyperparameter search before fitting
        n_trials — number of Optuna trials (only used when tune=True)
        """
        logger.info(f"Building training set from {len(tickers)} tickers…")
        frames = []
        for ticker in tickers:
            try:
                period = f"{lookback_days}d"
                df_raw = yf.download(ticker, period=period, progress=False,
                                     auto_adjust=True)
                if len(df_raw) < 60:
                    continue
                feat_df = build_features_from_history(ticker, df_raw)
                frames.append(feat_df)
            except Exception as exc:
                logger.warning(f"Train skip {ticker}: {exc}")

        if not frames:
            raise ValueError("No usable training data")

        full = pd.concat(frames, ignore_index=True)
        X = full[FEATURE_COLS].values
        y = full["label"].values

        logger.info(f"Dataset: {len(X)} samples, "
                    f"label balance: {y.mean():.1%} positive")

        # Scale
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)

        # Tune or use defaults
        if tune and HAVE_OPTUNA:
            best_params = self._tune_xgb(X_scaled, y, n_trials)
        else:
            best_params = {
                "n_estimators":      300,
                "max_depth":         6,
                "learning_rate":     0.05,
                "subsample":         0.8,
                "colsample_bytree":  0.8,
                "min_child_weight":  3,
                "gamma":             0.1,
                "scale_pos_weight":  (y == 0).sum() / (y == 1).sum(),
                "use_label_encoder": False,
                "eval_metric":       "logloss",
                "random_state":      42,
            }

        base = xgb.XGBClassifier(**best_params)
        self._model = CalibratedClassifierCV(base, cv=TimeSeriesSplit(n_splits=5),
                                             method="isotonic")
        self._model.fit(X_scaled, y)

        # Evaluate
        proba = self._model.predict_proba(X_scaled)[:, 1]
        auc   = roc_auc_score(y, proba)
        logger.info(f"Training AUC: {auc:.4f}")

        # Persist
        joblib.dump(self._model,  self.MODEL_FILE)
        joblib.dump(self._scaler, self.SCALER_FILE)
        self._meta = {
            "auc":       auc,
            "n_samples": len(X),
            "n_tickers": len(tickers),
            "features":  FEATURE_COLS,
            "trained_at": int(time.time()),
        }
        self.META_FILE.write_text(json.dumps(self._meta, indent=2))
        logger.success(f"Model saved → {self.MODEL_FILE}")

    def _tune_xgb(self, X: np.ndarray, y: np.ndarray, n_trials: int) -> dict:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        def objective(trial):
            params = {
                "n_estimators":     trial.suggest_int("n_estimators", 100, 500),
                "max_depth":        trial.suggest_int("max_depth", 3, 8),
                "learning_rate":    trial.suggest_float("lr", 0.01, 0.2, log=True),
                "subsample":        trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree": trial.suggest_float("colsample", 0.5, 1.0),
                "min_child_weight": trial.suggest_int("mcw", 1, 10),
                "gamma":            trial.suggest_float("gamma", 0.0, 1.0),
                "scale_pos_weight": (y == 0).sum() / max((y == 1).sum(), 1),
                "use_label_encoder": False,
                "eval_metric": "logloss",
            }
            clf = xgb.XGBClassifier(**params)
            tscv = TimeSeriesSplit(n_splits=3)
            scores = []
            for tr, va in tscv.split(X):
                clf.fit(X[tr], y[tr])
                p = clf.predict_proba(X[va])[:, 1]
                scores.append(roc_auc_score(y[va], p))
            return np.mean(scores)

        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        logger.info(f"Best AUC from Optuna: {study.best_value:.4f}")
        return {**study.best_params,
                "use_label_encoder": False, "eval_metric": "logloss",
                "scale_pos_weight": (y == 0).sum() / max((y == 1).sum(), 1)}

    # ──────────────────────────────────────────────────────
    #  Inference
    # ──────────────────────────────────────────────────────

    def predict(self, bundle: dict) -> BreakoutPrediction | None:
        if not self._model or not self._scaler:
            logger.warning("No trained model — run train() first")
            return None

        ticker = bundle.get("ticker", "UNKNOWN")
        price  = bundle.get("price", 0.0)

        X_raw    = _live_bundle_to_features(bundle).values
        X_scaled = self._scaler.transform(X_raw)
        proba    = self._model.predict_proba(X_scaled)[0]

        bull_prob = float(proba[1])
        bear_prob = max(0.0, 1.0 - bull_prob - 0.30)   # simplified
        neut_prob = max(0.0, 1.0 - bull_prob - bear_prob)

        if bull_prob >= _cfg.breakout_probability_threshold:
            direction = "bullish"
        elif bear_prob >= _cfg.breakout_probability_threshold:
            direction = "bearish"
        else:
            direction = "neutral"

        confidence = (
            "high"   if max(bull_prob, bear_prob) >= 0.75 else
            "medium" if max(bull_prob, bear_prob) >= 0.60 else
            "low"
        )

        # Rough price targets using ATR
        atr_pct = bundle.get("atr_pct", 0.03)
        return BreakoutPrediction(
            ticker=ticker,
            direction_prob=round(bull_prob, 4),
            bearish_prob=round(bear_prob, 4),
            neutral_prob=round(neut_prob, 4),
            predicted_direction=direction,
            confidence=confidence,
            price_target_low=round(price * (1 - atr_pct * 1.5), 2),
            price_target_mid=round(price * (1 + atr_pct), 2),
            price_target_high=round(price * (1 + atr_pct * 2.5), 2),
            current_price=price,
            model_version=str(self._meta.get("trained_at", "unknown")),
            ts=int(time.time()),
        )

    # ──────────────────────────────────────────────────────
    #  Load / save
    # ──────────────────────────────────────────────────────

    def _try_load(self):
        if self.MODEL_FILE.exists() and self.SCALER_FILE.exists():
            try:
                self._model  = joblib.load(self.MODEL_FILE)
                self._scaler = joblib.load(self.SCALER_FILE)
                if self.META_FILE.exists():
                    self._meta = json.loads(self.META_FILE.read_text())
                logger.info(f"Loaded breakout model (AUC={self._meta.get('auc', 'N/A')})")
            except Exception as exc:
                logger.warning(f"Could not load model: {exc}")


# ─────────────────────────────────────────────────────────
#  CLI entry point
# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train breakout model")
    parser.add_argument("action", choices=["train"])
    parser.add_argument("--tickers", default="AAPL,TSLA,NVDA,AMD,MARA,CLSK",
                        help="Comma-separated tickers")
    parser.add_argument("--lookback", type=int, default=365)
    parser.add_argument("--tune", action="store_true")
    parser.add_argument("--trials", type=int, default=30)
    args = parser.parse_args()

    if args.action == "train":
        ticker_list = [t.strip().upper() for t in args.tickers.split(",")]
        model = BreakoutModel()
        model.train(ticker_list, lookback_days=args.lookback,
                    tune=args.tune, n_trials=args.trials)