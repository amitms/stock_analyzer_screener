"""
Layer 4 - Output: Discord alerts, watchlist persistence, Plotly Dash dashboard
alerts.py

output/alerts.py
Layer 4 — Alert delivery and result persistence.

• Discord webhook alerts for top-scoring stocks
• SQLite persistence for historical scan results
• Ranked watchlist export (JSON / CSV)
"""

from __future__ import annotations

import csv
import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import requests
from loguru import logger
import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

from config.settings import get_api_config, get_path_config, get_scanner_config

_api   = get_api_config()
_paths = get_path_config()
_cfg   = get_scanner_config()

DB_PATH = _paths.data / "scan_results.db"


# ─────────────────────────────────────────────────────────
#  SQLite persistence
# ─────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scan_results (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker           TEXT NOT NULL,
            scan_ts          INTEGER NOT NULL,
            scan_date        TEXT NOT NULL,
            price            REAL,
            bucket           TEXT,
            composite_score  INTEGER,
            volume_score     INTEGER,
            technical_score  INTEGER,
            risk_score       INTEGER,
            options_score    INTEGER,
            catalyst_score   INTEGER,
            market_score     INTEGER,
            rvol             REAL,
            rsi14            REAL,
            short_float_pct  REAL,
            float_shares_M   REAL,
            bb_squeeze       INTEGER,
            squeeze_candidate INTEGER,
            news_sentiment   REAL,
            vix              REAL,
            flags            TEXT,
            breakout_direction TEXT,
            breakout_prob    REAL,
            breakout_conf    TEXT,
            price_target_mid REAL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ticker_ts ON scan_results(ticker, scan_ts)
    """)
    conn.commit()
    conn.close()


def save_results(results: list[dict], predictions: dict[str, dict] | None = None):
    """Persist ranked scan results to SQLite."""
    predictions = predictions or {}
    init_db()
    conn = sqlite3.connect(DB_PATH)
    now  = int(time.time())
    date = datetime.utcfromtimestamp(now).strftime("%Y-%m-%d")

    rows = []
    for r in results:
        pred = predictions.get(r["ticker"], {})
        rows.append((
            r["ticker"], now, date,
            r.get("price"),          r.get("bucket"),
            r.get("composite_score"), r.get("volume_score"),
            r.get("technical_score"), r.get("risk_score"),
            r.get("options_score"),   r.get("catalyst_score"),
            r.get("market_score"),    r.get("rvol"),
            r.get("rsi14"),           r.get("short_float_pct"),
            r.get("float_shares_M"),  int(r.get("bb_squeeze", False)),
            int(r.get("squeeze_candidate", False)),
            r.get("news_sentiment"),  r.get("vix"),
            json.dumps(r.get("flags", [])),
            pred.get("direction"),    pred.get("bullish_prob"),
            pred.get("confidence"),   pred.get("price_target_mid"),
        ))

    conn.executemany("""
        INSERT INTO scan_results (
          ticker, scan_ts, scan_date, price, bucket,
          composite_score, volume_score, technical_score, risk_score,
          options_score, catalyst_score, market_score, rvol, rsi14,
          short_float_pct, float_shares_M, bb_squeeze, squeeze_candidate,
          news_sentiment, vix, flags,
          breakout_direction, breakout_prob, breakout_conf, price_target_mid
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    conn.close()
    logger.info(f"Saved {len(rows)} results to DB")


def export_csv(results: list[dict], path: Path | None = None) -> Path:
    path = path or _paths.data / f"scan_{int(time.time())}.csv"
    if not results:
        return path
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    logger.info(f"Exported CSV → {path}")
    return path


# ─────────────────────────────────────────────────────────
#  Discord alerts
# ─────────────────────────────────────────────────────────

def _score_bar(score: int, width: int = 10) -> str:
    filled = round(score / 100 * width)
    return "█" * filled + "░" * (width - filled)


def send_discord_alert(
    results: list[dict],
    predictions: dict[str, dict] | None = None,
    title: str = "🔔 Stock Scanner Alert",
):
    """
    Post top scan results to a Discord channel via webhook.
    Sends up to 5 stocks per message to stay within embed limits.
    """
    if not _api.discord_webhook_url:
        logger.warning("Discord webhook not configured — skipping alert")
        return

    predictions = predictions or {}
    embeds      = []

    for r in results[:5]:
        ticker = r["ticker"]
        pred   = predictions.get(ticker, {})
        flags  = " ".join(r.get("flags", []))

        direction_str = ""
        if pred:
            arrow = "⬆️" if pred.get("direction") == "bullish" else "⬇️" if pred.get("direction") == "bearish" else "➡️"
            direction_str = (
                f"\n**AI Prediction:** {arrow} {pred.get('direction','').upper()} "
                f"({pred.get('bullish_prob', 0):.0f}% bull) | "
                f"Conf: {pred.get('confidence','N/A')} | "
                f"Target: ${pred.get('price_target_mid', 0):.2f}"
            )

        embed = {
            "title": f"${ticker} — Score: {r['composite_score']}/100",
            "color": (
                0x00ff88 if r["composite_score"] >= 75 else
                0xffaa00 if r["composite_score"] >= 50 else
                0xaaaaaa
            ),
            "fields": [
                {
                    "name": "📊 Signal Scores",
                    "value": (
                        f"**Vol:** {r['volume_score']} | "
                        f"**Tech:** {r['technical_score']} | "
                        f"**Risk:** {r['risk_score']}\n"
                        f"**Opts:** {r['options_score']} | "
                        f"**Cat:** {r['catalyst_score']} | "
                        f"**Mkt:** {r['market_score']}"
                    ),
                    "inline": False,
                },
                {
                    "name": "📈 Key Metrics",
                    "value": (
                        f"Price: **${r.get('price', 0):.2f}** | "
                        f"Bucket: {r.get('bucket','?').upper()}\n"
                        f"RVOL: **{r.get('rvol', 0):.1f}×** | "
                        f"RSI: {r.get('rsi14', 0):.1f} | "
                        f"Short Float: {r.get('short_float_pct', 0):.1f}% | "
                        f"Float: {r.get('float_shares_M', 0):.1f}M"
                    ),
                    "inline": False,
                },
                {
                    "name": "🏳️ Flags",
                    "value": flags or "None",
                    "inline": False,
                },
            ],
            "description": (
                f"```{_score_bar(r['composite_score'])}``` {direction_str}"
            ),
            "footer": {"text": f"VIX: {r.get('vix', 0):.1f} | "
                                f"{datetime.utcnow().strftime('%H:%M UTC')}"},
        }
        embeds.append(embed)

    payload = {"username": "StockScanner", "embeds": embeds,
                "content": f"**{title}** — {len(results)} stocks ranked"}
    try:
        resp = requests.post(_api.discord_webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Discord alert sent")
    except Exception as exc:
        logger.error(f"Discord alert failed: {exc}")

def send_squeeze_alert(squeeze_alert: dict):
    """Dedicated high-priority Discord ping for squeeze setups."""
    if not _api.discord_webhook_url:
        return

    ticker = squeeze_alert["ticker"]
    payload = {
        "username": "SqueezeDetector",
        "content":  f"🩳 **SHORT SQUEEZE ALERT** 🩳 — `${ticker}`",
        "embeds": [{
            "title":       f"${ticker} Squeeze Setup — {squeeze_alert['confidence']} Confidence",
            "color":       0xff0000 if squeeze_alert["confidence"] == "HIGH" else 0xff8800,
            "description": "\n".join(f"• {t}" for t in squeeze_alert["triggers"]),
            "fields": [
                {"name": "Short Float",    "value": f"{squeeze_alert['short_float_pct']:.1f}%",  "inline": True},
                {"name": "Days to Cover",  "value": f"{squeeze_alert['short_ratio']:.1f}",        "inline": True},
                {"name": "Float",          "value": f"{squeeze_alert['float_shares_M']:.1f}M",    "inline": True},
                {"name": "RVOL",           "value": f"{squeeze_alert['rvol']:.1f}×",              "inline": True},
                {"name": "Call Sweeps",    "value": "✅" if squeeze_alert["has_call_sweeps"] else "❌", "inline": True},
                {"name": "Dark Pool",      "value": "✅" if squeeze_alert["has_dark_pool"] else "❌",  "inline": True},
                {"name": "Borrow Rate",    "value": f"{squeeze_alert['borrow_rate']:.1f}%",        "inline": True},
                {"name": "Squeeze Score",  "value": f"{squeeze_alert['squeeze_score']}/100",        "inline": True},
            ],
        }],
    }
    try:
        requests.post(_api.discord_webhook_url, json=payload, timeout=10)
    except Exception as exc:
        logger.error(f"Squeeze alert failed: {exc}")