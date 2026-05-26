"""
tests/test_alerts.py
Full pytest suite for output/alerts.py

No network calls, no real SQLite on disk (tmp_path fixture),
no Discord webhook traffic (requests.post is patched).

Run
───
  pip install pytest
  pytest tests/test_alerts.py -v

Coverage
────────
  _score_bar
    score=0    → all░ (no filled)
    score=100  → all█ (fully filled)
    score=50   → half filled, half░
    total length always equals width
    custom width respected
    filled + empty = width for any score

  init_db
    creates scan_results table
    creates idx_ticker_ts index
    is idempotent (safe to call twice)
    DB file is created at DB_PATH

  save_results
    empty list → zero rows inserted
    single result inserted with correct column values
    multiple results inserted in one call
    predictions dict merged into rows by ticker
    missing prediction keys default to None
    bb_squeeze / squeeze_candidate stored as 0/1 integers
    flags stored as JSON string
    ticker key is used for pred lookup
    scan_ts and scan_date set to current time/date
    DB_PATH created if not exists (calls init_db)

  export_csv
    empty results → file not written, path returned
    single result → CSV file with header + one data row
    multiple results → all rows present
    headers match result dict keys
    custom path respected
    default path auto-generated when path=None
    returned path exists on disk

  send_discord_alert
    no webhook URL → returns immediately, no HTTP call
    builds payload with embeds list
    truncates to 5 embeds when more than 5 results
    content field contains title and count
    username is "StockScanner"
    embed color 0x00ff88 for score >= 75
    embed color 0xffaa00 for score 50–74
    embed color 0xaaaaaa for score < 50
    score_bar included in description
    bullish prediction → ⬆️ arrow in description
    bearish prediction → ⬇️ arrow in description
    neutral prediction → ➡️ arrow in description
    no prediction dict → direction_str is empty
    empty flags → "None" in flags field
    flags joined with space
    HTTP error → logged, no exception raised
    requests.post called once with correct URL

  send_squeeze_alert
    no webhook URL → returns immediately, no HTTP call
    username is "SqueezeDetector"
    content contains ticker name
    HIGH confidence → red color (0xff0000)
    MEDIUM confidence → orange color (0xff8800)
    triggers joined with bullet points in description
    all field names present in embed fields
    requests.post called with correct URL
    HTTP error logged, no exception raised
"""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
import os  		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

import time
import types
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from dotenv import load_dotenv, dotenv_values

import pytest

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

import output.alerts as alerts_mod
from output.alerts import (
    _score_bar,
    export_csv,
    init_db,
    save_results,
    send_discord_alert,
    send_squeeze_alert,
)


# ─────────────────────────────────────────────────────────
#  Fixtures & helpers
# ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def patch_db_path(tmp_path):
    """Redirect DB_PATH to a temp directory for every test."""
    db_file = tmp_path / "scan_results.db"
    with patch.object(alerts_mod, "DB_PATH", db_file):
        yield db_file


@pytest.fixture(autouse=True)
def patch_data_path(tmp_path):
    """Redirect _paths.data to tmp_path so CSV exports go there."""
    mock_paths = MagicMock()
    mock_paths.data = tmp_path
    with patch.object(alerts_mod, "_paths", mock_paths):
        yield tmp_path


def make_result(
    ticker: str = "AAPL",
    composite_score: int = 65,
    price: float = 150.0,
    bucket: str = "midcap",
    rvol: float = 2.5,
    rsi14: float = 55.0,
    short_float_pct: float = 5.0,
    float_shares_M: float = 15.0,
    bb_squeeze: bool = False,
    squeeze_candidate: bool = False,
    news_sentiment: float = 0.1,
    vix: float = 20.0,
    flags: list[str] | None = None,
    volume_score: int = 60,
    technical_score: int = 65,
    risk_score: int = 50,
    options_score: int = 40,
    catalyst_score: int = 55,
    market_score: int = 70,
) -> dict:
    return {
        "ticker":           ticker,
        "composite_score":  composite_score,
        "price":            price,
        "bucket":           bucket,
        "rvol":             rvol,
        "rsi14":            rsi14,
        "short_float_pct":  short_float_pct,
        "float_shares_M":   float_shares_M,
        "bb_squeeze":       bb_squeeze,
        "squeeze_candidate":squeeze_candidate,
        "news_sentiment":   news_sentiment,
        "vix":              vix,
        "flags":            flags or [],
        "volume_score":     volume_score,
        "technical_score":  technical_score,
        "risk_score":       risk_score,
        "options_score":    options_score,
        "catalyst_score":   catalyst_score,
        "market_score":     market_score,
    }


def make_prediction(
    direction: str = "bullish",
    bullish_prob: float = 72.0,
    confidence: str = "high",
    price_target_mid: float = 155.0,
) -> dict:
    return {
        "direction":       direction,
        "bullish_prob":    bullish_prob,
        "confidence":      confidence,
        "price_target_mid":price_target_mid,
    }


def make_squeeze_alert(
    ticker: str = "GME",
    confidence: str = "HIGH",
    triggers: list[str] | None = None,
    short_float_pct: float = 35.0,
    short_ratio: float = 8.5,
    float_shares_M: float = 5.0,
    rvol: float = 4.2,
    has_call_sweeps: bool = True,
    has_dark_pool: bool = False,
    borrow_rate: float = 25.0,
    squeeze_score: int = 78,
) -> dict:
    return {
        "ticker":          ticker,
        "confidence":      confidence,
        "triggers":        triggers or ["High short float: 35.0%", "Low float: 5.0M shares"],
        "short_float_pct": short_float_pct,
        "short_ratio":     short_ratio,
        "float_shares_M":  float_shares_M,
        "rvol":            rvol,
        "has_call_sweeps": has_call_sweeps,
        "has_dark_pool":   has_dark_pool,
        "borrow_rate":     borrow_rate,
        "squeeze_score":   squeeze_score,
    }


def read_db_rows(db_path: Path) -> list[dict]:
    conn   = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows   = conn.execute("SELECT * FROM scan_results").fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ═════════════════════════════════════════════════════════
#  1. _score_bar
# ═════════════════════════════════════════════════════════

class TestScoreBar:

    def test_score_0_all_empty(self):
        bar = _score_bar(0, width=10)
        assert bar == "░" * 10

    def test_score_100_all_filled(self):
        bar = _score_bar(100, width=10)
        assert bar == "█" * 10

    def test_score_50_half_filled(self):
        bar = _score_bar(50, width=10)
        assert bar.count("█") == 5
        assert bar.count("░") == 5

    def test_total_length_equals_width(self):
        for score in [0, 25, 50, 75, 100]:
            assert len(_score_bar(score, width=10)) == 10

    def test_custom_width_respected(self):
        assert len(_score_bar(50, width=20)) == 20

    def test_filled_plus_empty_equals_width(self):
        for score in range(0, 101, 10):
            bar = _score_bar(score, width=10)
            assert bar.count("█") + bar.count("░") == 10

    def test_returns_string(self):
        assert isinstance(_score_bar(42), str)

    def test_score_25_approximately_quarter_filled(self):
        bar = _score_bar(25, width=8)
        # round(25/100 * 8) = round(2.0) = 2 filled
        assert bar.count("█") == 2


# ═════════════════════════════════════════════════════════
#  2. init_db
# ═════════════════════════════════════════════════════════

class TestInitDb:

    def test_creates_db_file(self, patch_db_path):
        init_db()
        assert patch_db_path.exists()

    def test_creates_scan_results_table(self, patch_db_path):
        init_db()
        conn   = sqlite3.connect(patch_db_path)
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        conn.close()
        table_names = [t[0] for t in tables]
        assert "scan_results" in table_names

    def test_creates_ticker_ts_index(self, patch_db_path):
        init_db()
        conn    = sqlite3.connect(patch_db_path)
        indexes = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
        conn.close()
        index_names = [i[0] for i in indexes]
        assert "idx_ticker_ts" in index_names

    def test_idempotent_called_twice(self, patch_db_path):
        init_db()
        init_db()   # second call must not raise
        assert patch_db_path.exists()

    def test_table_has_correct_columns(self, patch_db_path):
        init_db()
        conn = sqlite3.connect(patch_db_path)
        info = conn.execute("PRAGMA table_info(scan_results)").fetchall()
        conn.close()
        col_names = [col[1] for col in info]
        for expected in ("ticker", "scan_ts", "scan_date", "price",
                         "composite_score", "flags", "breakout_direction"):
            assert expected in col_names, f"Column '{expected}' missing"


# ═════════════════════════════════════════════════════════
#  3. save_results
# ═════════════════════════════════════════════════════════

class TestSaveResults:

    def test_empty_list_inserts_zero_rows(self, patch_db_path):
        save_results([])
        rows = read_db_rows(patch_db_path)
        assert rows == []

    def test_single_result_inserted(self, patch_db_path):
        save_results([make_result("AAPL", composite_score=70)])
        rows = read_db_rows(patch_db_path)
        assert len(rows) == 1
        assert rows[0]["ticker"] == "AAPL"

    def test_ticker_stored_correctly(self, patch_db_path):
        save_results([make_result("TSLA")])
        rows = read_db_rows(patch_db_path)
        assert rows[0]["ticker"] == "TSLA"

    def test_composite_score_stored(self, patch_db_path):
        save_results([make_result("AAPL", composite_score=82)])
        rows = read_db_rows(patch_db_path)
        assert rows[0]["composite_score"] == 82

    def test_multiple_results_all_inserted(self, patch_db_path):
        results = [make_result("AAPL"), make_result("TSLA"), make_result("GME")]
        save_results(results)
        rows = read_db_rows(patch_db_path)
        assert len(rows) == 3

    def test_predictions_merged_by_ticker(self, patch_db_path):
        result = make_result("AAPL")
        pred   = {"AAPL": make_prediction(direction="bullish", bullish_prob=78.0)}
        save_results([result], predictions=pred)
        rows = read_db_rows(patch_db_path)
        assert rows[0]["breakout_direction"] == "bullish"
        assert rows[0]["breakout_prob"]       == pytest.approx(78.0)

    def test_missing_prediction_defaults_to_none(self, patch_db_path):
        save_results([make_result("AAPL")], predictions={})
        rows = read_db_rows(patch_db_path)
        assert rows[0]["breakout_direction"] is None
        assert rows[0]["breakout_prob"]       is None

    def test_bb_squeeze_true_stored_as_1(self, patch_db_path):
        save_results([make_result("AAPL", bb_squeeze=True)])
        rows = read_db_rows(patch_db_path)
        assert rows[0]["bb_squeeze"] == 1

    def test_bb_squeeze_false_stored_as_0(self, patch_db_path):
        save_results([make_result("AAPL", bb_squeeze=False)])
        rows = read_db_rows(patch_db_path)
        assert rows[0]["bb_squeeze"] == 0

    def test_squeeze_candidate_stored_as_int(self, patch_db_path):
        save_results([make_result("GME", squeeze_candidate=True)])
        rows = read_db_rows(patch_db_path)
        assert rows[0]["squeeze_candidate"] == 1

    def test_flags_stored_as_json_string(self, patch_db_path):
        flags = ["🔥 EXTREME RVOL", "🩳 SQUEEZE SETUP"]
        save_results([make_result("GME", flags=flags)])
        rows = read_db_rows(patch_db_path)
        assert json.loads(rows[0]["flags"]) == flags

    def test_empty_flags_stored_as_empty_json_array(self, patch_db_path):
        save_results([make_result("AAPL", flags=[])])
        rows = read_db_rows(patch_db_path)
        assert rows[0]["flags"] == "[]"

    def test_scan_date_is_todays_date(self, patch_db_path):
        from datetime import datetime
        save_results([make_result("AAPL")])
        rows    = read_db_rows(patch_db_path)
        today   = datetime.utcnow().strftime("%Y-%m-%d")
        assert rows[0]["scan_date"] == today

    def test_scan_ts_is_recent_epoch(self, patch_db_path):
        before = int(time.time())
        save_results([make_result("AAPL")])
        after = int(time.time())
        rows  = read_db_rows(patch_db_path)
        assert before <= rows[0]["scan_ts"] <= after

    def test_price_stored_as_float(self, patch_db_path):
        save_results([make_result("AAPL", price=174.56)])
        rows = read_db_rows(patch_db_path)
        assert rows[0]["price"] == pytest.approx(174.56)

    def test_none_predictions_treated_as_empty(self, patch_db_path):
        save_results([make_result("AAPL")], predictions=None)
        rows = read_db_rows(patch_db_path)
        assert rows[0]["breakout_direction"] is None


# ═════════════════════════════════════════════════════════
#  4. export_csv
# ═════════════════════════════════════════════════════════

class TestExportCsv:

    def test_empty_results_returns_path_without_writing(self, tmp_path):
        p = tmp_path / "out.csv"
        returned = export_csv([], path=p)
        assert returned == p
        assert not p.exists()

    def test_single_result_creates_csv(self, tmp_path):
        p       = tmp_path / "out.csv"
        results = [make_result("AAPL")]
        export_csv(results, path=p)
        assert p.exists()

    def test_csv_has_header_row(self, tmp_path):
        p = tmp_path / "out.csv"
        export_csv([make_result("AAPL")], path=p)
        with open(p) as f:
            header = f.readline().strip().split(",")
        assert "ticker" in header
        assert "composite_score" in header

    def test_csv_data_row_correct_ticker(self, tmp_path):
        p = tmp_path / "out.csv"
        export_csv([make_result("TSLA")], path=p)
        with open(p) as f:
            reader = csv.DictReader(f)
            rows   = list(reader)
        assert rows[0]["ticker"] == "TSLA"

    def test_multiple_results_all_rows_present(self, tmp_path):
        p = tmp_path / "out.csv"
        export_csv([make_result("AAPL"), make_result("TSLA"), make_result("GME")], path=p)
        with open(p) as f:
            reader = csv.DictReader(f)
            rows   = list(reader)
        assert len(rows) == 3

    def test_headers_match_result_keys(self, tmp_path):
        result = make_result("AAPL")
        p      = tmp_path / "out.csv"
        export_csv([result], path=p)
        with open(p) as f:
            header = f.readline().strip().split(",")
        for key in result:
            assert key in header

    def test_custom_path_used(self, tmp_path):
        custom = tmp_path / "custom_name.csv"
        export_csv([make_result("AAPL")], path=custom)
        assert custom.exists()

    def test_returns_the_path(self, tmp_path):
        p        = tmp_path / "out.csv"
        returned = export_csv([make_result("AAPL")], path=p)
        assert returned == p

    def test_default_path_auto_generated(self, patch_data_path):
        """When path=None, file is created in _paths.data."""
        results  = [make_result("AAPL")]
        returned = export_csv(results, path=None)
        assert returned.parent == patch_data_path
        assert returned.suffix == ".csv"


# ═════════════════════════════════════════════════════════
#  5. send_discord_alert
# ═════════════════════════════════════════════════════════

class TestSendDiscordAlert:

    @pytest.fixture(autouse=True)
    def set_webhook(self):
        with patch.object(alerts_mod._api, "discord_webhook_url",
                          "https://discord.com/api/webhooks/test"):
            yield

    def _send(self, results, predictions=None, title="Test Alert"):
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=204)
            mock_post.return_value.raise_for_status = MagicMock()
            send_discord_alert(results, predictions=predictions, title=title)
            return mock_post

    def test_no_webhook_url_no_http_call(self):
        with patch.object(alerts_mod._api, "discord_webhook_url", ""):
            with patch("requests.post") as mock_post:
                send_discord_alert([make_result("AAPL")])
        mock_post.assert_not_called()

    def test_post_called_once(self):
        mock_post = self._send([make_result("AAPL")])
        mock_post.assert_called_once()

    def test_post_url_is_webhook(self):
        mock_post = self._send([make_result("AAPL")])
        url = mock_post.call_args[0][0]
        assert "discord.com" in url

    def test_username_is_stockscanner(self):
        mock_post = self._send([make_result("AAPL")])
        payload   = mock_post.call_args[1]["json"]
        assert payload["username"] == "StockScanner"

    def test_content_contains_title(self):
        mock_post = self._send([make_result("AAPL")], title="My Alert")
        payload   = mock_post.call_args[1]["json"]
        assert "My Alert" in payload["content"]

    def test_content_contains_result_count(self):
        mock_post = self._send([make_result("AAPL"), make_result("TSLA")])
        payload   = mock_post.call_args[1]["json"]
        assert "2" in payload["content"]

    def test_embeds_capped_at_5(self):
        results   = [make_result(f"T{i}") for i in range(10)]
        mock_post = self._send(results)
        payload   = mock_post.call_args[1]["json"]
        assert len(payload["embeds"]) <= 5

    def test_single_result_one_embed(self):
        mock_post = self._send([make_result("AAPL")])
        payload   = mock_post.call_args[1]["json"]
        assert len(payload["embeds"]) == 1

    def test_embed_color_green_for_score_75_plus(self):
        mock_post = self._send([make_result("AAPL", composite_score=80)])
        payload   = mock_post.call_args[1]["json"]
        assert payload["embeds"][0]["color"] == 0x00ff88

    def test_embed_color_amber_for_score_50_to_74(self):
        mock_post = self._send([make_result("AAPL", composite_score=60)])
        payload   = mock_post.call_args[1]["json"]
        assert payload["embeds"][0]["color"] == 0xffaa00

    def test_embed_color_grey_for_score_below_50(self):
        mock_post = self._send([make_result("AAPL", composite_score=30)])
        payload   = mock_post.call_args[1]["json"]
        assert payload["embeds"][0]["color"] == 0xaaaaaa

    def test_score_bar_in_description(self):
        mock_post = self._send([make_result("AAPL", composite_score=50)])
        payload   = mock_post.call_args[1]["json"]
        desc = payload["embeds"][0]["description"]
        assert "█" in desc or "░" in desc

    def test_bullish_arrow_in_description(self):
        pred      = {"AAPL": make_prediction(direction="bullish")}
        mock_post = self._send([make_result("AAPL")], predictions=pred)
        payload   = mock_post.call_args[1]["json"]
        assert "⬆️" in payload["embeds"][0]["description"]

    def test_bearish_arrow_in_description(self):
        pred      = {"AAPL": make_prediction(direction="bearish")}
        mock_post = self._send([make_result("AAPL")], predictions=pred)
        payload   = mock_post.call_args[1]["json"]
        assert "⬇️" in payload["embeds"][0]["description"]

    def test_neutral_arrow_in_description(self):
        pred      = {"AAPL": make_prediction(direction="neutral")}
        mock_post = self._send([make_result("AAPL")], predictions=pred)
        payload   = mock_post.call_args[1]["json"]
        assert "➡️" in payload["embeds"][0]["description"]

    def test_no_prediction_no_direction_string(self):
        mock_post = self._send([make_result("AAPL")], predictions={})
        payload   = mock_post.call_args[1]["json"]
        desc      = payload["embeds"][0]["description"]
        assert "BULLISH" not in desc and "BEARISH" not in desc

    def test_flags_empty_shows_none(self):
        mock_post = self._send([make_result("AAPL", flags=[])])
        payload   = mock_post.call_args[1]["json"]
        flag_field = next(
            f for f in payload["embeds"][0]["fields"] if "Flags" in f["name"]
        )
        assert flag_field["value"] == "None"

    def test_flags_joined_with_space(self):
        flags     = ["🔥 EXTREME RVOL", "🩳 SQUEEZE SETUP"]
        mock_post = self._send([make_result("AAPL", flags=flags)])
        payload   = mock_post.call_args[1]["json"]
        flag_field = next(
            f for f in payload["embeds"][0]["fields"] if "Flags" in f["name"]
        )
        assert "🔥 EXTREME RVOL" in flag_field["value"]
        assert "🩳 SQUEEZE SETUP" in flag_field["value"]

    def test_http_error_does_not_raise(self):
        with patch.object(alerts_mod._api, "discord_webhook_url",
                          "https://discord.com/api/webhooks/test"):
            with patch("requests.post") as mock_post:
                mock_post.return_value.raise_for_status.side_effect = Exception("HTTP 500")
                # Must not raise
                send_discord_alert([make_result("AAPL")])


# ═════════════════════════════════════════════════════════
#  6. send_squeeze_alert
# ═════════════════════════════════════════════════════════

class TestSendSqueezeAlert:

    @pytest.fixture(autouse=True)
    def set_webhook(self):
        with patch.object(alerts_mod._api, "discord_webhook_url",
                          "https://discord.com/api/webhooks/test"):
            yield

    def _send(self, alert: dict):
        with patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=204)
            send_squeeze_alert(alert)
            return mock_post

    def test_no_webhook_no_http_call(self):
        with patch.object(alerts_mod._api, "discord_webhook_url", ""):
            with patch("requests.post") as mock_post:
                send_squeeze_alert(make_squeeze_alert())
        mock_post.assert_not_called()

    def test_post_called_once(self):
        mock_post = self._send(make_squeeze_alert())
        mock_post.assert_called_once()

    def test_username_is_squeeze_detector(self):
        mock_post = self._send(make_squeeze_alert())
        payload   = mock_post.call_args[1]["json"]
        assert payload["username"] == "SqueezeDetector"

    def test_content_contains_ticker(self):
        mock_post = self._send(make_squeeze_alert(ticker="GME"))
        payload   = mock_post.call_args[1]["json"]
        assert "GME" in payload["content"]

    def test_high_confidence_red_color(self):
        mock_post = self._send(make_squeeze_alert(confidence="HIGH"))
        payload   = mock_post.call_args[1]["json"]
        assert payload["embeds"][0]["color"] == 0xff0000

    def test_medium_confidence_orange_color(self):
        mock_post = self._send(make_squeeze_alert(confidence="MEDIUM"))
        payload   = mock_post.call_args[1]["json"]
        assert payload["embeds"][0]["color"] == 0xff8800

    def test_triggers_in_description(self):
        triggers  = ["High short float: 35%", "Volume spike: 4.2×"]
        mock_post = self._send(make_squeeze_alert(triggers=triggers))
        payload   = mock_post.call_args[1]["json"]
        desc      = payload["embeds"][0]["description"]
        assert "High short float: 35%" in desc
        assert "Volume spike: 4.2×"    in desc

    def test_required_field_names_in_embed(self):
        mock_post   = self._send(make_squeeze_alert())
        payload     = mock_post.call_args[1]["json"]
        field_names = [f["name"] for f in payload["embeds"][0]["fields"]]
        for expected in ("Short Float", "Days to Cover", "Float",
                         "RVOL", "Call Sweeps", "Dark Pool",
                         "Borrow Rate", "Squeeze Score"):
            assert expected in field_names, f"Field '{expected}' missing"

    def test_has_call_sweeps_true_shows_checkmark(self):
        mock_post = self._send(make_squeeze_alert(has_call_sweeps=True))
        payload   = mock_post.call_args[1]["json"]
        fields    = {f["name"]: f["value"] for f in payload["embeds"][0]["fields"]}
        assert "✅" in fields["Call Sweeps"]

    def test_has_call_sweeps_false_shows_cross(self):
        mock_post = self._send(make_squeeze_alert(has_call_sweeps=False))
        payload   = mock_post.call_args[1]["json"]
        fields    = {f["name"]: f["value"] for f in payload["embeds"][0]["fields"]}
        assert "❌" in fields["Call Sweeps"]

    def test_has_dark_pool_false_shows_cross(self):
        mock_post = self._send(make_squeeze_alert(has_dark_pool=False))
        payload   = mock_post.call_args[1]["json"]
        fields    = {f["name"]: f["value"] for f in payload["embeds"][0]["fields"]}
        assert "❌" in fields["Dark Pool"]

    def test_http_error_does_not_raise(self):
        with patch("requests.post", side_effect=Exception("network error")):
            send_squeeze_alert(make_squeeze_alert())   # must not raise
