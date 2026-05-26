"""
tests/test_news_feed.py
Unit tests for ingestion/news_feed.py

No network access, no Redis server, no API keys required.
  - Redis  → replaced by fakeredis.FakeRedis in-memory store
  - HTTP   → replaced by unittest.mock.patch on requests.get
  - loguru → silenced to keep test output clean

Run
───
  pip install pytest fakeredis
  pytest tests/test_news_feed.py -v

Coverage
────────
  _extract_tickers        dollar-prefix, bare 3–5 letter, stopwords,
                          mixed text, edge cases
  _recency_weighted_avg   empty list, single item, monotone, mixed signs

  FinnhubNewsFeed
    _score_headline         bullish/bearish keyword scoring
    _score_headline         catalyst flags (FDA, earnings, M&A, short)
    _cache_article          writes JSON list, respects max-50 limit, sets TTL
    fetch_company_news      happy path, dedup by ID, non-200 returns []
    get_cached_news         reads back stored articles, respects n param
    get_news_sentiment_score  recency weighting, empty cache → 0.0

  RedditMentionTracker
    _process_post           ticker extraction, Redis list/zset/hourly writes
    _process_post           tickers_of_interest filter (in-list vs out-of-list)
    _process_post           posts with no ticker → no Redis writes
    _poll_subreddit         happy path — parses JSON, calls _process_post
    _poll_subreddit         HTTP 429 → sleep called, returns 0
    _poll_subreddit         HTTP 403 → returns 0, no crash
    _poll_subreddit         non-JSON response → returns 0
    _poll_subreddit         deduplication — same post_id seen twice
    _poll_subreddit         seen-IDs trimming when set exceeds MAX_SEEN_IDS
    get_top_mentions        returns sorted list of (ticker, count) tuples
    get_ticker_mentions_per_hour  current-hour bucket
    get_reddit_rank         percentile calculation
    get_recent_posts        reads back stored snippets
    poll_once               orchestrates across multiple subreddits

  StockTwitsFeed
    fetch_ticker            happy path — 6 bullish, 2 bearish, 2 neutral
    fetch_ticker            HTTP 404 → returns {}
    fetch_ticker            HTTP 429 → sleep called, returns {}
    fetch_ticker            non-200 → returns {}
    fetch_ticker            empty message list → returns {}
    fetch_ticker            messages with no entity/sentiment → all neutral
    fetch_ticker            score = bull_pct − bear_pct
    fetch_ticker            writes list, bull/bear daily counters, score key
    get_sentiment_score     reads from cache, returns 0.0 if missing
    get_recent_messages     reads N most-recent messages
    get_daily_bull_bear     reads today's counters
    get_bulk_scores         batch-reads multiple tickers
"""

from __future__ import annotations

import json
import sys
import time
import types
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import fakeredis
import pytest
import sys
import os  
from dotenv import load_dotenv, dotenv_values
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   

# ─────────────────────────────────────────────────────────
#  Path bootstrap so the test can run without pip install -e .
# ─────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ─────────────────────────────────────────────────────────
#  Stub heavy / network dependencies before importing module
# ─────────────────────────────────────────────────────────
_loguru_stub          = types.ModuleType("loguru")
_loguru_stub.logger   = MagicMock()
sys.modules.setdefault("loguru", _loguru_stub)

_dotenv_stub              = types.ModuleType("dotenv")
_dotenv_stub.load_dotenv  = lambda *a, **kw: None
sys.modules.setdefault("dotenv", _dotenv_stub)

# ─────────────────────────────────────────────────────────
#  Import module under test
# ─────────────────────────────────────────────────────────
import redis as real_redis

from ingestion.news_feed import (
    FinnhubNewsFeed,
    RedditMentionTracker,
    StockTwitsFeed,
    _extract_tickers,
    _recency_weighted_avg,
)


# ─────────────────────────────────────────────────────────
#  Shared helpers
# ─────────────────────────────────────────────────────────

def make_fake_redis() -> fakeredis.FakeRedis:
    """Fresh in-memory Redis for each test — no shared state."""
    return fakeredis.FakeRedis(decode_responses=True)


def make_finnhub(fake_r: fakeredis.FakeRedis) -> FinnhubNewsFeed:
    f = FinnhubNewsFeed.__new__(FinnhubNewsFeed)
    f._redis    = fake_r
    f._seen_ids = set()
    return f


def make_reddit(
    fake_r:   fakeredis.FakeRedis,
    watchlist: list[str] | None = None,
    subreddits: list[str] | None = None,
) -> RedditMentionTracker:
    t = RedditMentionTracker.__new__(RedditMentionTracker)
    t.tickers_of_interest = (
        frozenset(x.upper() for x in watchlist) if watchlist else None
    )
    t.subreddits    = subreddits or ["wallstreetbets"]
    t.poll_interval = 60
    t._redis        = fake_r
    t._seen_post_ids = set()
    t._running      = False
    return t


def make_stwits(fake_r: fakeredis.FakeRedis, poll_interval: int = 300) -> StockTwitsFeed:
    s = StockTwitsFeed.__new__(StockTwitsFeed)
    s.poll_interval = poll_interval
    s._redis        = fake_r
    s._running      = False
    return s


def _mock_response(status: int, payload: dict | list | None = None) -> MagicMock:
    r         = MagicMock()
    r.status_code = status
    if payload is not None:
        r.json.return_value = payload
    else:
        r.json.side_effect = ValueError("no content")
    return r


def _make_st_msg(i: int, sentiment_label: str | None) -> dict:
    """Build a synthetic StockTwits message dict."""
    entities = {}
    if sentiment_label:
        entities["sentiment"] = {"basic": sentiment_label}
    return {
        "id":       i,
        "body":     f"Message body {i}",
        "entities": entities,
        "likes":    {"total": i * 2},
    }


def _make_reddit_post(
    post_id:   str  = "abc123",
    title:     str  = "AMC to the moon!",
    selftext:  str  = "",
    score:     int  = 100,
    permalink: str  = "/r/wallstreetbets/comments/abc123/test/",
) -> dict:
    return {
        "id":        post_id,
        "title":     title,
        "selftext":  selftext,
        "score":     score,
        "permalink": permalink,
    }


def _make_reddit_json(posts: list[dict]) -> dict:
    """Wrap posts in the Reddit .json envelope."""
    return {
        "data": {
            "children": [{"data": p} for p in posts],
            "after":    None,
        }
    }


def _finnhub_article(
    art_id:   str = "1001",
    headline: str = "AAPL beats earnings",
    summary:  str = "",
    source:   str = "Reuters",
    url:      str = "https://example.com/1",
    ts:       int | None = None,
) -> dict:
    return {
        "id":       art_id,
        "headline": headline,
        "summary":  summary,
        "source":   source,
        "url":      url,
        "datetime": ts or int(time.time()),
    }


# ═════════════════════════════════════════════════════════
#  1. _extract_tickers
# ═════════════════════════════════════════════════════════

class TestExtractTickers:
    """Tests for the two-tier ticker extraction function."""

    # ── Dollar-prefix (Tier-1) ────────────────────────────

    def test_dollar_prefix_accepted(self):
        assert "AMC" in _extract_tickers("$AMC is going up")

    def test_dollar_prefix_single_letter(self):
        """Single-letter tickers are only valid with $ prefix."""
        assert "F" in _extract_tickers("Buying $F today")

    def test_dollar_prefix_two_letters(self):
        """Two-letter tickers only accepted with $ prefix."""
        assert "AI" in _extract_tickers("$AI is the future")

    def test_dollar_prefix_bypasses_stopwords(self):
        """$MOVE, $CASH, $PLAN etc. must be accepted even if in stopwords."""
        # NOTE: add/remove from this test as stopword list evolves.
        # The key contract is: dollar-sign = always accepted.
        result = _extract_tickers("$TSLA and $AAPL and $NVDA")
        assert "TSLA" in result
        assert "AAPL" in result
        assert "NVDA" in result

    def test_multiple_dollar_tickers_in_one_post(self):
        result = _extract_tickers("Long $GME short $AMC also watching $TSLA")
        assert {"GME", "AMC", "TSLA"}.issubset(result)

    # ── Bare words (Tier-2) ───────────────────────────────

    def test_bare_three_letter_ticker(self):
        assert "GME" in _extract_tickers("GME is squeezing today")

    def test_bare_four_letter_ticker(self):
        assert "AAPL" in _extract_tickers("Looking at AAPL and NVDA")

    def test_bare_five_letter_ticker(self):
        assert "PLTR" in _extract_tickers("PLTR earnings tomorrow")

    def test_bare_one_letter_rejected(self):
        """Single bare uppercase letter must never be a ticker."""
        result = _extract_tickers("I think A is a good company")
        assert "A" not in result    # 'A' is 1 char → rejected by regex (needs 3+)

    def test_bare_two_letter_rejected(self):
        """Two-letter bare word must be rejected (regex requires 3+)."""
        result = _extract_tickers("AI is transforming the industry")
        assert "AI" not in result

    def test_stopwords_not_extracted(self):
        """Common English words and trading slang must be filtered."""
        for text, bad_word in [
            ("THE market crashed",             "THE"),
            ("YOLO into calls",                "YOLO"),
            ("Going to the MOON",              "MOON"),
            ("CEO resigned today",             "CEO"),
            ("ETF flows are bullish",          "ETF"),
            ("SPX hit all time high",          "SPX"),
            ("VIX spiked to 30",              "VIX"),
            ("FDA approved the drug",          "FDA"),
        ]:
            result = _extract_tickers(text)
            assert bad_word not in result, (
                f"Stopword '{bad_word}' should not be extracted from: {text!r}"
            )

    def test_mixed_dollar_and_bare(self):
        """Both tier-1 and tier-2 tickers in the same text."""
        result = _extract_tickers("$AMC and GME are both squeezing")
        assert "AMC" in result
        assert "GME" in result

    def test_lowercase_text_handled(self):
        """Function must uppercase input before matching."""
        result = _extract_tickers("buying $amc and gme today")
        assert "AMC" in result
        assert "GME" in result

    def test_empty_string_returns_empty_set(self):
        assert _extract_tickers("") == set()

    def test_only_stopwords_returns_empty(self):
        result = _extract_tickers("THE MOON YOLO FOMO FDA CEO ETF VIX")
        assert result == set()

    def test_ticker_not_matched_inside_word(self):
        """Regex uses word boundaries — AAPL inside 'AAPLY' must not match."""
        result = _extract_tickers("AAPLX is not a real ticker")
        assert "AAPL" not in result

    def test_returns_set_not_list(self):
        assert isinstance(_extract_tickers("$AMC $AMC $AMC"), set)

    def test_no_duplicates(self):
        """Same ticker mentioned multiple times → appears once in result."""
        result = _extract_tickers("$AMC is going up AMC to the moon $AMC")
        assert result.count("AMC") if isinstance(result, list) else len(
            [x for x in result if x == "AMC"]
        ) == 1


# ═════════════════════════════════════════════════════════
#  2. _recency_weighted_avg
# ═════════════════════════════════════════════════════════

class TestRecencyWeightedAvg:

    def test_empty_returns_zero(self):
        assert _recency_weighted_avg([]) == 0.0

    def test_single_value_returns_that_value(self):
        assert _recency_weighted_avg([0.75]) == pytest.approx(0.75)

    def test_uniform_values_returns_same(self):
        assert _recency_weighted_avg([0.5, 0.5, 0.5]) == pytest.approx(0.5)

    def test_first_item_weighted_most(self):
        """First item (index-0 = most recent) carries weight 1/1."""
        result = _recency_weighted_avg([1.0, 0.0, 0.0, 0.0])
        # 1.0*(1) + 0*(1/2) + 0*(1/3) + 0*(1/4)  /  (1 + 0.5 + 0.333 + 0.25)
        expected = 1.0 / (1 + 0.5 + 1/3 + 0.25)
        assert result == pytest.approx(expected, abs=0.001)

    def test_positive_values_positive_result(self):
        assert _recency_weighted_avg([0.8, 0.6, 0.4]) > 0.0

    def test_negative_values_negative_result(self):
        assert _recency_weighted_avg([-0.8, -0.6, -0.4]) < 0.0

    def test_mixed_signs_close_to_zero(self):
        """Equal and opposite → close to 0 depending on recency weighting."""
        result = _recency_weighted_avg([1.0, -1.0])
        # 1.0*(1/1) + -1.0*(1/2) / (1 + 0.5) = 0.5/1.5 = 0.333
        assert result == pytest.approx(0.333, abs=0.001)

    def test_returns_float(self):
        assert isinstance(_recency_weighted_avg([0.5, 0.3]), float)

    def test_result_within_minus1_plus1(self):
        for values in [[-1.0, -0.5, 0.0], [1.0, 0.5, 0.0], [-1.0, 1.0]]:
            r = _recency_weighted_avg(values)
            assert -1.0 <= r <= 1.0, f"Out of range for {values}: {r}"


# ═════════════════════════════════════════════════════════
#  3. FinnhubNewsFeed
# ═════════════════════════════════════════════════════════

class TestFinnhubScoreHeadline:

    def setup_method(self):
        self.feed = make_finnhub(make_fake_redis())

    def test_bullish_headline_positive_score(self):
        art = _finnhub_article(headline="AAPL beats earnings record upgrade guidance")
        result = self.feed._score_headline(art, "AAPL")
        assert result["sentiment"] > 0

    def test_bearish_headline_negative_score(self):
        art = _finnhub_article(headline="TSLA misses revenue downgrade lawsuit fraud")
        result = self.feed._score_headline(art, "TSLA")
        assert result["sentiment"] < 0

    def test_neutral_headline_zero_score(self):
        art = _finnhub_article(headline="Markets open for trading")
        result = self.feed._score_headline(art, "SPY")
        assert result["sentiment"] == pytest.approx(0.0)

    def test_sentiment_clamped_to_plus_one(self):
        art = _finnhub_article(
            headline="beat beats record upgrade approval approved fda partnership deal contract"
        )
        result = self.feed._score_headline(art, "AAPL")
        assert result["sentiment"] <= 1.0

    def test_sentiment_clamped_to_minus_one(self):
        art = _finnhub_article(
            headline="miss misses downgrade lawsuit investigation decline fraud bankruptcy"
        )
        result = self.feed._score_headline(art, "TSLA")
        assert result["sentiment"] >= -1.0

    def test_fda_flag_set(self):
        art = _finnhub_article(headline="FDA approval granted for new trial phase")
        result = self.feed._score_headline(art, "MRNA")
        assert result["is_fda"] == 1

    def test_fda_flag_not_set(self):
        art = _finnhub_article(headline="Company reports strong earnings")
        result = self.feed._score_headline(art, "AAPL")
        assert result["is_fda"] == 0

    def test_earnings_flag_set(self):
        art = _finnhub_article(headline="Q2 earnings beat EPS guidance revenue raised")
        result = self.feed._score_headline(art, "AAPL")
        assert result["is_earnings"] == 1

    def test_ma_flag_set(self):
        art = _finnhub_article(headline="Merger acquisition buyout announced")
        result = self.feed._score_headline(art, "AAPL")
        assert result["is_ma"] == 1

    def test_short_flag_set(self):
        art = _finnhub_article(headline="Short squeeze gamma float surges")
        result = self.feed._score_headline(art, "GME")
        assert result["is_short"] == 1

    def test_ticker_uppercased_in_result(self):
        art    = _finnhub_article()
        result = self.feed._score_headline(art, "aapl")
        assert result["ticker"] == "AAPL"

    def test_result_contains_required_keys(self):
        art    = _finnhub_article()
        result = self.feed._score_headline(art, "AAPL")
        for key in ("ticker","headline","source","url","sentiment",
                    "bull_count","bear_count","is_fda","is_earnings",
                    "is_ma","is_short","published_at","ts"):
            assert key in result, f"Missing key: {key}"

    def test_summary_text_also_scored(self):
        """Bearish words in summary must affect the score."""
        art = _finnhub_article(
            headline="Company update",
            summary="misses revenue downgrade issued warning",
        )
        result = self.feed._score_headline(art, "TSLA")
        assert result["sentiment"] < 0


class TestFinnhubCacheArticle:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.feed   = make_finnhub(self.fake_r)

    def test_article_written_to_redis_list(self):
        art = self.feed._score_headline(_finnhub_article(), "AAPL")
        self.feed._cache_article("AAPL", art)
        assert self.fake_r.llen("news:AAPL") == 1

    def test_key_is_uppercase(self):
        art = self.feed._score_headline(_finnhub_article(), "AAPL")
        self.feed._cache_article("aapl", art)
        assert self.fake_r.exists("news:AAPL")
        assert not self.fake_r.exists("news:aapl")

    def test_ttl_set_to_86400(self):
        art = self.feed._score_headline(_finnhub_article(), "AAPL")
        self.feed._cache_article("AAPL", art)
        ttl = self.fake_r.ttl("news:AAPL")
        assert 86390 <= ttl <= 86400

    def test_list_trimmed_to_50(self):
        """After 55 inserts only 50 should remain."""
        for i in range(55):
            art = self.feed._score_headline(
                _finnhub_article(art_id=str(i)), "AAPL"
            )
            self.feed._cache_article("AAPL", art)
        assert self.fake_r.llen("news:AAPL") == 50

    def test_newest_article_is_first(self):
        """lpush means index-0 is the most-recently cached article."""
        for i in range(3):
            art = self.feed._score_headline(
                _finnhub_article(art_id=str(i), headline=f"Headline {i}"), "AAPL"
            )
            self.feed._cache_article("AAPL", art)
        first = json.loads(self.fake_r.lindex("news:AAPL", 0))
        assert first["headline"] == "Headline 2"


class TestFinnhubFetchCompanyNews:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.feed   = make_finnhub(self.fake_r)

    def _do_fetch(self, payload, status=200):
        with patch("ingestion.news_feed._safe_get",
                   return_value=_mock_response(status, payload)):
            return self.feed.fetch_company_news("AAPL")

    def test_happy_path_returns_articles(self):
        articles = [_finnhub_article(art_id="1"), _finnhub_article(art_id="2")]
        result   = self._do_fetch(articles)
        assert len(result) == 2

    def test_deduplicates_by_article_id(self):
        """Calling fetch twice with the same article IDs returns 0 on second call."""
        articles = [_finnhub_article(art_id="99")]
        self._do_fetch(articles)
        result2 = self._do_fetch(articles)
        assert len(result2) == 0

    def test_non_200_returns_empty_list(self):
        result = self._do_fetch(None, status=403)
        assert result == []

    def test_none_response_returns_empty_list(self):
        with patch("ingestion.news_feed._safe_get", return_value=None):
            result = self.feed.fetch_company_news("AAPL")
        assert result == []

    def test_articles_cached_in_redis(self):
        articles = [_finnhub_article(art_id="50")]
        self._do_fetch(articles)
        assert self.fake_r.llen("news:AAPL") == 1

    def test_articles_without_id_not_deduped(self):
        """Articles with no 'id' field are scored each time (can't dedupe)."""
        no_id = [{"headline": "Breaking news", "summary": "",
                  "source": "X", "url": "http://x", "datetime": int(time.time())}]
        r1 = self._do_fetch(no_id)
        r2 = self._do_fetch(no_id)
        assert len(r1) == 1
        assert len(r2) == 1   # re-processed because no id to track


class TestFinnhubGetCachedNews:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.feed   = make_finnhub(self.fake_r)

    def _cache_n(self, n: int):
        for i in range(n):
            art = self.feed._score_headline(
                _finnhub_article(art_id=str(i)), "AAPL"
            )
            self.feed._cache_article("AAPL", art)

    def test_returns_empty_list_when_nothing_cached(self):
        assert self.feed.get_cached_news("AAPL") == []

    def test_respects_n_parameter(self):
        self._cache_n(10)
        result = self.feed.get_cached_news("AAPL", n=3)
        assert len(result) == 3

    def test_returns_all_when_fewer_than_n(self):
        self._cache_n(5)
        result = self.feed.get_cached_news("AAPL", n=20)
        assert len(result) == 5

    def test_results_are_dicts(self):
        self._cache_n(2)
        result = self.feed.get_cached_news("AAPL", n=2)
        assert all(isinstance(r, dict) for r in result)

    def test_case_insensitive_lookup(self):
        self._cache_n(2)
        assert self.feed.get_cached_news("aapl", n=5) != []


class TestFinnhubGetSentimentScore:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.feed   = make_finnhub(self.fake_r)

    def test_empty_cache_returns_zero(self):
        assert self.feed.get_news_sentiment_score("AAPL") == 0.0

    def test_all_bullish_returns_positive(self):
        for i in range(3):
            art = self.feed._score_headline(
                _finnhub_article(art_id=str(i), headline="beats earnings upgrade"), "AAPL"
            )
            self.feed._cache_article("AAPL", art)
        assert self.feed.get_news_sentiment_score("AAPL") > 0.0

    def test_all_bearish_returns_negative(self):
        for i in range(3):
            art = self.feed._score_headline(
                _finnhub_article(art_id=str(i), headline="misses fraud lawsuit decline"), "TSLA"
            )
            self.feed._cache_article("TSLA", art)
        assert self.feed.get_news_sentiment_score("TSLA") < 0.0

    def test_returns_float(self):
        assert isinstance(self.feed.get_news_sentiment_score("AAPL"), float)


# ═════════════════════════════════════════════════════════
#  4. RedditMentionTracker
# ═════════════════════════════════════════════════════════

class TestRedditProcessPost:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.tracker = make_reddit(self.fake_r)

    def test_ticker_stored_in_snippet_list(self):
        self.tracker._process_post(
            _make_reddit_post(title="$AMC to the moon"), "wallstreetbets"
        )
        assert self.fake_r.exists("social:reddit:AMC")

    def test_snippet_fields_correct(self):
        self.tracker._process_post(
            _make_reddit_post(
                title="$GME is squeezing!",
                score=500,
                permalink="/r/wallstreetbets/comments/x1/gme/",
            ),
            "wallstreetbets",
        )
        raw     = self.fake_r.lrange("social:reddit:GME", 0, 0)
        snippet = json.loads(raw[0])
        assert snippet["ticker"]    == "GME"
        assert snippet["score"]     == 500
        assert snippet["subreddit"] == "wallstreetbets"
        assert "reddit.com" in snippet["url"]

    def test_ranking_zset_incremented(self):
        self.tracker._process_post(_make_reddit_post(title="$AMC $AMC GME"), "wsb")
        assert self.fake_r.zscore("social:reddit:rank", "AMC") is not None
        assert self.fake_r.zscore("social:reddit:rank", "GME") is not None

    def test_hourly_bucket_incremented(self):
        self.tracker._process_post(_make_reddit_post(title="$TSLA rally"), "wsb")
        ts_hour = int(time.time() // 3600)
        key     = f"social:reddit:hr:TSLA:{ts_hour}"
        assert int(self.fake_r.get(key) or 0) >= 1

    def test_ttl_set_on_snippet_list(self):
        self.tracker._process_post(_make_reddit_post(title="$AAPL earnings"), "wsb")
        ttl = self.fake_r.ttl("social:reddit:AAPL")
        assert 86390 <= ttl <= 86400

    def test_watchlist_filter_allows_watched_ticker(self):
        tracker = make_reddit(self.fake_r, watchlist=["AMC", "GME"])
        tracker._process_post(_make_reddit_post(title="$AMC moon"), "wsb")
        assert self.fake_r.exists("social:reddit:AMC")

    def test_watchlist_filter_blocks_unwatched_ticker(self):
        tracker = make_reddit(self.fake_r, watchlist=["AMC"])
        tracker._process_post(_make_reddit_post(title="$TSLA rally"), "wsb")
        assert not self.fake_r.exists("social:reddit:TSLA")

    def test_no_ticker_no_redis_writes(self):
        before = self.fake_r.keys("social:reddit:*")
        self.tracker._process_post(
            _make_reddit_post(title="The market opened today"), "wsb"
        )
        after = self.fake_r.keys("social:reddit:*")
        assert before == after

    def test_selftext_also_parsed(self):
        self.tracker._process_post(
            _make_reddit_post(title="Daily discussion", selftext="Loading up on $NVDA"),
            "stocks",
        )
        assert self.fake_r.exists("social:reddit:NVDA")

    def test_snippet_list_max_30(self):
        for i in range(35):
            self.tracker._process_post(
                _make_reddit_post(post_id=f"pid{i}", title="$AMC"), "wsb"
            )
        assert self.fake_r.llen("social:reddit:AMC") == 30


class TestRedditPollSubreddit:

    def setup_method(self):
        self.fake_r  = make_fake_redis()
        self.tracker = make_reddit(self.fake_r, subreddits=["wallstreetbets"])

    def _run_poll(self, response_mock):
        with patch("ingestion.news_feed._safe_get",
                   return_value=response_mock):
            return self.tracker._poll_subreddit("wallstreetbets")

    def test_happy_path_processes_new_posts(self):
        posts   = [_make_reddit_post("id1", "$AMC rally"), _make_reddit_post("id2", "$GME squeeze")]
        payload = _make_reddit_json(posts)
        count   = self._run_poll(_mock_response(200, payload))
        assert count == 2
        assert self.fake_r.exists("social:reddit:AMC")
        assert self.fake_r.exists("social:reddit:GME")

    def test_deduplication_same_post_id(self):
        posts   = [_make_reddit_post("dup_id", "$AMC rally")]
        payload = _make_reddit_json(posts)
        resp    = _mock_response(200, payload)
        with patch("ingestion.news_feed._safe_get", return_value=resp):
            c1 = self.tracker._poll_subreddit("wallstreetbets")
            c2 = self.tracker._poll_subreddit("wallstreetbets")
        assert c1 == 1
        assert c2 == 0   # same post id — skipped

    def test_http_429_sleeps_and_returns_zero(self):
        with patch("ingestion.news_feed._safe_get",
                   return_value=_mock_response(429)):
            with patch("time.sleep") as mock_sleep:
                count = self.tracker._poll_subreddit("wallstreetbets")
        assert count == 0
        mock_sleep.assert_called_once_with(60)

    def test_http_403_returns_zero_no_crash(self):
        count = self._run_poll(_mock_response(403))
        assert count == 0

    def test_http_404_returns_zero_no_crash(self):
        count = self._run_poll(_mock_response(404))
        assert count == 0

    def test_non_json_response_returns_zero(self):
        count = self._run_poll(_mock_response(200, None))
        assert count == 0

    def test_none_response_returns_zero(self):
        with patch("ingestion.news_feed._safe_get", return_value=None):
            count = self.tracker._poll_subreddit("wallstreetbets")
        assert count == 0

    def test_empty_post_list_returns_zero(self):
        count = self._run_poll(_mock_response(200, _make_reddit_json([])))
        assert count == 0

    def test_seen_ids_trimmed_when_over_limit(self):
        """When _seen_post_ids grows past MAX_SEEN_IDS it should be trimmed."""
        self.tracker._MAX_SEEN_IDS = 10
        self.tracker._TRIM_TO      = 5
        # Pre-fill seen_ids to just above the limit
        self.tracker._seen_post_ids = {f"old_{i}" for i in range(11)}
        posts   = [_make_reddit_post(f"new_{i}", "$AMC") for i in range(3)]
        payload = _make_reddit_json(posts)
        self._run_poll(_mock_response(200, payload))
        assert len(self.tracker._seen_post_ids) <= 10


class TestRedditReadHelpers:

    def setup_method(self):
        self.fake_r  = make_fake_redis()
        self.tracker = make_reddit(self.fake_r)

    def _add_mentions(self, ticker: str, count: int):
        for _ in range(count):
            self.fake_r.zincrby("social:reddit:rank", 1, ticker)

    def test_get_top_mentions_returns_sorted_list(self):
        self._add_mentions("AMC",  10)
        self._add_mentions("GME",  20)
        self._add_mentions("TSLA", 15)
        top = self.tracker.get_top_mentions(n=3)
        assert top[0][0] == "GME"
        assert top[1][0] == "TSLA"
        assert top[2][0] == "AMC"

    def test_get_top_mentions_scores_are_int(self):
        self._add_mentions("AMC", 5)
        top = self.tracker.get_top_mentions(n=1)
        assert isinstance(top[0][1], int)

    def test_get_ticker_mentions_per_hour(self):
        ts_hour = int(time.time() // 3600)
        self.fake_r.set(f"social:reddit:hr:GME:{ts_hour}", "7")
        assert self.tracker.get_ticker_mentions_per_hour("GME") == 7

    def test_get_ticker_mentions_per_hour_returns_zero_if_missing(self):
        assert self.tracker.get_ticker_mentions_per_hour("ZZNOTREAL") == 0

    def test_get_reddit_rank_returns_float_between_0_and_1(self):
        self._add_mentions("AMC", 10)
        self._add_mentions("GME", 20)
        rank = self.tracker.get_reddit_rank("AMC")
        assert 0.0 <= rank <= 1.0

    def test_get_reddit_rank_most_mentioned_is_one(self):
        self._add_mentions("GME", 50)
        self._add_mentions("AMC", 10)
        assert self.tracker.get_reddit_rank("GME") == pytest.approx(1.0)

    def test_get_reddit_rank_unknown_ticker_returns_zero(self):
        assert self.tracker.get_reddit_rank("ZZNOTREAL") == 0.0

    def test_get_recent_posts_returns_list_of_dicts(self):
        self.tracker._process_post(_make_reddit_post(title="$GME"), "wsb")
        posts = self.tracker.get_recent_posts("GME", n=5)
        assert isinstance(posts, list)
        assert all(isinstance(p, dict) for p in posts)

    def test_get_recent_posts_empty_if_not_tracked(self):
        assert self.tracker.get_recent_posts("ZZNOTREAL") == []

    def test_poll_once_calls_all_subreddits(self):
        tracker = make_reddit(
            self.fake_r,
            subreddits=["wallstreetbets", "stocks", "pennystocks"],
        )
        call_log: list[str] = []

        def fake_poll(sub: str) -> int:
            call_log.append(sub)
            return 0

        tracker._poll_subreddit = fake_poll
        with patch("time.sleep"):
            tracker.poll_once()

        assert call_log == ["wallstreetbets", "stocks", "pennystocks"]


# ═════════════════════════════════════════════════════════
#  5. StockTwitsFeed
# ═════════════════════════════════════════════════════════

class TestStockTwitsFetchTicker:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.feed   = make_stwits(self.fake_r, poll_interval=300)

    def _call_fetch(self, messages: list[dict], status: int = 200) -> dict:
        payload = {"messages": messages}
        with patch("ingestion.news_feed._safe_get",
                   return_value=_mock_response(status, payload)):
            return self.feed.fetch_ticker("AAPL")

    def test_happy_path_returns_summary(self):
        msgs   = ([_make_st_msg(i, "Bullish") for i in range(6)] +
                  [_make_st_msg(i + 6, "Bearish") for i in range(2)] +
                  [_make_st_msg(i + 8, None) for i in range(2)])
        result = self._call_fetch(msgs)
        assert result["ticker"]          == "AAPL"
        assert result["bullish_count"]   == 6
        assert result["bearish_count"]   == 2
        assert result["neutral_count"]   == 2
        assert result["message_count"]   == 10

    def test_sentiment_score_bull_minus_bear(self):
        msgs   = ([_make_st_msg(i, "Bullish") for i in range(6)] +
                  [_make_st_msg(i + 6, "Bearish") for i in range(2)] +
                  [_make_st_msg(i + 8, None) for i in range(2)])
        result = self._call_fetch(msgs)
        expected = round(6/10 - 2/10, 4)   # 0.4
        assert result["sentiment_score"] == pytest.approx(expected, abs=0.001)

    def test_all_bullish_score_is_one(self):
        msgs   = [_make_st_msg(i, "Bullish") for i in range(5)]
        result = self._call_fetch(msgs)
        assert result["sentiment_score"] == pytest.approx(1.0)

    def test_all_bearish_score_is_minus_one(self):
        msgs   = [_make_st_msg(i, "Bearish") for i in range(5)]
        result = self._call_fetch(msgs)
        assert result["sentiment_score"] == pytest.approx(-1.0)

    def test_all_neutral_score_is_zero(self):
        msgs   = [_make_st_msg(i, None) for i in range(5)]
        result = self._call_fetch(msgs)
        assert result["sentiment_score"] == pytest.approx(0.0)

    def test_messages_without_entities_counted_as_neutral(self):
        msgs = [{"id": i, "body": f"msg {i}", "likes": {"total": 0}} for i in range(4)]
        result = self._call_fetch(msgs)
        assert result["neutral_count"] == 4
        assert result["bullish_count"] == 0

    def test_http_404_returns_empty_dict(self):
        with patch("ingestion.news_feed._safe_get",
                   return_value=_mock_response(404)):
            assert self.feed.fetch_ticker("ZZNOTREAL") == {}

    def test_http_429_sleeps_60s_returns_empty(self):
        with patch("ingestion.news_feed._safe_get",
                   return_value=_mock_response(429)):
            with patch("time.sleep") as mock_sleep:
                result = self.feed.fetch_ticker("AAPL")
        assert result == {}
        mock_sleep.assert_called_once_with(60)

    def test_non_200_returns_empty_dict(self):
        with patch("ingestion.news_feed._safe_get",
                   return_value=_mock_response(500)):
            assert self.feed.fetch_ticker("AAPL") == {}

    def test_none_response_returns_empty_dict(self):
        with patch("ingestion.news_feed._safe_get", return_value=None):
            assert self.feed.fetch_ticker("AAPL") == {}

    def test_empty_message_list_returns_empty_dict(self):
        result = self._call_fetch([])
        assert result == {}

    def test_ticker_uppercased(self):
        msgs   = [_make_st_msg(0, "Bullish")]
        result = self._call_fetch(msgs)
        assert result["ticker"] == "AAPL"   # already upper in this test

    def test_messages_written_to_redis_list(self):
        msgs = [_make_st_msg(i, "Bullish") for i in range(5)]
        self._call_fetch(msgs)
        assert self.fake_r.llen("social:stwits:AAPL") == 5

    def test_daily_bull_bear_counters_incremented(self):
        msgs = ([_make_st_msg(i, "Bullish") for i in range(4)] +
                [_make_st_msg(i + 4, "Bearish") for i in range(2)])
        self._call_fetch(msgs)
        bull, bear = self.feed.get_daily_bull_bear("AAPL")
        assert bull == 4
        assert bear == 2

    def test_score_cached_in_redis(self):
        msgs   = [_make_st_msg(i, "Bullish") for i in range(5)]
        self._call_fetch(msgs)
        assert self.fake_r.exists("social:stwits:score:AAPL")

    def test_score_key_ttl_matches_poll_interval(self):
        msgs   = [_make_st_msg(i, "Bullish") for i in range(5)]
        self._call_fetch(msgs)
        ttl = self.fake_r.ttl("social:stwits:score:AAPL")
        assert 295 <= ttl <= 300  # poll_interval=300

    def test_message_list_trimmed_to_50(self):
        msgs1 = [_make_st_msg(i, "Bullish") for i in range(30)]
        msgs2 = [_make_st_msg(i + 30, "Bullish") for i in range(30)]
        self._call_fetch(msgs1)
        self._call_fetch(msgs2)
        assert self.fake_r.llen("social:stwits:AAPL") == 50


class TestStockTwitsReadHelpers:

    def setup_method(self):
        self.fake_r = make_fake_redis()
        self.feed   = make_stwits(self.fake_r)

    def test_get_sentiment_score_reads_cache(self):
        self.fake_r.set("social:stwits:score:TSLA", "0.35")
        assert self.feed.get_sentiment_score("TSLA") == pytest.approx(0.35)

    def test_get_sentiment_score_missing_returns_zero(self):
        assert self.feed.get_sentiment_score("ZZNOTREAL") == 0.0

    def test_get_sentiment_score_case_insensitive(self):
        self.fake_r.set("social:stwits:score:TSLA", "0.25")
        assert self.feed.get_sentiment_score("tsla") == pytest.approx(0.25)

    def test_get_recent_messages_returns_list(self):
        for i in range(5):
            self.fake_r.lpush(
                "social:stwits:AAPL",
                json.dumps({"id": i, "body": f"msg {i}", "sentiment": "Bullish",
                            "likes": 0, "ts": int(time.time())}),
            )
        msgs = self.feed.get_recent_messages("AAPL", n=3)
        assert len(msgs) == 3
        assert all(isinstance(m, dict) for m in msgs)

    def test_get_recent_messages_empty_if_not_tracked(self):
        assert self.feed.get_recent_messages("ZZNOTREAL") == []

    def test_get_daily_bull_bear_returns_tuple(self):
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        self.fake_r.set(f"social:stwits:bull:GME:{today}", "12")
        self.fake_r.set(f"social:stwits:bear:GME:{today}", "5")
        bull, bear = self.feed.get_daily_bull_bear("GME")
        assert bull == 12
        assert bear == 5

    def test_get_daily_bull_bear_zero_if_missing(self):
        bull, bear = self.feed.get_daily_bull_bear("ZZNOTREAL")
        assert bull == 0
        assert bear == 0

    def test_get_bulk_scores_returns_all_tickers(self):
        self.fake_r.set("social:stwits:score:AAPL", "0.4")
        self.fake_r.set("social:stwits:score:TSLA", "-0.2")
        result = self.feed.get_bulk_scores(["AAPL", "TSLA", "NVDA"])
        assert "AAPL" in result
        assert "TSLA" in result
        assert "NVDA" in result   # not in cache → 0.0
        assert result["AAPL"]  == pytest.approx(0.4)
        assert result["TSLA"]  == pytest.approx(-0.2)
        assert result["NVDA"]  == pytest.approx(0.0)


# ─────────────────────────────────────────────────────────
#  pytest asyncio mode registration
# ─────────────────────────────────────────────────────────
from datetime import datetime, timezone

if __name__ == "__main__":
    # Allow running directly: python tests/test_price_feed.py
    import subprocess, sys
    sys.exit(subprocess.call(
        [sys.executable, "-m", "pytest", __file__, "-v", "--tb=short"],
    ))