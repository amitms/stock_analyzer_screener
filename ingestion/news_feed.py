"""
ingestion/news_feed.py
News and social sentiment ingestion.  PRAW-free.

Sources
───────
Finnhub (REST)          Company news, free tier 60 req/min, email signup only.
Reddit JSON endpoint    No auth, no library, no API key. Append ".json" to any
                        Reddit URL and the page data comes back as JSON. Reddit
                        has exposed this since 2008; it survived every API policy
                        change because it is treated as a browser page load, not
                        third-party API access.
StockTwits (REST)       api.stocktwits.com/api/2/streams/symbol/{TICKER}.json
                        Fully public, unauthenticated.  Returns the 30 most recent
                        messages for a ticker, each optionally labelled Bullish or
                        Bearish by the poster.  Rate limit ~200 req/hr per IP.

Ticker extraction rules
───────────────────────
Two-tier matching avoids the endless stopword arms race:

  Tier-1  $TICKER  — dollar-sign prefix → accept 1–5 uppercase letters.
                      "$AI", "$F", "$T" are all valid tickers when dollar-prefixed.
  Tier-2  BARE     — no prefix → require 3–5 uppercase letters AND the word must
                      not be in _STOPWORDS.  This drops "AI", "IT", "OR" etc. while
                      keeping real 3-letter tickers like "GME", "AMC", "AMD".

This two-tier rule eliminates the need for an ever-growing stopword list while
still catching the most important tickers in WSB posts.

Redis schema
────────────
news:{TICKER}              list  scored Finnhub articles    (max 50, 24h TTL)
social:reddit:{TICKER}     list  Reddit post snippets       (max 30, 24h TTL)
social:reddit:rank         zset  ticker → mention count     (1h TTL)
social:reddit:hr:{T}:{H}   str   hourly mention bucket      (2h TTL)
social:stwits:{TICKER}     list  StockTwits messages        (max 50, 24h TTL)
social:stwits:bull:{T}:{D} str   daily bullish count        (24h TTL)
social:stwits:bear:{T}:{D} str   daily bearish count        (24h TTL)
social:stwits:score:{T}    str   latest sentiment score     (poll_interval TTL)
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Optional

import redis
import requests
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
import sys
import os  
		 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   
from config.settings import get_api_config, get_scanner_config

_api = get_api_config()
_cfg = get_scanner_config()


# ─────────────────────────────────────────────────────────
#  Ticker extraction
# ─────────────────────────────────────────────────────────

# Tier-1: $TICKER  (dollar-sign prefix, 1-5 letters)
_DOLLAR_TICKER_RE = re.compile(r"\$([A-Z]{1,5})")

# Tier-2: BARE word (3-5 uppercase letters, word-boundary anchored)
_BARE_TICKER_RE   = re.compile(r"(?<!\w)([A-Z]{3,5})(?!\w)")

# Stopwords for BARE matches only.  Dollar-prefixed tickers are always accepted.
# Kept deliberately narrow: only the highest-frequency false-positives.
_STOPWORDS: frozenset[str] = frozenset({
    # Articles / prepositions / pronouns
    "THE","AND","FOR","ARE","BUT","NOT","YOU","ALL","CAN","HER","WAS","ONE",
    "OUR","HAD","HIM","HIS","WHO","WHY","HOW","ITS","DID","HAS","MAY","TOO",
    "YET","OFF","OWN","OUT","SET","OLD","NOW","NEW","TEN","TWO","SIX","TRY",
    "RAN","RUN","SAY","SAW","LET","GOT","GET","USE","WAY","MAN","MEN","DAY",
    "END","BIG","FEW","FAR","TOP","LOW",
    # Common verbs / adverbs / adjectives
    "VERY","JUST","ONLY","EVEN","BACK","COME","LOOK","LIKE","ALSO","WELL",
    "WILL","REAL","BOTH","MADE","OVER","SUCH","THAN","THEN","THEM","THEY",
    "THIS","THAT","INTO","BEEN","FROM","HAVE","HERE","KNOW","MAKE","NEXT",
    "SOME","TAKE","TIME","WHAT","WITH","YOUR","BEST","GOOD","MOST","MUCH",
    "SAME","SAID","LAST","EACH","WHEN","NEED","PLUS","SOON","WEEK","YEAR",
    "WENT","WERE","USED","MANY","PART","RATE","RISK","TERM","ONCE","OPEN",
    "STAY","STOP","FULL","ABLE","LESS","WAIT","DOES","DONE","TOOK","HIGH",
    "HOLD","SAID","GOES","MEAN","PUTS","CALL","LONG","ALSO","JUST","MORE",
    "MOVE","DROP","RISE","FELL","SELL","SOLD","HOLD","HELD","GIVE","GAVE",
    # Time / days
    "TODAY","WEEK","MONTH","YEAR","MONDAY","TUESDAY","WEDNESDAY","THURSDAY",
    "FRIDAY","WEEKEND","MORNING","EVENING","EARLY","LATER","DAILY","WEEKLY",
    # Connectives / qualifiers
    "COULD","WOULD","SHOULD","MIGHT","THERE","WHERE","WHICH","WHILE","UNDER",
    "SINCE","THESE","THOSE","OTHER","EVERY","STILL","AGAIN","NEVER","OFTEN",
    "AFTER","BEFORE","ABOVE","BELOW","ABOUT","FIRST","SECOND","THIRD","ALSO",
    "THEIR","OURS",
    # Reddit / trading slang
    "YOLO","FOMO","HODL","MOON","BULL","BEAR","SHORT","EDIT","TLDR","FUD",
    "GAIN","LOSS","LMAO","DYOR","MOONING","GOING","GONNA","DOING","BEING",
    # Finance acronyms that aren't ticker symbols
    "CEO","CFO","CTO","COO","SEC","FDA","GDP","IPO","ETF","VIX","SPX",
    "NDX","DJI","DOW","CPI","PPI","FED","ECB","IMF","NYSE","USD","EUR",
    "GBP","JPY","CAD","AUD","CHF","OTC","ATH","EPS","YTD","EOD","EOM",
    "WSB","NFA","ITM","OTM","ATM","DCA",
    # Common words that look like tickers
    "NEWS","DATA","PLAN","DEAL","CASH","DEBT","LOAN","FUND","BANK","FIRM",
    "CORP","UNIT","BOND","COST","RISK","TERM","RATE","SALE","SHOT","DRUG",
    "GENE","CELL","TEST","TRIAL","COURT","JUDGE","JURY","CLAIM","SUIT",
    "CASE","PLAY","IDEA","PICK","VIEW","TYPE","KIND","FORM","EXEC","PART",
    "REPORT","STUDY","STOCK","TRADE","CHART","VALUE","SHARE","PRICE",
    "CRASH","RALLY","SURGE","PUMP","DUMP","DIPS",
})


def _extract_tickers(text: str) -> set[str]:
    """
    Extract probable stock tickers from free text.

    Rules
    ─────
    1. $TICKER  — dollar prefix → accept 1-5 uppercase letters, no stopword check.
    2. BARE     — no prefix → accept 3-5 uppercase letters NOT in _STOPWORDS.

    The two-tier rule avoids maintaining an infinite stopword list:
    - 1-2 letter bare words ("AI", "IT") are never accepted without "$".
    - Common 3-5 letter words ("THE", "PLAN", "CASH") are blocked by stopwords.
    - Real tickers with a "$" prefix always get through regardless of length.
    """
    result: set[str] = set()
    upper  = text.upper()

    # Tier-1: dollar-prefixed (highest confidence)
    for m in _DOLLAR_TICKER_RE.finditer(upper):
        result.add(m.group(1))

    # Tier-2: bare uppercase words (lower confidence, filtered)
    for m in _BARE_TICKER_RE.finditer(upper):
        word = m.group(1)
        if word not in _STOPWORDS:
            result.add(word)

    return result


def _recency_weighted_avg(values: list[float]) -> float:
    """
    Recency-weighted mean.  index-0 = most recent → highest weight (1/1).
    index-1 → weight 1/2, index-2 → weight 1/3, …
    Returns 0.0 for an empty list.
    """
    if not values:
        return 0.0
    total = weight_sum = 0.0
    for i, v in enumerate(values):
        w       = 1.0 / (i + 1)
        total      += v * w
        weight_sum += w
    return round(total / weight_sum, 3)


# ─────────────────────────────────────────────────────────
#  Shared HTTP helpers
# ─────────────────────────────────────────────────────────

# A realistic browser UA is required by Reddit (empty UA → instant 429).
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_REDDIT_HEADERS   = {"User-Agent": "StockScanner/1.0 (personal use, not commercial)"}
_STWITS_HEADERS   = {"User-Agent": _BROWSER_UA, "Accept": "application/json"}
_FINNHUB_HEADERS  = {"User-Agent": "StockScanner/1.0"}


def _safe_get(url: str, *, headers: dict, params: dict | None = None,
              timeout: int = 12) -> requests.Response | None:
    """GET with error swallowing.  Returns None on any network error."""
    try:
        return requests.get(url, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as exc:
        logger.debug(f"HTTP GET failed {url}: {exc}")
        return None


# ─────────────────────────────────────────────────────────
#  Finnhub news feed
# ─────────────────────────────────────────────────────────

class FinnhubNewsFeed:
    """
    Polls Finnhub's company-news endpoint (free tier: 60 req/min).
    Email signup at finnhub.io — no SSN, no brokerage account.

    Deduplicates by article ID so each article is scored once per
    process lifetime.  Cached in Redis as JSON blobs in a list.
    """

    BASE = "https://finnhub.io/api/v1"

    # Keyword lists for rule-based sentiment scoring.
    # FinBERT (ai/squeeze_detector.py) overrides this at runtime when available.
    _BULLISH = frozenset([
        "beat","beats","record","upgrade","approval","approved","fda",
        "partnership","deal","contract","breakout","surge","strong",
        "positive","raises","guidance","buyback","revenue","growth",
    ])
    _BEARISH = frozenset([
        "miss","misses","downgrade","lawsuit","investigation","decline",
        "warning","loss","cut","resign","fraud","recall","delay",
        "disapproved","rejected","bankruptcy","probe","charges",
    ])

    def __init__(self):
        self._redis = redis.Redis(
            host=_api.redis_host, port=_api.redis_port,
            db=_api.redis_db, decode_responses=True,
        )
        self._seen_ids: set[str] = set()

    # ── Public ────────────────────────────────────────────

    def fetch_company_news(self, ticker: str, lookback_hours: int = 24) -> list[dict]:
        """
        Fetch today's news for `ticker`, score each article, cache new ones.
        Returns only articles not seen before in this process session.
        """
        today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        params = {"symbol": ticker.upper(), "from": today,
                  "to": today, "token": _api.finnhub_api_key}
        resp   = _safe_get(f"{self.BASE}/company-news",
                           headers=_FINNHUB_HEADERS, params=params)
        if resp is None or resp.status_code != 200:
            if resp:
                logger.warning(f"Finnhub news [{ticker}]: HTTP {resp.status_code}")
            return []

        try:
            articles = resp.json()
        except ValueError:
            return []

        new_articles: list[dict] = []
        for art in articles:
            art_id = str(art.get("id", ""))
            if art_id and art_id in self._seen_ids:
                continue
            if art_id:
                self._seen_ids.add(art_id)
            scored = self._score_headline(art, ticker)
            new_articles.append(scored)
            self._cache_article(ticker, scored)

        return new_articles

    def fetch_general_market_news(self, category: str = "general") -> list[dict]:
        """Broad market news — macro, M&A, FDA catalysts."""
        resp = _safe_get(f"{self.BASE}/news", headers=_FINNHUB_HEADERS,
                         params={"category": category, "token": _api.finnhub_api_key})
        if resp is None or resp.status_code != 200:
            return []
        try:
            return resp.json()[:20]
        except ValueError:
            return []

    def get_cached_news(self, ticker: str, n: int = 10) -> list[dict]:
        """Return the N most-recent cached articles for a ticker."""
        raw = self._redis.lrange(f"news:{ticker.upper()}", 0, n - 1)
        out: list[dict] = []
        for r in raw:
            try:
                out.append(json.loads(r))
            except ValueError:
                pass
        return out

    def get_news_sentiment_score(self, ticker: str) -> float:
        """
        Recency-weighted average sentiment across cached articles.
        Returns a float in [-1.0, +1.0].  0.0 if nothing cached.
        """
        articles   = self.get_cached_news(ticker, n=20)
        sentiments = [a.get("sentiment", 0.0) for a in articles]
        return _recency_weighted_avg(sentiments)

    # ── Internal ──────────────────────────────────────────

    def _score_headline(self, article: dict, ticker: str) -> dict:
        """Keyword-based sentiment scoring.  Range: -1.0 to +1.0."""
        text = (
            article.get("headline", "") + " " + article.get("summary", "")
        ).lower()

        bull_n = sum(1 for w in self._BULLISH if w in text)
        bear_n = sum(1 for w in self._BEARISH if w in text)
        raw    = bull_n - bear_n
        mag    = max(abs(raw), 1)
        score  = max(-1.0, min(1.0, raw / mag))

        return {
            "ticker":       ticker.upper(),
            "headline":     article.get("headline", ""),
            "source":       article.get("source", ""),
            "url":          article.get("url", ""),
            "sentiment":    round(score, 3),
            "bull_count":   bull_n,
            "bear_count":   bear_n,
            "is_fda":       int(any(w in text for w in
                                    ("fda","approval","clinical","trial","phase"))),
            "is_earnings":  int(any(w in text for w in
                                    ("earnings","eps","revenue","quarter","guidance"))),
            "is_ma":        int(any(w in text for w in
                                    ("merger","acquisition","buyout","takeover"))),
            "is_short":     int(any(w in text for w in
                                    ("short","squeeze","borrow","gamma","float"))),
            "published_at": article.get("datetime", int(time.time())),
            "ts":           int(time.time()),
        }

    def _cache_article(self, ticker: str, article: dict) -> None:
        key = f"news:{ticker.upper()}"
        self._redis.lpush(key, json.dumps(article))
        self._redis.ltrim(key, 0, 49)   # keep 50 most recent
        self._redis.expire(key, 86400)


# ─────────────────────────────────────────────────────────
#  Reddit mention tracker  (JSON endpoint — no PRAW, no auth)
# ─────────────────────────────────────────────────────────

class RedditMentionTracker:
    """
    Polls Reddit subreddits via the public .json endpoint.

    How the JSON endpoint works
    ───────────────────────────
    Appending ".json" to any public Reddit URL returns the page data
    as a JSON document.  Example:

        GET https://www.reddit.com/r/wallstreetbets/new.json?limit=100

    Reddit has served this since 2008.  It is not part of the paid API —
    it is the same data that a logged-out browser would receive.  A
    non-empty User-Agent string is required (empty UA → HTTP 429).

    What we extract
    ───────────────
    Post titles and selftext from the "new" sort.  This captures any
    post within 60 seconds of publication with a single HTTP request
    per subreddit.  Comment scanning is not done — it would require
    one request per post thread and is too expensive at scale.

    Ticker extraction
    ─────────────────
    Titles are processed with _extract_tickers() (two-tier rule).
    Mentions are stored in Redis sorted sets for ranking.

    Rate limit
    ──────────
    Reddit allows ~30 unauthenticated requests/minute per IP.
    With a 60-second poll_interval across 6 subreddits we use ~6 req/min.
    A mandatory 1-second sleep between subreddits is enforced.
    Back-off on HTTP 429 is automatic (60-second sleep).
    """

    _BASE_URL       = "https://www.reddit.com/r/{sub}/new.json"
    _DEFAULT_SUBS   = [
        "wallstreetbets", "stocks", "pennystocks",
        "investing",      "shortsqueeze",
    ]
    _MAX_SEEN_IDS   = 10_000    # prevent unbounded set growth
    _TRIM_TO        = 5_000

    def __init__(
        self,
        tickers_of_interest: Optional[list[str]] = None,
        subreddits:          Optional[list[str]] = None,
        poll_interval:       int = 60,
    ):
        self.tickers_of_interest = (
            frozenset(t.upper() for t in tickers_of_interest)
            if tickers_of_interest else None
        )
        self.subreddits    = subreddits or self._DEFAULT_SUBS
        self.poll_interval = poll_interval
        self._redis        = redis.Redis(
            host=_api.redis_host, port=_api.redis_port,
            db=_api.redis_db, decode_responses=True,
        )
        self._seen_post_ids: set[str] = set()
        self._running: bool           = False

    # ── Lifecycle ─────────────────────────────────────────

    def run_poll_loop(self) -> None:
        """
        Blocking polling loop.  Run in a background thread:

            import threading
            t = threading.Thread(target=tracker.run_poll_loop, daemon=True)
            t.start()

        Or from asyncio:

            loop.run_in_executor(None, tracker.run_poll_loop)
        """
        self._running = True
        logger.info(
            f"Reddit JSON poller started | subs={self.subreddits} "
            f"interval={self.poll_interval}s"
        )
        while self._running:
            for sub in self.subreddits:
                if not self._running:
                    break
                self._poll_subreddit(sub)
                time.sleep(1.0)          # mandatory inter-subreddit pause
            time.sleep(self.poll_interval)

    def stop(self) -> None:
        self._running = False

    def poll_once(self) -> int:
        """
        Single synchronous pass across all subreddits.
        Returns the total number of new posts processed.
        Useful for testing and manual refresh.
        """
        total = 0
        for sub in self.subreddits:
            total += self._poll_subreddit(sub)
            time.sleep(1.0)
        return total

    # ── Polling ───────────────────────────────────────────

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        reraise=False,
    )
    def _poll_subreddit(self, subreddit: str) -> int:
        """
        Fetch the newest 100 posts from one subreddit.
        Returns the count of newly-seen posts processed.
        """
        url  = self._BASE_URL.format(sub=subreddit)
        resp = _safe_get(url, headers=_REDDIT_HEADERS,
                         params={"limit": 100, "raw_json": 1})
        if resp is None:
            return 0

        if resp.status_code == 429:
            logger.warning(f"Reddit 429 on r/{subreddit} — backing off 60s")
            time.sleep(60)
            return 0
        if resp.status_code in (403, 404):
            logger.warning(f"Reddit r/{subreddit}: HTTP {resp.status_code} "
                           "(private or banned subreddit?)")
            return 0
        if resp.status_code != 200:
            logger.debug(f"Reddit r/{subreddit}: unexpected HTTP {resp.status_code}")
            return 0

        try:
            data = resp.json()
        except ValueError:
            logger.debug(f"Reddit r/{subreddit}: non-JSON response")
            return 0

        posts     = data.get("data", {}).get("children", [])
        processed = 0
        for wrapper in posts:
            post = wrapper.get("data", {})
            pid  = post.get("id", "")
            if pid in self._seen_post_ids:
                continue
            self._seen_post_ids.add(pid)
            self._process_post(post, subreddit)
            processed += 1

        # Prevent unbounded growth of the seen-IDs set
        if len(self._seen_post_ids) > self._MAX_SEEN_IDS:
            self._seen_post_ids = set(
                list(self._seen_post_ids)[-self._TRIM_TO:]
            )

        if processed:
            logger.debug(f"Reddit r/{subreddit}: {processed} new posts")
        return processed

    def _process_post(self, post: dict, subreddit: str) -> None:
        """Extract tickers from a post and update Redis counters."""
        title    = post.get("title",    "")
        selftext = post.get("selftext", "")
        text     = f"{title} {selftext}"
        tickers  = _extract_tickers(text)

        if not tickers:
            return

        ts_hour = int(time.time() // 3600)
        score   = int(post.get("score", 0) or 0)
        url     = f"https://reddit.com{post.get('permalink', '')}"

        for ticker in tickers:
            if (self.tickers_of_interest is not None
                    and ticker not in self.tickers_of_interest):
                continue

            # Hourly bucket  (for velocity / rate-of-change)
            hour_key = f"social:reddit:hr:{ticker}:{ts_hour}"
            self._redis.incr(hour_key)
            self._redis.expire(hour_key, 7200)  # keep for 2 hours

            # Global ranking sorted set  (decays on 1h TTL refresh)
            self._redis.zincrby("social:reddit:rank", 1, ticker)
            self._redis.expire("social:reddit:rank", 3600)

            # Post snippet for display / debugging
            snippet = {
                "ticker":    ticker,
                "title":     title[:200],
                "score":     score,
                "subreddit": subreddit,
                "url":       url,
                "ts":        int(time.time()),
            }
            list_key = f"social:reddit:{ticker}"
            self._redis.lpush(list_key, json.dumps(snippet))
            self._redis.ltrim(list_key, 0, 29)
            self._redis.expire(list_key, 86400)

    # ── Read helpers ──────────────────────────────────────

    def get_top_mentions(self, n: int = 20) -> list[tuple[str, int]]:
        """Top-N tickers by total mention count in the current hour."""
        raw = self._redis.zrevrange("social:reddit:rank", 0, n - 1, withscores=True)
        return [(ticker, int(score)) for ticker, score in raw]

    def get_ticker_mentions_per_hour(self, ticker: str) -> int:
        """Mention count for `ticker` in the current hour bucket."""
        ts_hour = int(time.time() // 3600)
        val     = self._redis.get(f"social:reddit:hr:{ticker.upper()}:{ts_hour}")
        return int(val) if val else 0

    def get_reddit_rank(self, ticker: str) -> float:
        """
        Percentile rank of `ticker` among all seen tickers (0–1).
        0.0 → not mentioned, 1.0 → most-mentioned.
        """
        t     = ticker.upper()
        score = self._redis.zscore("social:reddit:rank", t)
        if score is None:
            return 0.0
        total = self._redis.zcard("social:reddit:rank")
        rank  = self._redis.zrank("social:reddit:rank", t)
        if total is None or rank is None or total == 0:
            return 0.0
        return round((rank + 1) / total, 3)

    def get_recent_posts(self, ticker: str, n: int = 10) -> list[dict]:
        """Most-recent Reddit post snippets mentioning `ticker`."""
        raw = self._redis.lrange(f"social:reddit:{ticker.upper()}", 0, n - 1)
        out: list[dict] = []
        for r in raw:
            try:
                out.append(json.loads(r))
            except ValueError:
                pass
        return out


# ─────────────────────────────────────────────────────────
#  StockTwits feed  (public unauthenticated API)
# ─────────────────────────────────────────────────────────

class StockTwitsFeed:
    """
    Polls the StockTwits public symbol stream.

    Endpoint
    ────────
    GET https://api.stocktwits.com/api/2/streams/symbol/{TICKER}.json

    No API key, no signup, no auth.  Returns the 30 most recent messages
    for a ticker, each optionally tagged Bullish or Bearish by the poster.
    This gives us a crowd-sourced, stock-specific sentiment signal — far
    more relevant than generic text NLP because the labelling is done by
    traders who are actively discussing that ticker.

    Sentiment score
    ───────────────
    score = bull_pct - bear_pct   (range -1.0 to +1.0)

    Where bull_pct = bullish_messages / total_messages.
    Unlabelled messages are counted as neutral (neither bull nor bear).

    Rate limit
    ──────────
    ~200 requests/hour per IP.  With a 5-minute poll and 20 tickers
    we consume 4 req/min — well within limits.  A 2-second sleep between
    tickers is enforced.
    """

    _BASE_URL = "https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"

    def __init__(self, poll_interval: int = 300):
        self.poll_interval = poll_interval
        self._redis        = redis.Redis(
            host=_api.redis_host, port=_api.redis_port,
            db=_api.redis_db, decode_responses=True,
        )
        self._running: bool = False

    # ── Lifecycle ─────────────────────────────────────────

    def run_poll_loop(self, tickers: list[str]) -> None:
        """
        Blocking poll loop across all tickers.  Run in a background thread.

            import threading
            t = threading.Thread(
                target=feed.run_poll_loop,
                args=(["AAPL", "TSLA", "AMC"],),
                daemon=True,
            )
            t.start()
        """
        self._running = True
        logger.info(f"StockTwits poller started | {len(tickers)} tickers")
        while self._running:
            for ticker in tickers:
                if not self._running:
                    break
                self.fetch_ticker(ticker)
                time.sleep(2.0)
            time.sleep(self.poll_interval)

    def stop(self) -> None:
        self._running = False

    # ── Fetch ─────────────────────────────────────────────

    @retry(
        retry=retry_if_exception_type(requests.RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=3, max=20),
        reraise=False,
    )
    def fetch_ticker(self, ticker: str) -> dict:
        """
        Fetch the latest StockTwits messages for one ticker.
        Writes results to Redis and returns a summary dict.

        Return shape
        ────────────
        {
          "ticker":          "AAPL",
          "message_count":   30,
          "bullish_count":   18,
          "bearish_count":   6,
          "neutral_count":   6,
          "bull_pct":        0.60,
          "bear_pct":        0.20,
          "sentiment_score": 0.40,
          "ts":              <epoch>
        }
        Returns {} on any error.
        """
        ticker = ticker.upper()
        url    = self._BASE_URL.format(ticker=ticker)
        resp   = _safe_get(url, headers=_STWITS_HEADERS)

        if resp is None:
            return {}
        if resp.status_code == 404:
            logger.debug(f"StockTwits: {ticker} not found")
            return {}
        if resp.status_code == 429:
            logger.warning("StockTwits rate-limited — sleeping 60s")
            time.sleep(60)
            return {}
        if resp.status_code != 200:
            logger.debug(f"StockTwits [{ticker}]: HTTP {resp.status_code}")
            return {}

        try:
            data = resp.json()
        except ValueError:
            return {}

        messages = data.get("messages", [])
        if not messages:
            return {}

        bull_n = bear_n = neut_n = 0
        for msg in messages:
            label = (
                (msg.get("entities") or {})
                .get("sentiment", {})
                .get("basic", "")
                .lower()
            )
            if label == "bullish":
                bull_n += 1
            elif label == "bearish":
                bear_n += 1
            else:
                neut_n += 1

        total    = len(messages)
        bull_pct = round(bull_n / total, 4) if total else 0.0
        bear_pct = round(bear_n / total, 4) if total else 0.0
        score    = round(bull_pct - bear_pct, 4)

        summary = {
            "ticker":          ticker,
            "message_count":   total,
            "bullish_count":   bull_n,
            "bearish_count":   bear_n,
            "neutral_count":   neut_n,
            "bull_pct":        bull_pct,
            "bear_pct":        bear_pct,
            "sentiment_score": score,
            "ts":              int(time.time()),
        }

        # ── Persist to Redis ──────────────────────────────
        list_key = f"social:stwits:{ticker}"
        for msg in messages:
            label = (
                (msg.get("entities") or {})
                .get("sentiment", {})
                .get("basic", "neutral")
            ) or "neutral"
            record = {
                "id":        msg.get("id"),
                "body":      (msg.get("body") or "")[:280],
                "sentiment": label,
                "likes":     (msg.get("likes") or {}).get("total", 0),
                "ts":        int(time.time()),
            }
            self._redis.lpush(list_key, json.dumps(record))
        self._redis.ltrim(list_key, 0, 49)
        self._redis.expire(list_key, 86400)

        today_key = datetime.now(timezone.utc).strftime("%Y%m%d")
        self._redis.incr(f"social:stwits:bull:{ticker}:{today_key}", bull_n)
        self._redis.incr(f"social:stwits:bear:{ticker}:{today_key}", bear_n)
        self._redis.expire(f"social:stwits:bull:{ticker}:{today_key}", 86400)
        self._redis.expire(f"social:stwits:bear:{ticker}:{today_key}", 86400)

        # Short-TTL score for real-time composite signal consumption
        self._redis.setex(
            f"social:stwits:score:{ticker}",
            self.poll_interval,
            str(score),
        )

        logger.debug(
            f"StockTwits {ticker}: "
            f"bull={bull_n} bear={bear_n} neut={neut_n} score={score:+.3f}"
        )
        return summary

    # ── Read helpers ──────────────────────────────────────

    def get_sentiment_score(self, ticker: str) -> float:
        """Latest cached sentiment score (-1 to +1).  0.0 if stale/missing."""
        raw = self._redis.get(f"social:stwits:score:{ticker.upper()}")
        return float(raw) if raw else 0.0

    def get_recent_messages(self, ticker: str, n: int = 20) -> list[dict]:
        """Most-recent cached StockTwits messages for a ticker."""
        raw = self._redis.lrange(f"social:stwits:{ticker.upper()}", 0, n - 1)
        out: list[dict] = []
        for r in raw:
            try:
                out.append(json.loads(r))
            except ValueError:
                pass
        return out

    def get_daily_bull_bear(self, ticker: str) -> tuple[int, int]:
        """Today's cumulative (bullish_count, bearish_count)."""
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        t     = ticker.upper()
        bull  = self._redis.get(f"social:stwits:bull:{t}:{today}") or "0"
        bear  = self._redis.get(f"social:stwits:bear:{t}:{today}") or "0"
        return int(bull), int(bear)

    def get_bulk_scores(self, tickers: list[str]) -> dict[str, float]:
        """Batch-read latest sentiment scores for multiple tickers from cache."""
        return {t.upper(): self.get_sentiment_score(t) for t in tickers}
