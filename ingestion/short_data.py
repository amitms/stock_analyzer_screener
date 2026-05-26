"""
ingestion/short_data.py
Short interest, float, and borrow-rate data — Fintel-free rewrite.

Why Fintel was removed
──────────────────────
Fintel's short-interest API requires a paid subscription.
There is no meaningful free tier.

Free replacement sources  (in priority order within each field)
───────────────────────────────────────────────────────────────
Field                   Primary source          Fallback
─────────────────────── ──────────────────────  ────────────────────────
sharesShort             yfinance .info          —
floatShares             yfinance .info          —
sharesOutstanding       yfinance .info          —
shortRatio (DTC)        yfinance .info          calculated from above
short_float_pct         yfinance .info          calculated from above
heldPercentInstitutions yfinance .info          —
heldPercentInsiders     yfinance .info          —
beta                    yfinance .info          —
marketCap / price       yfinance .info          yfinance fast_info
shortVolRatio           FINRA RegSHO daily file —  (OTC short vol / total vol)
borrow_rate_pct         Finnhub /stock/metric   0.0 if no key configured

Source details
──────────────
yfinance .info
  Yahoo Finance's summary endpoint.  Free, no signup, no API key.
  Returns sharesShort, floatShares, shortRatio (days-to-cover),
  sharesShortPriorMonth, and all ownership percentages.
  Short interest data lags the exchange settlement by ~2 weeks
  (FINRA settlement cycle publishes twice a month).
  Rate limit: ~2 req/sec; cache aggressively.

FINRA RegSHO daily short-sale volume  (regsho.finra.org)
  Publicly downloadable pipe-delimited text files, one per trading day.
  URL pattern: https://cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt
  Columns: Date | Symbol | ShortVolume | ShortExemptVolume | TotalVolume | Market
  "CNMS" is the consolidated report (all venues combined — preferred).
  Published each trading day after market close (~6:30 PM ET).
  Use case here: derive short_vol_ratio = ShortVolume / TotalVolume as a
  DAILY proxy for short selling pressure.  This is NOT short interest —
  it is the fraction of today's volume that was a short sale.
  Typical range: 0.30–0.60 (30–60% of OTC volume is usually short).

Finnhub /stock/metric
  Free tier provides beta, 52-week high/low, and shortInterestRatio.
  Does NOT provide a borrow rate — borrow_rate_pct remains 0.0 unless
  another paid source is added.  The field is kept in ShortProfile for
  forward-compatibility.

Caching
───────
Redis TTL = 4 hours (14 400 s).  Short interest data changes at most
twice per month (FINRA settlement), so 4-hour staleness is fine for
scanning purposes.  RegSHO data is fetched once per day.
"""

from __future__ import annotations

import io
import time
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import redis
import requests
import yfinance as yf
from loguru import logger
import sys
import os  
		 
from config.settings import get_api_config

_api = get_api_config()

# ─────────────────────────────────────────────────────────
#  FINRA RegSHO URL templates
# ─────────────────────────────────────────────────────────

# Consolidated short volume file (all FINRA trade reporting venues combined).
# Date format: YYYYMMDD  e.g. CNMSshvol20250421.txt
_REGSHO_URL = (
    "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt"
)
_REGSHO_HEADERS = {
    "User-Agent": "StockScanner/1.0 (research use)"
}

# ─────────────────────────────────────────────────────────
#  Data class
# ─────────────────────────────────────────────────────────

@dataclass
class ShortProfile:
    """
    Complete short-interest profile for a single ticker.
    All monetary amounts in USD, all percentages as decimals (0.20 = 20 %).
    """
    ticker:                 str
    # ── Share structure ───────────────────────────────────
    float_shares:           float = 0.0   # public float (shares)
    shares_outstanding:     float = 0.0   # total issued shares
    # ── Short interest ────────────────────────────────────
    short_interest:         float = 0.0   # shares sold short (bi-monthly FINRA)
    short_float_pct:        float = 0.0   # short_interest / float_shares
    short_ratio:            float = 0.0   # days-to-cover
    short_vol_ratio:        float = 0.0   # FINRA daily short vol / total vol
    shares_short_prior:     float = 0.0   # previous settlement period
    # ── Borrow ────────────────────────────────────────────
    borrow_rate_pct:        float = 0.0   # annualised cost-to-borrow (%)
    # ── Ownership ─────────────────────────────────────────
    inst_ownership_pct:     float = 0.0   # institutional holders %
    insider_ownership_pct:  float = 0.0   # insider holders %
    # ── Market data ───────────────────────────────────────
    market_cap:             float = 0.0
    price:                  float = 0.0
    beta:                   float = 1.0
    avg_volume_10d:         float = 0.0
    # ── Metadata ──────────────────────────────────────────
    data_source:            str   = "yfinance"
    updated_at:             int   = field(default_factory=lambda: int(time.time()))

    # ── Derived helpers ───────────────────────────────────

    def is_squeeze_candidate(
        self,
        min_short_float:    float = 0.20,
        max_float_shares:   float = 20_000_000,
        min_days_to_cover:  float = 5.0,
    ) -> bool:
        """
        True when the stock meets the classic squeeze setup criteria:
          - High short float (bears are trapped in size)
          - Low float (few shares available → small volume can move price)
          - High days-to-cover (shorts take many days to exit)
        """
        return (
            self.short_float_pct  >= min_short_float
            and self.float_shares <= max_float_shares
            and self.short_ratio  >= min_days_to_cover
        )

    def squeeze_score(self) -> float:
        """
        Simple 0–1 squeeze readiness score derived only from free data.
        Higher = more squeeze pressure.
        """
        si_component    = min(self.short_float_pct / 0.50, 1.0)         # cap at 50%
        float_component = max(0.0, 1.0 - self.float_shares / 20_000_000)
        dtc_component   = min(self.short_ratio / 20.0, 1.0)
        svr_component   = max(0.0, (self.short_vol_ratio - 0.40) / 0.40) # > 40% is elevated
        return round(
            0.40 * si_component +
            0.30 * float_component +
            0.20 * dtc_component +
            0.10 * svr_component,
            3,
        )

    def to_redis_mapping(self) -> dict[str, str]:
        return {k: str(v) for k, v in asdict(self).items()}

    @classmethod
    def from_redis(cls, raw: dict) -> "ShortProfile":
        _FLOAT_FIELDS = {
            "float_shares", "shares_outstanding", "short_interest",
            "short_float_pct", "short_ratio", "short_vol_ratio",
            "shares_short_prior", "borrow_rate_pct", "inst_ownership_pct",
            "insider_ownership_pct", "market_cap", "price", "beta",
            "avg_volume_10d",
        }
        _INT_FIELDS = {"updated_at"}
        kwargs: dict = {}
        for k, v in raw.items():
            if k not in cls.__dataclass_fields__:
                continue
            if k in _FLOAT_FIELDS:
                kwargs[k] = float(v)
            elif k in _INT_FIELDS:
                kwargs[k] = int(float(v))
            else:
                kwargs[k] = v
        if "ticker" not in kwargs:
            kwargs["ticker"] = raw.get("ticker", "")
        return cls(**kwargs)


# ─────────────────────────────────────────────────────────
#  FINRA RegSHO helper
# ─────────────────────────────────────────────────────────

def _regsho_date_str(offset_days: int = 0) -> str:
    """Return YYYYMMDD for today minus offset_days, skipping weekends."""
    d = date.today() - timedelta(days=offset_days)
    # If landing on a weekend, step back to Friday
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


def fetch_regsho_short_vol_ratio(ticker: str) -> float:
    """
    Fetch today's (or most-recent) FINRA RegSHO consolidated short-volume
    file and return the short-volume ratio for `ticker`.

    short_vol_ratio = ShortVolume / TotalVolume

    Typical values:
      0.30–0.45  normal
      0.45–0.60  elevated short selling
      > 0.60     heavy short selling (potential bearish signal or squeeze fuel)

    Returns 0.0 if the file is unavailable (weekend, holiday, fetch error)
    or the ticker is not present in today's file.

    Note: This is off-exchange short volume only (FINRA TRF / ADF / ORF).
    On-exchange short volume is not included.  The number is a directional
    indicator, not the total market short volume.
    """
    ticker = ticker.upper()

    # Try today first, then fall back up to 3 prior trading days
    for offset in range(4):
        date_str = _regsho_date_str(offset)
        url = _REGSHO_URL.format(date=date_str)
        try:
            resp = requests.get(url, headers=_REGSHO_HEADERS, timeout=15)
            if resp.status_code == 404:
                logger.debug(f"RegSHO: no file for {date_str}, trying earlier date")
                continue
            if resp.status_code != 200:
                logger.debug(f"RegSHO: HTTP {resp.status_code} for {date_str}")
                continue

            # File is pipe-delimited: Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
            df = pd.read_csv(
                io.StringIO(resp.text),
                sep="|",
                dtype=str,
                on_bad_lines="skip",
            )

            # Normalise column names (header varies slightly by date)
            df.columns = [c.strip().lower() for c in df.columns]

            symbol_col = next(
                (c for c in df.columns if "symbol" in c), None
            )
            short_col = next(
                (c for c in df.columns if "shortvol" in c and "exempt" not in c), None
            )
            total_col = next(
                (c for c in df.columns if "totalvol" in c or "totalvolume" in c), None
            )

            if not all([symbol_col, short_col, total_col]):
                logger.debug(f"RegSHO: unexpected columns: {list(df.columns)}")
                continue

            row = df[df[symbol_col].str.strip() == ticker]
            if row.empty:
                logger.debug(f"RegSHO: {ticker} not found in {date_str} file")
                return 0.0

            short_vol = float(row.iloc[0][short_col])
            total_vol = float(row.iloc[0][total_col])

            if total_vol <= 0:
                return 0.0

            ratio = round(short_vol / total_vol, 4)
            logger.debug(f"RegSHO {ticker} [{date_str}]: svr={ratio:.3f}")
            return ratio

        except Exception as exc:
            logger.debug(f"RegSHO fetch error [{date_str}]: {exc}")
            continue

    return 0.0


# ─────────────────────────────────────────────────────────
#  Main client
# ─────────────────────────────────────────────────────────

class ShortDataClient:
    """
    Assembles a ShortProfile for a ticker from three free sources:

      1. yfinance .info  — short interest, float, shares, DTC, ownership, beta
      2. FINRA RegSHO    — daily short-volume ratio (directional pressure proxy)
      3. Finnhub metrics — beta / 52-week range crosscheck  (optional, free key)

    Results are cached in Redis with a 4-hour TTL.
    Short interest data itself changes at most twice a month (FINRA cycle),
    so 4-hour caching introduces negligible staleness for scanning purposes.

    Usage
    ─────
        client = ShortDataClient()
        profile = client.get_short_profile("GME")
        print(profile.short_float_pct, profile.short_ratio)

        profiles = client.get_bulk_profiles(["GME", "AMC", "TSLA"])
        top_si   = client.get_top_short_float(["GME", "AMC", "TSLA"], top_n=2)
    """

    _CACHE_TTL = 14_400   # 4 hours

    def __init__(self):
        self._redis = redis.Redis(
            host=_api.redis_host,
            port=_api.redis_port,
            db=_api.redis_db,
            decode_responses=True,
        )

    # ── Public API ────────────────────────────────────────

    def get_short_profile(
        self,
        ticker: str,
        force_refresh: bool = False,
        include_regsho: bool = True,
    ) -> ShortProfile:
        """
        Return the ShortProfile for `ticker`.

        Parameters
        ──────────
        force_refresh   — bypass Redis cache and pull fresh data.
        include_regsho  — whether to fetch the FINRA RegSHO daily file
                          (adds ~0.5 s but provides short_vol_ratio).
                          Set False for bulk scans where speed matters more.
        """
        ticker = ticker.upper()
        if not force_refresh:
            cached = self._load_cache(ticker)
            if cached:
                return cached

        profile = self._build_profile(ticker, include_regsho=include_regsho)
        self._save_cache(profile)
        return profile

    def get_bulk_profiles(
        self,
        tickers: list[str],
        include_regsho: bool = False,
    ) -> dict[str, ShortProfile]:
        """
        Return ShortProfiles for multiple tickers.
        RegSHO is disabled by default in bulk mode to avoid rate-limiting the
        FINRA CDN with one request per ticker — use a separate batch RegSHO
        call (fetch_regsho_batch) instead.
        """
        return {
            t.upper(): self.get_short_profile(t, include_regsho=include_regsho)
            for t in tickers
        }

    def get_top_short_float(
        self,
        tickers: list[str],
        top_n: int = 10,
    ) -> list[ShortProfile]:
        """Return the top-N tickers ranked by short float %."""
        profiles = self.get_bulk_profiles(tickers)
        return sorted(
            profiles.values(),
            key=lambda p: p.short_float_pct,
            reverse=True,
        )[:top_n]

    def fetch_regsho_batch(self, tickers: list[str]) -> dict[str, float]:
        """
        Fetch one RegSHO file and extract short_vol_ratio for all tickers
        in a single HTTP request.  More efficient than one call per ticker.

        Returns dict[ticker → short_vol_ratio]
        """
        ticker_set = {t.upper() for t in tickers}
        results    = {t: 0.0 for t in ticker_set}

        for offset in range(4):
            date_str = _regsho_date_str(offset)
            url      = _REGSHO_URL.format(date=date_str)
            try:
                resp = requests.get(url, headers=_REGSHO_HEADERS, timeout=20)
                if resp.status_code == 404:
                    continue
                if resp.status_code != 200:
                    break

                df = pd.read_csv(
                    io.StringIO(resp.text),
                    sep="|",
                    dtype=str,
                    on_bad_lines="skip",
                )
                df.columns = [c.strip().lower() for c in df.columns]

                symbol_col = next((c for c in df.columns if "symbol" in c), None)
                short_col  = next((c for c in df.columns
                                   if "shortvol" in c and "exempt" not in c), None)
                total_col  = next((c for c in df.columns
                                   if "totalvol" in c or "totalvolume" in c), None)

                if not all([symbol_col, short_col, total_col]):
                    break

                df[symbol_col] = df[symbol_col].str.strip()
                sub = df[df[symbol_col].isin(ticker_set)].copy()

                for _, row in sub.iterrows():
                    sym   = row[symbol_col]
                    s_vol = float(row[short_col])
                    t_vol = float(row[total_col])
                    if t_vol > 0:
                        results[sym] = round(s_vol / t_vol, 4)

                logger.debug(
                    f"RegSHO batch [{date_str}]: "
                    f"found {sum(1 for v in results.values() if v > 0)}"
                    f"/{len(ticker_set)} tickers"
                )
                return results   # success — return without trying older dates

            except Exception as exc:
                logger.debug(f"RegSHO batch error [{date_str}]: {exc}")
                continue

        return results

    # ── Build profile ─────────────────────────────────────

    def _build_profile(self, ticker: str, include_regsho: bool = True) -> ShortProfile:
        """Merge data from all sources into one ShortProfile."""
        yf_info  = self._fetch_yfinance_info(ticker)
        fh_data  = self._fetch_finnhub_metrics(ticker)

        # ── Share structure ───────────────────────────────
        float_shares = (
            yf_info.get("floatShares") or 0.0
        )
        shares_out = (
            yf_info.get("sharesOutstanding") or 0.0
        )

        # ── Short interest ────────────────────────────────
        short_interest   = yf_info.get("sharesShort") or 0.0
        shares_short_prev= yf_info.get("sharesShortPreviousMonthDate") and \
                           yf_info.get("sharesShortPriorMonth") or 0.0
        # yfinance provides shortRatio directly (days-to-cover)
        short_ratio = yf_info.get("shortRatio") or 0.0
        if short_ratio == 0.0 and short_interest > 0:
            avg_vol = yf_info.get("averageDailyVolume10Day") or \
                      yf_info.get("averageVolume") or 1
            short_ratio = round(short_interest / max(avg_vol, 1), 2)

        short_float_pct = (
            yf_info.get("shortPercentOfFloat") or
            (short_interest / float_shares if float_shares > 0 else 0.0)
        )

        # ── FINRA RegSHO short-volume ratio ───────────────
        short_vol_ratio = (
            fetch_regsho_short_vol_ratio(ticker) if include_regsho else 0.0
        )

        # ── Ownership ─────────────────────────────────────
        inst_pct   = yf_info.get("heldPercentInstitutions") or 0.0
        insider_pct= yf_info.get("heldPercentInsiders")     or 0.0

        # ── Market data ───────────────────────────────────
        market_cap = yf_info.get("marketCap") or 0.0
        price = (
            yf_info.get("currentPrice") or
            yf_info.get("regularMarketPrice") or
            yf_info.get("previousClose") or 0.0
        )
        beta = (
            yf_info.get("beta") or
            fh_data.get("beta") or 1.0
        )
        avg_vol_10d = (
            yf_info.get("averageDailyVolume10Day") or
            yf_info.get("averageVolume") or 0.0
        )

        # Determine the best data source label
        sources = ["yfinance"]
        if include_regsho and short_vol_ratio > 0:
            sources.append("finra_regsho")
        if fh_data:
            sources.append("finnhub")

        return ShortProfile(
            ticker               = ticker,
            float_shares         = float(float_shares),
            shares_outstanding   = float(shares_out),
            short_interest       = float(short_interest),
            short_float_pct      = round(float(short_float_pct), 4),
            short_ratio          = round(float(short_ratio), 2),
            short_vol_ratio      = float(short_vol_ratio),
            shares_short_prior   = float(shares_short_prev),
            borrow_rate_pct      = 0.0,   # no free source available
            inst_ownership_pct   = round(float(inst_pct), 4),
            insider_ownership_pct= round(float(insider_pct), 4),
            market_cap           = float(market_cap),
            price                = float(price),
            beta                 = round(float(beta), 3),
            avg_volume_10d       = float(avg_vol_10d),
            data_source          = "+".join(sources),
            updated_at           = int(time.time()),
        )

    # ── Data fetchers ─────────────────────────────────────

    def _fetch_yfinance_info(self, ticker: str) -> dict:
        """
        Pull Yahoo Finance summary info for a ticker.
        Returns {} on any error so callers always get a safe dict.
        """
        try:
            info = yf.Ticker(ticker).info
            return info if isinstance(info, dict) else {}
        except Exception as exc:
            logger.warning(f"yfinance.info [{ticker}]: {exc}")
            return {}

    def _fetch_finnhub_metrics(self, ticker: str) -> dict:
        """
        Fetch Finnhub basic financials (beta, 52w range etc.).
        Returns {} if no API key is configured or the request fails.
        Free tier: 60 req/min — fine for periodic refreshes.
        """
        if not getattr(_api, "finnhub_api_key", ""):
            return {}
        url    = "https://finnhub.io/api/v1/stock/metric"
        params = {"symbol": ticker, "metric": "all", "token": _api.finnhub_api_key}
        try:
            resp = requests.get(url, params=params, timeout=10,
                                headers={"User-Agent": "StockScanner/1.0"})
            if resp.status_code == 200:
                return resp.json().get("metric", {})
        except Exception as exc:
            logger.debug(f"Finnhub metrics [{ticker}]: {exc}")
        return {}

    # ── Redis cache ───────────────────────────────────────

    def _save_cache(self, profile: ShortProfile) -> None:
        key = f"short:{profile.ticker}"
        try:
            self._redis.hset(key, mapping=profile.to_redis_mapping())
            self._redis.expire(key, self._CACHE_TTL)
        except Exception as exc:
            logger.debug(f"Cache write failed [{profile.ticker}]: {exc}")

    def _load_cache(self, ticker: str) -> Optional[ShortProfile]:
        key = f"short:{ticker.upper()}"
        try:
            raw = self._redis.hgetall(key)
            if not raw:
                return None
            return ShortProfile.from_redis(raw)
        except Exception as exc:
            logger.debug(f"Cache read failed [{ticker}]: {exc}")
            return None
