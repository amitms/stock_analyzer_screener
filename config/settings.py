"""
config/settings.py
Central configuration — all API keys and tunable thresholds live here.
Copy .env.example to .env and fill in your keys.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv, dotenv_values

load_dotenv(Path(__file__).parent.parent/"config"/"config.env")
# print(dotenv_values(Path(__file__).resolve().parent.parent /"config"/"config.env"))
# ─────────────────────────────────────────────────────────
#  API credentials
# ─────────────────────────────────────────────────────────
@dataclass
class APIConfig:
    # get environment values from Github Action Secrets
    # alpaca_endpoint = os.environ.get('ALPACA_ENDPOINT')
    # apca_api_key_id = os.environ.get('APCA-API-KEY-ID')
    # apca_api_secret = os.environ.get('APCA-API-SECRET-KEY')
    # polygon_api_key = os.environ.get('POLYGON_API_KEY')
    # finnhub_api_key = os.environ.get('FINNHUB_API_KEY')
    # benzinga_api_key = os.environ.get('BENZINGA_API_KEY')
    # tradier_api_key = os.environ.get('TRADIER_API_KEY')
    # unusual_whales_token = os.environ.get('UNUSUAL_WHALES_TOKEN')
    # fintel_api_key = os.environ.get('FINTEL_API_KEY')
    # reddit_client_id = os.environ.get('REDDIT_CLIENT_ID')
    # reddit_client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
    # discord_webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
    # redis_host = os.environ.get('REDIS_HOST')

    # apaca (real-time WebSocket )
    alpaca_endpoint: str = field(default_factory=lambda: os.getenv("ALPACA_ENDPOINT", ""))
    apca_api_key_id: str = field(default_factory=lambda: os.getenv("APCA-API-KEY-ID", "")) 
    apca_api_secret: str = field(default_factory=lambda: os.getenv("APCA-API-SECRET-KEY", ""))

    # Polygon.io  (real-time WebSocket — requires Starter plan or above)
    polygon_api_key: str = field(default_factory=lambda: os.getenv("POLYGON_API_KEY", ""))

    # Finnhub  (free tier: 60 calls/min, real-time news webhooks on paid)
    finnhub_api_key: str = field(default_factory=lambda: os.getenv("FINNHUB_API_KEY", ""))

    # Benzinga  (real-time news; paid API)
    benzinga_api_key: str = field(default_factory=lambda: os.getenv("BENZINGA_API_KEY", ""))

    # Tradier  (options chain, real-time quotes on sandbox or brokerage account)
    tradier_api_key: str = field(default_factory=lambda: os.getenv("TRADIER_API_KEY", ""))
    tradier_base_url: str = "https://api.tradier.com/v1"

    # Unusual Whales  (options flow / dark pool WebSocket)
    unusual_whales_token: str = field(default_factory=lambda: os.getenv("UNUSUAL_WHALES_TOKEN", ""))

    # Fintel  (short interest data — scrape-friendly or API tier)
    fintel_api_key: str = field(default_factory=lambda: os.getenv("FINTEL_API_KEY", ""))

    # Reddit  (PRAW — create an app at reddit.com/prefs/apps)
    reddit_client_id: str = field(default_factory=lambda: os.getenv("REDDIT_CLIENT_ID", ""))
    reddit_client_secret: str = field(default_factory=lambda: os.getenv("REDDIT_CLIENT_SECRET", ""))
    reddit_user_agent: str = "StockScanner/1.0"

    # Discord webhook for alerts
    discord_webhook_url: str = field(default_factory=lambda: os.getenv("DISCORD_WEBHOOK_URL", ""))

    # Redis
    redis_host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    redis_port: int = 6379
    redis_db: int = 0


# ─────────────────────────────────────────────────────────
#  Scanner thresholds
# ─────────────────────────────────────────────────────────
@dataclass
class ScannerConfig:
    # Stock universe filters
    min_price_penny: float = 0.10     # floor to avoid sub-penny junk
    max_price_penny: float = 5.00     # penny stock ceiling
    min_price_midcap: float = 5.01
    max_price_midcap: float = 20.00
    min_dollar_volume: float = 500_000  # must trade at least $500k/day

    # Volume
    rvol_alert_threshold: float = 3.0    # flag when RVOL >= 3×
    rvol_squeeze_threshold: float = 5.0  # used in squeeze detector
    avg_volume_lookback_days: int = 10

    # Short squeeze
    short_float_threshold: float = 0.20   # >= 20% short float
    short_days_to_cover_min: float = 5.0  # days-to-cover >= 5
    float_shares_max_squeeze: float = 20_000_000  # low-float: < 20M shares

    # Options
    put_call_ratio_bullish: float = 0.70  # < 0.7 = bullish flow
    iv_rank_elevated: float = 0.50        # IV rank > 50 = elevated premium

    # Breakout model
    breakout_probability_threshold: float = 0.65  # min ML confidence to alert
    breakout_pct_target: float = 0.05             # 5% next-day move = label=1

    # Market context
    vix_high_regime: float = 25.0   # above = high-vol regime, tighten filters
    vix_fear_regime: float = 35.0   # above = extreme fear, only squeeze setups

    # Composite score weights (must sum to 1.0 within each category)
    weights_penny: dict = field(default_factory=lambda: {
        "volume":    0.30,
        "technical": 0.20,
        "risk":      0.10,
        "options":   0.15,
        "catalyst":  0.20,
        "market":    0.05,
    })
    weights_midcap: dict = field(default_factory=lambda: {
        "volume":    0.20,
        "technical": 0.20,
        "risk":      0.15,
        "options":   0.25,
        "catalyst":  0.10,
        "market":    0.10,
    })

    # Top-N results to surface per run
    top_n_results: int = 20

    # Subreddits to monitor
    reddit_subreddits: list = field(default_factory=lambda: [
        "wallstreetbets", "stocks", "pennystocks",
        "investing", "shortsqueeze", "Shortsqueeze",
    ])

    # Scheduler intervals (seconds)
    price_refresh_interval: int = 5
    signal_refresh_interval: int = 30
    reddit_refresh_interval: int = 300
    news_refresh_interval: int = 60


# ─────────────────────────────────────────────────────────
#  Paths
# ─────────────────────────────────────────────────────────
@dataclass
class PathConfig:
    base: Path = Path(__file__).parent.parent
    data: Path = field(default_factory=lambda: Path(__file__).parent.parent / "data")
    models: Path = field(default_factory=lambda: Path(__file__).parent.parent / "models")
    logs: Path = field(default_factory=lambda: Path(__file__).parent.parent / "logs")

    def __post_init__(self):
        for p in (self.data, self.models, self.logs):
            p.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────
#  Singleton accessors
# ─────────────────────────────────────────────────────────
_api = APIConfig()
_scanner = ScannerConfig()
_paths = PathConfig()


def get_api_config() -> APIConfig:
    return _api


def get_scanner_config() -> ScannerConfig:
    return _scanner


def get_path_config() -> PathConfig:
    return _paths

# print(get_api_config())
# print(get_scanner_config())
# print(get_path_config())

