
"""
test_options_feed.py
Unit and integration tests for options_feed.py.

Uses actual yfinance feed for integration tests, mocks Redis and other dependencies.
"""

import json
import pytest
from unittest.mock import patch, MagicMock, call
import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))																			   
from ingestion.options_feed import (
    YFinanceOptionsClient,
    OptionsMetrics,
    SweepCandidate,
    UnusualWhalesFeed,
    _api,
)


# ─────────────────────────────────────────────────────────
#  Fixtures
# ─────────────────────────────────────────────────────────

@pytest.fixture
def mock_redis():
    """Mock Redis instance."""
    return MagicMock()


@pytest.fixture
def sample_calls_df():
    """Sample calls DataFrame mimicking yfinance output."""
    return pd.DataFrame({
        'contractSymbol': ['AAPL231231C00150000'],
        'strike': [150.0],
        'bid': [1.0],
        'ask': [1.5],
        'lastPrice': [1.2],
        'volume': [1000],
        'openInterest': [500],
        'impliedVolatility': [0.3],
        'inTheMoney': [False],
        'expiry': ['2023-12-31'],
        'option_type': ['call'],
    })


@pytest.fixture
def sample_puts_df():
    """Sample puts DataFrame."""
    return pd.DataFrame({
        'contractSymbol': ['AAPL231231P00140000'],
        'strike': [140.0],
        'bid': [0.5],
        'ask': [1.0],
        'lastPrice': [0.8],
        'volume': [800],
        'openInterest': [400],
        'impliedVolatility': [0.25],
        'inTheMoney': [True],
        'expiry': ['2023-12-31'],
        'option_type': ['put'],
    })


# ─────────────────────────────────────────────────────────
#  Data class tests
# ─────────────────────────────────────────────────────────

def test_sweep_candidate_to_dict():
    """Test SweepCandidate.to_dict()."""
    sc = SweepCandidate(
        ticker='AAPL',
        option_type='call',
        strike=150.0,
        expiry='2023-12-31',
        volume=1000,
        open_interest=500,
        vol_oi_ratio=2.0,
        bid=1.0,
        ask=1.5,
        last=1.2,
        iv=0.3,
        in_the_money=False,
    )
    d = sc.to_dict()
    assert d['ticker'] == 'AAPL'
    assert d['option_type'] == 'call'
    assert d['strike'] == 150.0
    assert d['volume'] == 1000
    assert d['vol_oi_ratio'] == 2.0


def test_options_metrics_to_redis_mapping():
    """Test OptionsMetrics.to_redis_mapping()."""
    om = OptionsMetrics(
        ticker='AAPL',
        call_volume=1000,
        put_volume=800,
        total_volume=1800,
        pc_volume_ratio=0.8,
    )
    mapping = om.to_redis_mapping()
    assert isinstance(mapping, dict)
    assert mapping['ticker'] == 'AAPL'
    assert mapping['call_volume'] == '1000'
    assert mapping['pc_volume_ratio'] == '0.8'


def test_options_metrics_from_redis():
    """Test OptionsMetrics.from_redis()."""
    raw = {
        'ticker': 'AAPL',
        'call_volume': '1000',
        'put_volume': '800',
        'total_volume': '1800',
        'pc_volume_ratio': '0.8',
        'avg_iv': '0.25',
        'sweep_count': '2',
        'top_sweeps': '[]',
        'ts': '1640995200',
    }
    om = OptionsMetrics.from_redis(raw)
    assert om.ticker == 'AAPL'
    assert om.call_volume == 1000
    assert om.put_volume == 800
    assert om.pc_volume_ratio == 0.8
    assert om.avg_iv == 0.25
    assert om.sweep_count == 2


# ─────────────────────────────────────────────────────────
#  YFinanceOptionsClient tests
# ─────────────────────────────────────────────────────────

@patch('ingestion.options_feed.redis.Redis')
def test_yfinance_client_init(mock_redis_class, mock_redis):
    """Test YFinanceOptionsClient initialization."""
    mock_redis_class.return_value = mock_redis
    client = YFinanceOptionsClient()
    assert client._redis == mock_redis
    assert client.poll_interval == 120


@patch('ingestion.options_feed.redis.Redis')
def test_load_cache_none(mock_redis_class, mock_redis):
    """Test _load_cache when no data."""
    mock_redis_class.return_value = mock_redis
    mock_redis.hgetall.return_value = {}
    client = YFinanceOptionsClient()
    result = client._load_cache('AAPL')
    assert result is None


@patch('ingestion.options_feed.redis.Redis')
def test_load_cache_with_data(mock_redis_class, mock_redis):
    """Test _load_cache with data."""
    mock_redis_class.return_value = mock_redis
    raw = {'ticker': 'AAPL', 'call_volume': '1000'}
    mock_redis.hgetall.return_value = raw
    client = YFinanceOptionsClient()
    result = client._load_cache('AAPL')
    assert isinstance(result, OptionsMetrics)
    assert result.ticker == 'AAPL'


@patch('ingestion.options_feed.redis.Redis')
def test_save_cache(mock_redis_class, mock_redis):
    """Test _save_cache."""
    mock_redis_class.return_value = mock_redis
    client = YFinanceOptionsClient()
    metrics = OptionsMetrics(ticker='AAPL')
    client._save_cache(metrics)
    mock_redis.hset.assert_called_once()
    mock_redis.expire.assert_called_once_with('options:AAPL', 300)


@patch('ingestion.options_feed.redis.Redis')
def test_get_cached_metrics_empty(mock_redis_class, mock_redis):
    """Test get_cached_metrics when no cache."""
    mock_redis_class.return_value = mock_redis
    mock_redis.hgetall.return_value = {}
    client = YFinanceOptionsClient()
    result = client.get_cached_metrics('AAPL')
    assert result == {}


@patch('ingestion.options_feed.redis.Redis')
def test_get_cached_metrics_with_data(mock_redis_class, mock_redis):
    """Test get_cached_metrics with data."""
    mock_redis_class.return_value = mock_redis
    raw = {'ticker': 'AAPL', 'top_sweeps': '[{"ticker": "AAPL"}]'}
    mock_redis.hgetall.return_value = raw
    client = YFinanceOptionsClient()
    result = client.get_cached_metrics('AAPL')
    assert result['ticker'] == 'AAPL'
    assert result['top_sweeps'] == [{"ticker": "AAPL"}]


def test_compute_metrics(sample_calls_df, sample_puts_df):
    """Test _compute_metrics with sample data."""
    client = YFinanceOptionsClient()
    expiries = ('2023-12-31',)
    metrics = client._compute_metrics('AAPL', sample_calls_df, sample_puts_df, expiries)
    assert isinstance(metrics, OptionsMetrics)
    assert metrics.ticker == 'AAPL'
    assert metrics.call_volume == 1000
    assert metrics.put_volume == 800
    assert metrics.total_volume == 1800
    assert metrics.pc_volume_ratio == 0.8
    assert metrics.sweep_count == 2  # Both qualify as sweeps
    assert len(metrics.top_sweeps) > 0


@pytest.mark.integration
@patch('ingestion.options_feed.redis.Redis')
def test_fetch_chain_integration(mock_redis_class, mock_redis):
    """Integration test: fetch_chain with actual yfinance (if available)."""
    mock_redis_class.return_value = mock_redis
    mock_redis.hgetall.return_value = {}  # No cache
    client = YFinanceOptionsClient()
    # This will call real yfinance API
    metrics = client.fetch_chain('AAPL', force_refresh=True)
    assert isinstance(metrics, OptionsMetrics)
    assert metrics.ticker == 'AAPL'
    # Note: Actual values depend on market data


@pytest.mark.integration
@patch('ingestion.options_feed.redis.Redis')
def test_fetch_chain_bulk_integration(mock_redis_class, mock_redis):
    """Integration test: fetch_chain_bulk with actual yfinance."""
    mock_redis_class.return_value = mock_redis
    client = YFinanceOptionsClient()
    results = client.fetch_chain_bulk(['AAPL'], sleep_between=0.1)
    assert 'AAPL' in results
    assert isinstance(results['AAPL'], OptionsMetrics)


@patch('ingestion.options_feed.redis.Redis')
def test_get_top_sweeps(mock_redis_class, mock_redis):
    """Test get_top_sweeps."""
    mock_redis_class.return_value = mock_redis
    raw = {'top_sweeps': '[{"ticker": "AAPL", "volume": 1000}]'}
    mock_redis.hgetall.return_value = raw
    client = YFinanceOptionsClient()
    sweeps = client.get_top_sweeps('AAPL')
    assert sweeps == [{"ticker": "AAPL", "volume": 1000}]


@patch('ingestion.options_feed.redis.Redis')
def test_get_iv_surface_summary(mock_redis_class, mock_redis):
    """Test get_iv_surface_summary."""
    mock_redis_class.return_value = mock_redis
    raw = {'avg_iv': '0.25', 'call_avg_iv': '0.3', 'put_avg_iv': '0.2', 'iv_skew': '-0.1', 'pc_vol': '0.8', 'pc_oi': '1.2'}
    mock_redis.hgetall.return_value = raw
    client = YFinanceOptionsClient()
    summary = client.get_iv_surface_summary('AAPL')
    assert summary['ticker'] == 'AAPL'
    assert summary['avg_iv'] == 0.25
    assert summary['iv_skew'] == -0.1


# ─────────────────────────────────────────────────────────
#  UnusualWhalesFeed tests
# ─────────────────────────────────────────────────────────

@patch('ingestion.options_feed.redis.Redis')
def test_unusual_whales_feed_init(mock_redis_class, mock_redis):
    """Test UnusualWhalesFeed initialization."""
    mock_redis_class.return_value = mock_redis
    feed = UnusualWhalesFeed()
    assert feed._redis == mock_redis
    assert not feed._running


@patch('ingestion.options_feed.redis.Redis')
@patch('ingestion.options_feed.time.time', return_value=1234567890)
def test_handle_alert_sweep(mock_time, mock_redis_class, mock_redis):
    """Test _handle_alert for sweep."""
    mock_redis_class.return_value = mock_redis
    feed = UnusualWhalesFeed()
    payload = {
        'type': 'sweep',
        'ticker': 'AAPL',
        'total_premium': 10000,
        'put_call': 'CALL',
        'strike_price': 150.0,
        'expiry_date': '2023-12-31',
        'size': 100,
        'underlying_price': 145.0,
        'sentiment': 'bullish',
    }
    feed._handle_alert(payload)
    expected_record = {
        'ticker': 'AAPL',
        'type': 'sweep',
        'premium': 10000,
        'side': 'CALL',
        'strike': 150.0,
        'expiry': '2023-12-31',
        'size': 100,
        'spot': 145.0,
        'sentiment': 'bullish',
        'ts': 1234567890,
    }
    expected_json = json.dumps(expected_record)
    mock_redis.lpush.assert_has_calls([
        call("uw:sweeps", expected_json),
        call("uw:ticker:AAPL", expected_json),
    ])
    mock_redis.ltrim.assert_has_calls([
        call("uw:sweeps", 0, 499),
        call("uw:ticker:AAPL", 0, 49),
    ])
    mock_redis.expire.assert_called_with("uw:ticker:AAPL", 86400)


@patch('ingestion.options_feed.redis.Redis')
@patch('ingestion.options_feed.time.time', return_value=1234567890)
def test_handle_alert_darkpool(mock_time, mock_redis_class, mock_redis):
    """Test _handle_alert for dark pool."""
    mock_redis_class.return_value = mock_redis
    feed = UnusualWhalesFeed()
    payload = {
        'type': 'dark pool block',
        'ticker': 'AAPL',
        'total_premium': 50000,
    }
    feed._handle_alert(payload)
    expected_record = {
        'ticker': 'AAPL',
        'type': 'dark pool block',
        'premium': 50000,
        'side': '',
        'strike': None,
        'expiry': None,
        'size': 0,
        'spot': None,
        'sentiment': None,
        'ts': 1234567890,
    }
    expected_json = json.dumps(expected_record)
    mock_redis.lpush.assert_has_calls([
        call("uw:darkpool", expected_json),
        call("uw:ticker:AAPL", expected_json),
    ])
    mock_redis.ltrim.assert_has_calls([
        call("uw:darkpool", 0, 499),
        call("uw:ticker:AAPL", 0, 49),
    ])
    mock_redis.expire.assert_called_with("uw:ticker:AAPL", 86400)


@patch('ingestion.options_feed.redis.Redis')
def test_get_ticker_alerts(mock_redis_class, mock_redis):
    """Test get_ticker_alerts."""
    mock_redis_class.return_value = mock_redis
    alerts = ['{"ticker": "AAPL"}', '{"ticker": "AAPL"}']
    mock_redis.lrange.return_value = alerts
    feed = UnusualWhalesFeed()
    result = feed.get_ticker_alerts('AAPL')
    assert len(result) == 2
    assert result[0]['ticker'] == 'AAPL'


@patch('ingestion.options_feed.redis.Redis')
def test_get_recent_sweeps(mock_redis_class, mock_redis):
    """Test get_recent_sweeps."""
    mock_redis_class.return_value = mock_redis
    sweeps = ['{"type": "sweep"}']
    mock_redis.lrange.return_value = sweeps
    feed = UnusualWhalesFeed()
    result = feed.get_recent_sweeps()
    assert len(result) == 1
    assert result[0]['type'] == 'sweep'


@patch('ingestion.options_feed.redis.Redis')
def test_get_recent_darkpool(mock_redis_class, mock_redis):
    """Test get_recent_darkpool."""
    mock_redis_class.return_value = mock_redis
    darkpool = ['{"type": "dark pool"}']
    mock_redis.lrange.return_value = darkpool
    feed = UnusualWhalesFeed()
    result = feed.get_recent_darkpool()
    assert len(result) == 1
    assert result[0]['type'] == 'dark pool'


if __name__ == "__main__":
    import subprocess, sys
    sys.exit(subprocess.call(
        [sys.executable, "-m", "pytest", __file__, "-v", "--tb=short"],
    ))
