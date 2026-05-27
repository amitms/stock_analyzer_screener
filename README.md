this application fetches real-time stock data from a. free api , b. latest real-time news for that stock. source should be 24 hour real-time and not delayed source
calculates trending stock with optional filter of penny stock (under $5) , or other higher value stocks by considering financial, technical parameters, latest news catalyst, market sentiment (such as future indices), reddit trends, unsual whales , unsual options activity and "dark pool" prints, such as current volume, average volume, unusual relative volume (RVOL) , volatility index (such as Beta), float share, short index, short float and other necessary catalyst, shortsqueeze, low float factors
calculate and predict price breakout for next day by considering above metrics, if needed using ai algorithms.

<img width="200" height="500" alt="realtime_stock_analysis_system" src="https://github.com/user-attachments/assets/84da62c1-4f0a-411b-b282-8a12973b38c2" />
<br>
═════════════════════════════<br>
LAYER 1 — DATA INGESTION
<br>═════════════════════════════<br>
Price / OHLCV  <br>
Options flow  <br>
Dark pool / tape <br>
Sentiment / news <br>
Social row <br>
═════════════════════════════<br>
LAYER 2 — SIGNAL COMPUTATION ENGINE
<br>═════════════════════════════<br>
Volume signals <br>
Technical signals <br>
Risk signals <br>
Options signals <br>
Catalyst signals <br>
Market context <br>

<br>═════════════════════════════<br>
LAYER 3 — AI SCORING & BREAKOUT PREDICTION
<br>═════════════════════════════ <br>
Composite scorer <br>
ML breakout model <br>
Short squeeze detector <br>
<br>═════════════════════════════<br>
       LAYER 4 — OUTPUT
<br>═════════════════════════════<br> 
dashboard <br>
backtest <br>
<br>═════════════════════════════<br>
       LAYER 5 — Python stack
<br>═════════════════════════════<br>

 ## Project Structure

```
stock_scanner/
├── .env.example           API key template
├── requirements.txt       All Python dependencies
├── scanner.py             Main entry point
├── config/
│   └── settings.py        Central config
├── ingestion/
│   ├── price_feed.py      Polygon WebSocket + yfinance
│   ├── options_feed.py    Tradier + Unusual Whales
│   ├── news_feed.py       Finnhub + Reddit PRAW
│   └── short_data.py      Fintel + yfinance short data
├── signals/
│   ├── volume_signals.py  RVOL, technical indicators
│   └── composite_signals.py Options, risk, catalyst, market
├── ai/
│   ├── composite_scorer.py Signal fusion
│   ├── breakout_model.py  XGBoost breakout predictor
│   └── squeeze_detector.py Squeeze + FinBERT NLP
├── output/
│   ├── alerts.py          Discord + SQLite
│   ├── dashboard.py       Plotly Dash UI
│   └── backtest.py        Strategy backtesting
└── data/                  Auto-created: DB, CSVs, equity curves
    models/                Auto-created: trained model files
    logs/                  Auto-created: log files
```
### Redis intallation and run:
from docker:
docker ps
docker run -d -p 6379:6379 redis
test
docker exec -it redis redis-cli ping

Start Redis (if not running)
redis-server
or docker run -d -p 6379:6379 redis

fake redis in python script : pip install pytest pytest-asyncio fakeredis pandas

<img width="407" height="494" alt="redis_server" src="https://github.com/user-attachments/assets/c1ae3e90-bd95-4afb-af96-ab6cf12b9a2c" />

### sqlite db sample
./data/scan_results.db
<img width="328" height="394" alt="sqlite3_scan_results_db" src="https://github.com/user-attachments/assets/3122caf3-7955-42c7-8970-636641f4a94f" />

## Usage:
1. make sure docker / redis is running.
2. Model train: 
python -m ai.breakout_model train --tickers AAPL,TSLA,NVDA,AMD --lookback 365 --tune --trials 50  <br>
backtest: python -m output.backtest --tickers AAPL,TSLA,NVDA,AMD,MARA --period 1y --top-n 3 --freq W  <br>
3. python scanner.py <br>
4. Dashboard run: python -m output.dashboard <br>
http://127.0.0.1:8050 <br>
http://192.168.1.69:8050 <br>
<img width="938" height="466" alt="dashboard" src="https://github.com/user-attachments/assets/1de42cec-f6ae-4b48-9669-48b7408509ed" />
