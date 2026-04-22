this application fetches real-time stock data from a. free api , b. latest real-time news for that stock. source should be 24 hour real-time and not delayed source
calculates trending stock with optional filter of penny stock (under $5) , or other higher value stocks by considering financial, technical parameters, latest news catalyst, market sentiment (such as future indices), reddit trends, unsual whales , unsual options activity and "dark pool" prints, such as current volume, average volume, unusual relative volume (RVOL) , volatility index (such as Beta), float share, short index, short float and other necessary catalyst, shortsqueeze, low float factors
calculate and predict price breakout for next day by considering above metrics, if needed using ai algorithms.

<img width="200" height="500" alt="realtime_stock_analysis_system" src="https://github.com/user-attachments/assets/84da62c1-4f0a-411b-b282-8a12973b38c2" />
<br>
═════════════════════════════<br>
LAYER 1 — DATA INGESTION
═════════════════════════════<br>
Price / OHLCV 
Options flow 
Dark pool / tape 
Sentiment / news 
 Social row 
═════════════════════════════<br>
LAYER 2 — SIGNAL COMPUTATION ENGINE
═════════════════════════════<br>
Volume signals 
Technical signals 
Risk signals 
Options signals 
Catalyst signals 
Market context 

═════════════════════════════<br>
LAYER 3 — AI SCORING & BREAKOUT PREDICTION
═════════════════════════════ <br>
Composite scorer 
ML breakout model 
Short squeeze detector 
═════════════════════════════<br>
       LAYER 4 — OUTPUT
═════════════════════════════<br> 

═════════════════════════════<br>
       LAYER 5 — Python stack
═════════════════════════════<br>
 
