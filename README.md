# Indian Stock Market Data Pipeline (BSE)

## Project Overview
An end-to-end data engineering pipeline that ingests live BSE stock market data 
for 5 major Indian companies via Alpha Vantage REST API, processes it using 
PySpark on Databricks following the Medallion Architecture (Bronze → Silver → Gold), 
and visualizes business insights through a Tableau Public dashboard.

## Stocks Covered
| Symbol | Company |
| TCS.BSE | Tata Consultancy Services |
| RELIANCE.BSE | Reliance Industries |
| INFY.BSE | Infosys |
| HDFCBANK.BSE | HDFC Bank |
| WIPRO.BSE | Wipro |

## Architecture

Alpha Vantage REST API (Free Tier)
↓
Python requests library
↓
┌─────────────────────────┐
│     BRONZE LAYER        │
│  Raw API JSON → Delta   │
│  StructType Schema      │
└─────────────────────────┘
↓
┌─────────────────────────┐
│     SILVER LAYER        │
│  PySpark Transformations│
│  8 Derived Financial    │
│  Metrics Added          │
└─────────────────────────┘
↓
┌─────────────────────────┐
│      GOLD LAYER         │
│  Spark SQL Analytics    │
│  4 Business Tables      │
└─────────────────────────┘
↓
┌─────────────────────────┐
│   TABLEAU DASHBOARD     │
│  4 Interactive Charts   │
└─────────────────────────┘

## Tech Stack
| Tool | Purpose |
|---|---|
| Alpha Vantage API | Free BSE stock market data source |
| Python requests | REST API ingestion |
| Databricks Free Edition | Cloud compute platform |
| Apache Spark / PySpark | Data transformation engine |
| Delta Lake | ACID storage with time travel + MERGE |
| Spark SQL | Business analytics queries |
| Unity Catalog Volumes | File storage (modern replacement for DBFS) |
| Tableau Public | Dashboard and visualization |
| GitHub | Version control and portfolio |


## Project Structure

indian-stock-pipeline/
├── notebooks/
│   ├── 01_API_Config.py              # API key, base URL, stock symbols config
│   ├── 02_Bronze_Ingestion.py        # API ingestion → raw Delta table
│   ├── 03_Silver_Transformation.py   # PySpark cleaning + derived columns
│   └── 04_Gold_Analytics.py          # 4 Spark SQL Gold analytics tables
├── dashboard/
│   └── screenshots/
│       ├── dashboard_overview.png
│       ├── stock_price_trend.png
│       ├── risk_vs_return.png
│       └── monthly_returns.png
└── README.md

## Medallion Architecture — Layer by Layer

### Bronze Layer
- Calls Alpha Vantage REST API for each stock using Python requests library
- Saves raw JSON response to Unity Catalog Volume
- Converts to Spark DataFrame using explicit StructType schema
- Saves as Delta table — preserves raw data as-is
- ~500 rows (5 stocks × 100 trading days)

### Silver Layer
- Reads from Bronze Delta table
- Converts date from StringType to DateType
- Removes null rows for critical columns (symbol, date, close)
- Adds 8 derived financial columns using PySpark Window functions:

| Derived Column | Formula | Business Meaning |
|---|---|---|
| daily_range | high - low | How much stock moved that day |
| prev_close | LAG(close, 1) | Previous day closing price |
| daily_return_pct | (close - prev_close) / prev_close × 100 | Daily % gain or loss |
| mov_avg_7 | AVG(close) over last 7 days | Short term price trend |
| mov_avg_20 | AVG(close) over last 20 days | Long term price trend |
| is_bullish | 1 if close > open else 0 | Was it a positive day? |
| prev_volume | LAG(volume, 1) | Previous day volume |
| is_high_volume | 1 if volume > 1.5x prev_volume | Unusually high trading activity |

### Gold Layer — 4 Business Analytics Tables

**1. Stock Performance Summary**
- One row per stock
- Metrics: min/max/avg price, avg daily return %, volatility, bullish days, bullish %
- Ordered by best performing stock first

**2. Monthly Returns Analysis**
- One row per stock per month
- Metrics: avg monthly close, total monthly return %, monthly high/low, total volume
- Used for monthly returns heatmap in Tableau

**3. Volatility Analysis**
- One row per stock
- Metrics: volatility (std dev), max single day gain, max single day loss
- Days with >2% gain and days with >2% loss
- Used for risk-return scatter plot in Tableau

**4. Moving Average Crossover Signals**
- One row per stock per day
- Classifies each day as Bullish Signal / Bearish Signal / Neutral
- Based on 7-day MA vs 20-day MA crossover — real trading indicator

## Delta Lake Features Used

| Feature | How Used |
|---|---|
| ACID Transactions | Safe writes to Bronze/Silver/Gold layers |
| StructType Schema | Explicit schema enforcement at Bronze ingestion |
| Time Travel | Query Silver table at version 0 for auditing |
| Table History | Track all operations on Silver table |
| MERGE (Upsert) | Simulate incremental daily stock data loads |
| overwriteSchema | Allow schema evolution when changing data types |


## Key Business Insights Found

- **RELIANCE.BSE** was the best performer — lowest avg daily decline of -0.09%
- **INFY.BSE** was the most volatile — highest std deviation of 2.22
- **TCS.BSE** had the highest single day gain at 6.7%
- **WIPRO.BSE** had the lowest bullish day % at 31.31%
- **RELIANCE.BSE** had the highest bullish day % at 45.45% — most resilient stock
- **April 2026** was the best month across most stocks
- All 5 stocks showed bearish overall trend — reflecting broader Indian market decline in this period
- Moving average crossover signals confirmed bearish momentum for IT stocks (TCS, INFY) throughout the period


## Tableau Dashboard

**Live Dashboard:** https://public.tableau.com/app/profile/aniket.sonmali5355/viz/IndianStockMarketAnalyticsDashboard_17832556024540/IndianStockMarketAnalyticsDashboard?publish=yes

### Dashboard contains 4 charts:
1. **Stock Price Trend** — Line chart showing 100-day price movement for all 5 stocks
2. **Stock Performance Comparison** — Bar chart comparing avg daily return % colored by bullish %
3. **Risk Vs Return Analysis** — Scatter plot showing volatility vs max single day gain per stock
4. **Monthly Returns Heatmap** — Color-coded heatmap (blue=positive, orange=negative) by stock and month

## How to Run This Project

### Prerequisites
- Databricks Free Edition account — community.cloud.databricks.com
- Alpha Vantage free API key — alphavantage.co
- Tableau Public free account — public.tableau.com

### Steps
**1 — Get API key:**
- Go to alphavantage.co
- Register for free API key (no credit card needed)
- Free tier: 25 API calls per day

**2 — Set up Databricks:**
- Create Databricks Free Edition account
- Create Unity Catalog Volumes:
```sql
CREATE VOLUME IF NOT EXISTS workspace.default.stocks_raw_data;
CREATE VOLUME IF NOT EXISTS workspace.default.bronze;
CREATE VOLUME IF NOT EXISTS workspace.default.silver;
CREATE VOLUME IF NOT EXISTS workspace.default.gold;
```

**3 — Configure API key:**
- Open `01_API_Config.py`
- Replace `'your_api_key_here'` with your actual API key

**4 — Run notebooks in order:**
01_API_Config.py              → sets up config variables
02_Bronze_Ingestion.py        → fetches API data, saves Bronze Delta table
03_Silver_Transformation.py   → cleans and enriches data, saves Silver Delta table
04_Gold_Analytics.py          → runs Spark SQL, saves 4 Gold Delta tables

**5 — Export and visualize:**
- Export Gold tables as CSV from Databricks
- Connect to Tableau Public
- Build dashboard

### Important Notes
- Free API tier allows 25 calls/day — fetch 5 stocks = 5 calls
- Save JSON responses to Volume immediately after fetching
- DBFS is disabled in Databricks Free Edition — use Unity Catalog Volumes
- Use `overwriteSchema=true` when changing Delta table schema
