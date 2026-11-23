# Databricks notebook source
# Databricks notebook source
# Settings used by the other notebooks

# Where raw API files and tables live
RAW_PATH = "/Volumes/workspace/default/stocks_raw_data/alphavantage/daily"
BRONZE_TABLE = "workspace.default.bronze_stock_daily"
SILVER_TABLE = "workspace.default.silver_stock_daily"
QUARANTINE_TABLE = "workspace.default.quarantine_stock_daily"

GOLD_PERFORMANCE = "workspace.default.gold_stock_performance"
GOLD_MONTHLY = "workspace.default.gold_monthly_returns"
GOLD_VOLATILITY = "workspace.default.gold_volatility_analysis"
GOLD_MA_SIGNALS = "workspace.default.gold_ma_signals"

# CSV exports for Tableau
EXPORT_PATH = "/Volumes/workspace/default/stocks_exports"

# API settings
BASE_URL = "https://www.alphavantage.co/query"
STOCKS = ["TCS.BSE", "RELIANCE.BSE", "INFY.BSE", "HDFCBANK.BSE", "WIPRO.BSE"]

# API key: paste it into the box at the top of the notebook (never hardcode it)
API_KEY = "LZAHJXXR2GGER7XF"
