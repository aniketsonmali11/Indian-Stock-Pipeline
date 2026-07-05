# Databricks notebook source
API_KEY = 'YOUR_API_KEY'
BASE_URL = 'https://www.alphavantage.co/query'
STOCKS = ['TCS.BSE','RELIANCE.BSE','INFY.BSE','HDFCBANK.BSE','WIPRO.BSE']

# COMMAND ----------

import os
print(os.listdir('/Volumes/workspace/default/stocks_raw_data/'))

# COMMAND ----------

