# Databricks notebook source
API_KEY = 'LZAHJXXR2GGER7XF'
BASE_URL = 'https://www.alphavantage.co/query'
STOCKS = ['TCS.BSE','RELIANCE.BSE','INFY.BSE','HDFCBANK.BSE','WIPRO.BSE']

# COMMAND ----------

import os
print(os.listdir('/Volumes/workspace/default/stocks_raw_data/'))

# COMMAND ----------

