# Databricks notebook source
# MAGIC %run /Workspace/Users/aniketsonmali11@gmail.com/NSE_Stock_Pipeline/01_API_Config
# MAGIC

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

#function to fetch stock data from API
def fetch_stock_data(symbol):
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'outputsize': 'compact',
        'apikey': API_KEY       # directly using from config
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    return data

# COMMAND ----------

import json
import os
import requests
from datetime import datetime

stocks_to_fetch = ['TCS.BSE', 'INFY.BSE', 'HDFCBANK.BSE', 'WIPRO.BSE']

for symbol in stocks_to_fetch:
    raw_data = fetch_stock_data(symbol)
    filename = f"/Volumes/workspace/default/raw_data/{symbol.replace('.', '_')}_raw.json"
    with open(filename, 'w') as f:
        json.dump(raw_data, f)
    print(f"Saved {symbol}")

# COMMAND ----------

for filename in os.listdir('/Volumes/workspace/default/raw_data/'):
    with open(f'/Volumes/workspace/default/raw_data/{filename}', 'r') as f:
        data = json.load(f)
    keys = list(data.keys())
    days = len(data.get('Time Series (Daily)', {}))
    print(f"{filename}: keys={keys}, days={days}")

# COMMAND ----------

# Delete corrupted files
bad_stocks = ['INFY.BSE', 'HDFCBANK.BSE', 'WIPRO.BSE']

for symbol in STOCKS:
    filename = f"/Volumes/workspace/default/raw_data/{symbol.replace('.', '_')}_raw.json"
    os.remove(filename)
    print(f"Deleted {filename}")

# Verify only good files remain
print(os.listdir('/Volumes/workspace/default/raw_data/'))
# Should show only: RELIANCE_BSE_raw.json, TCS_BSE_raw.json

# COMMAND ----------

print(os.listdir('/Volumes/workspace/default/stocks_raw_data/'))

# COMMAND ----------

for filename in os.listdir('/Volumes/workspace/default/stocks_raw_data/'):
    with open(f'/Volumes/workspace/default/stocks_raw_data/{filename}', 'r') as f:
        data = json.load(f)
    days = len(data.get('Time Series (Daily)', {}))
    print(f"{filename}: days={days}")

# COMMAND ----------

all_rows = []

for symbol in STOCKS:
    filename = f"/Volumes/workspace/default/stocks_raw_data/{symbol.replace('.', '_')}_raw.json"
    with open(filename, 'r') as f:
        raw_data = json.load(f)
    
    time_series = raw_data.get('Time Series (Daily)', {})
    for date_str, values in time_series.items():
        all_rows.append({
            'symbol':      symbol,
            'date':        date_str,
            'open':        float(values['1. open']),
            'high':        float(values['2. high']),
            'low':         float(values['3. low']),
            'close':       float(values['4. close']),
            'volume':      int(values['5. volume']),
            'ingested_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

print(f'Total rows: {len(all_rows)}')

# COMMAND ----------

schema = StructType([
    StructField('symbol',      StringType(),  True),
    StructField('date',        StringType(),  True),
    StructField('open',        DoubleType(),  True),
    StructField('high',        DoubleType(),  True),
    StructField('low',         DoubleType(),  True),
    StructField('close',       DoubleType(),  True),
    StructField('volume',      LongType(),    True),
    StructField('ingested_at', StringType(),  True),
])

bronze_df = spark.createDataFrame(all_rows, schema=schema)
bronze_df.printSchema()
bronze_df.show(5)

# COMMAND ----------

bronze_df.write.format('delta')\
         .mode('overwrite')\
         .option('overwriteSchema', 'true')\
         .save('/Volumes/workspace/default/bronze/nse_stocks_raw')

print(f'Bronze table rows: {bronze_df.count()}')

# COMMAND ----------

bronze_df.printSchema()
bronze_df.show(5)
