# Databricks notebook source
# MAGIC %run ./01_API_Config

# COMMAND ----------

import json
import os
import time
import requests
from datetime import date
from pyspark.sql import functions as F

today = str(date.today())
folder = f"{RAW_PATH}/ingest_date={today}"

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.stocks_raw_data")
os.makedirs(folder, exist_ok=True)

# COMMAND ----------

# STEP 1: call the API and save the raw JSON files (one per stock)

for symbol in STOCKS:
    file_path = f"{folder}/{symbol.replace('.', '_')}.json"

    # Already downloaded today? Don't waste an API call.
    if os.path.exists(file_path):
        print(f"{symbol}: already downloaded today, skipping")
        continue

    params = {"function": "TIME_SERIES_DAILY", "symbol": symbol,
              "outputsize": "compact", "apikey": API_KEY}
    data = requests.get(BASE_URL, params=params, timeout=30).json()

    # A good response has price data. Otherwise it is an error or limit message.
    if "Time Series (Daily)" not in data:
        print(f"{symbol}: no data returned -> {data}")
        break                       # most likely the daily limit is reached

    with open(file_path, "w") as f:
        json.dump(data, f)
    print(f"{symbol}: saved")
    time.sleep(5)                  # small gap between API calls

# COMMAND ----------

os.listdir(folder)

# COMMAND ----------

# STEP 2: read the saved files and build a table of rows

rows = []
for file_name in os.listdir(folder):
    with open(f"{folder}/{file_name}") as f:
        data = json.load(f)

    symbol = data["Meta Data"]["2. Symbol"]
    for trade_date, v in data["Time Series (Daily)"].items():
        rows.append((symbol, trade_date, v["1. open"], v["2. high"],
                     v["3. low"], v["4. close"], v["5. volume"], file_name))

new_df = (spark.createDataFrame(
              rows,
              "symbol string, trade_date string, open string, high string, "
              "low string, close string, volume string, source_file string")
          .withColumn("ingested_at", F.current_timestamp()))

print(f"Rows read: {new_df.count()}")

# COMMAND ----------

# STEP 3: load into the Bronze Delta table (insert new rows, update existing ones)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        symbol STRING, trade_date STRING, open STRING, high STRING,
        low STRING, close STRING, volume STRING,
        source_file STRING, ingested_at TIMESTAMP)
""")

new_df.createOrReplaceTempView("new_data")

spark.sql(f"""
    MERGE INTO {BRONZE_TABLE} t
    USING new_data s
    ON t.symbol = s.symbol AND t.trade_date = s.trade_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# Quick check
display(spark.sql(f"""
    SELECT symbol, COUNT(*) AS days, MIN(trade_date) AS first_day, MAX(trade_date) AS last_day
    FROM {BRONZE_TABLE}
    GROUP BY symbol
"""))

# COMMAND ----------

# Stop the pipeline if there is nothing to load
if len(rows) == 0:
    raise ValueError("No data was downloaded today. Check the API key / daily limit.")