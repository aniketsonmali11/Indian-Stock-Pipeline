# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import *
bronze = spark.read.format('delta').load('/Volumes/workspace/default/bronze/nse_stocks_raw')


# COMMAND ----------

bronze = spark.read.format('delta')\
              .load('/Volumes/workspace/default/bronze/nse_stocks_raw')

bronze.printSchema()
bronze.show(5)

# COMMAND ----------

silver_df = bronze\
    .withColumn('date', to_date(col('date'), 'yyyy-MM-dd'))\
    .dropna(subset=['symbol', 'date', 'close'])
# No casting needed — already correct types from Bronze StructType!

# COMMAND ----------

# MAGIC %md
# MAGIC TRANFORMATIONS
# MAGIC

# COMMAND ----------

w_stock = Window.partitionBy('symbol').orderBy('date') # Window for each stock


# COMMAND ----------

# Transformation 1 — Daily Range:
# Subtracts lowest price from highest price of that day
# Shows how much the stock moved that day
silver_df = silver_df.withColumn('daily_range', round(col('high') - col('low'), 2))
silver_df.show(5)

# COMMAND ----------

# Transformation 2 — Previous Close:
# lag('close', 1) = take the close value from 1 row above (previous day)
# This is a helper column — needed to calculate daily return % in next step
silver_df = silver_df.withColumn('prev_close', lag('close', 1).over(w_stock))
silver_df.show(5)

# COMMAND ----------

# Transformation 3 — Daily Return %
# Formula: (today's close - yesterday's close) / yesterday's close × 100
# Shows how much % the stock gained or lost compared to previous day
# Positive = stock went up, Negative = stock went down
silver_df = silver_df.withColumn('daily_return_pct',
                                 round((col('close') - col('prev_close')) / col('prev_close') * 100, 4))
silver_df.show(5)

# COMMAND ----------

# Transformation 4 — 7-Day Moving Average:
# rowsBetween(-6, 0) = look at current row (0) and 6 rows behind (-6) = 7 days total
# Takes average of closing price over last 7 trading days
# Smooths out daily fluctuations — shows short term trend
silver_df = silver_df.withColumn('mov_avg_7',round(avg('close').over(w_stock.rowsBetween(-6, 0)), 2))
silver_df.show(5)

# COMMAND ----------

silver_df = silver_df.drop('ma_7')
silver_df.printSchema()

# COMMAND ----------

# Transformation 5 — 20-Day Moving Average:
# Same as ma_7 but looks at last 20 trading days
# Shows longer term trend
# When ma_7 crosses above ma_20 = bullish signal (stock gaining momentum)
# When ma_7 crosses below ma_20 = bearish signal (stock losing momentum)
silver_df = silver_df.withColumn('mov_avg_20',
    round(avg('close').over(w_stock.rowsBetween(-19, 0)), 2))
silver_df.show(15)  

# COMMAND ----------

# Transformation 6 — Is Bullish:
# If closing price > opening price → stock went UP that day → 1 (bullish)
# If closing price < opening price → stock went DOWN that day → 0 (bearish)
# Simple flag — 1 or 0
silver_df = silver_df.withColumn('is_bullish', when(col('close') > col('open'), 1).otherwise(0))
silver_df.show(10)

# COMMAND ----------

# Transformation 7 — Previous Volume:
# Same as prev_close but for volume
# Takes volume from previous trading day
# Helper column needed for is_high_volume calculation below
silver_df = silver_df.withColumn('prev_volume', lag('volume', 1).over(w_stock))
silver_df.show(10)

# COMMAND ----------

# Transformation 8 — Is High Volume:
# If today's volume > 1.5x yesterday's volume → unusually high trading activity → 1
# Otherwise → normal volume → 0
# High volume days are significant — usually means big news about the stock
silver_df = silver_df.withColumn('is_high_volume',
    when(col('volume') > col('prev_volume') * 1.5, 1).otherwise(0))
silver_df.show(10)

# COMMAND ----------

silver_df.printSchema()
print(f'Silver rows: {silver_df.count()}')
print(silver_df.columns)

# COMMAND ----------

# Saving the silver layer to delta
silver_df.write.format('delta')\
         .mode('overwrite')\
         .save('/Volumes/workspace/default/silver/nse_stocks_enriched')

print(f'Silver table rows: {silver_df.count()}')

# COMMAND ----------

# checking after saving delta 
verify_silver = spark.read.format('delta')\
                     .load('/Volumes/workspace/default/silver/nse_stocks_enriched')

print(f'Rows: {verify_silver.count()}')
verify_silver.show(5)
