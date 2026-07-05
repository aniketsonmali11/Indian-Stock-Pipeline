# Databricks notebook source
silver = spark.read.format('delta').load('/Volumes/workspace/default/silver/nse_stocks_enriched')

silver.createOrReplaceTempView('nse_stocks')

# COMMAND ----------

stock_performance = spark.sql('''
  SELECT
    symbol,
    ROUND(MIN(close), 2)                          AS min_price,
    ROUND(MAX(close), 2)                          AS max_price,
    ROUND(AVG(close), 2)                          AS avg_price,
    ROUND(AVG(daily_return_pct), 4)               AS avg_daily_return_pct,
    ROUND(STDDEV(daily_return_pct), 4)            AS volatility,
    SUM(is_bullish)                               AS bullish_days,
    COUNT(*)                                      AS total_trading_days,
    ROUND(SUM(is_bullish)*100.0/COUNT(*), 2)      AS bullish_pct
  FROM nse_stocks
  WHERE daily_return_pct IS NOT NULL
  GROUP BY symbol
  ORDER BY avg_daily_return_pct DESC
''')
stock_performance.write.format('delta').mode('overwrite').save('/Volumes/workspace/default/gold/stock_performance')
stock_performance.show()


# COMMAND ----------

monthly_returns = spark.sql('''
  SELECT
    symbol,
    DATE_FORMAT(date, 'yyyy-MM')          AS month,
    ROUND(AVG(close), 2)                  AS avg_monthly_close,
    ROUND(SUM(daily_return_pct), 4)       AS monthly_total_return_pct,
    ROUND(MAX(close), 2)                  AS monthly_high,
    ROUND(MIN(close), 2)                  AS monthly_low,
    SUM(volume)                           AS total_monthly_volume
  FROM nse_stocks
  WHERE daily_return_pct IS NOT NULL
  GROUP BY symbol, month
  ORDER BY symbol, month
''')
monthly_returns.write.format('delta').mode('overwrite').save('/Volumes/workspace/default/gold/monthly_returns')
monthly_returns.show(10)



# COMMAND ----------

volatility_analysis = spark.sql('''
  SELECT
    symbol,
    ROUND(STDDEV(daily_return_pct), 4)    AS volatility,
    ROUND(MAX(daily_return_pct), 4)       AS max_single_day_gain,
    ROUND(MIN(daily_return_pct), 4)       AS max_single_day_loss,
    COUNT(CASE WHEN daily_return_pct > 2  THEN 1 END) AS days_up_over_2pct,
    COUNT(CASE WHEN daily_return_pct < -2 THEN 1 END) AS days_down_over_2pct
  FROM nse_stocks
  WHERE daily_return_pct IS NOT NULL
  GROUP BY symbol
  ORDER BY volatility DESC
''')
volatility_analysis.write.format('delta').mode('overwrite').save('/Volumes/workspace/default/gold/volatility_analysis')
volatility_analysis.show()


# COMMAND ----------

mov_avg_signals = spark.sql('''
  SELECT
    symbol, date, close, mov_avg_7, mov_avg_20,
    CASE
      WHEN mov_avg_7 > mov_avg_20 THEN 'Bullish Signal'
      WHEN mov_avg_7 < mov_avg_20 THEN 'Bearish Signal'
      ELSE 'Neutral'
    END AS ma_signal,
    daily_return_pct
  FROM nse_stocks
  WHERE mov_avg_7 IS NOT NULL AND mov_avg_20 IS NOT NULL
  ORDER BY symbol, date
''')
mov_avg_signals.write.format('delta').mode('overwrite').save('/Volumes/workspace/default/gold/mov_avg_signals')
mov_avg_signals.show(10)


# COMMAND ----------

spark.read.format('delta').option('versionAsOf', 0)\
     .load('/Volumes/workspace/default/silver/nse_stocks_enriched').show(5)


# COMMAND ----------

from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, '/Volumes/workspace/default/silver/nse_stocks_enriched')
dt.history().show()


# COMMAND ----------

# Simulate fetching fresh data for today
new_data = spark.createDataFrame([
    ('TCS.BSE', '2025-01-01', 4100.0, 4150.0, 4080.0, 4120.0, 1200000)
], ['symbol','date','open','high','low','close','volume'])

dt.alias('existing').merge(
    new_data.alias('new'),
    'existing.symbol = new.symbol AND existing.date = new.date'
).whenNotMatchedInsertAll().execute()


# COMMAND ----------

stock_performance.toPandas().to_csv('/Volumes/workspace/default/gold/stock_performance.csv', index=False)
monthly_returns.toPandas().to_csv('/Volumes/workspace/default/gold/monthly_returns.csv', index=False)
volatility_analysis.toPandas().to_csv('/Volumes/workspace/default/gold/volatility_analysis.csv', index=False)
mov_avg_signals.toPandas().to_csv('/Volumes/workspace/default/gold/mov_avg_signals.csv', index=False)

print("All CSVs saved!")
