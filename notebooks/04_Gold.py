# Databricks notebook source
# MAGIC %run ./01_API_Config

# COMMAND ----------

# Make the Silver table available to SQL under a short name
spark.table(SILVER_TABLE).createOrReplaceTempView("silver_stocks")


# COMMAND ----------

# Gold 1: overall performance of each stock

stock_performance = spark.sql("""
    SELECT
        symbol,
        ROUND(MIN(close), 2)                      AS min_price,
        ROUND(MAX(close), 2)                      AS max_price,
        ROUND(AVG(close), 2)                      AS avg_price,
        ROUND(AVG(daily_return_pct), 4)           AS avg_daily_return_pct,
        ROUND(STDDEV(daily_return_pct), 4)        AS volatility,
        SUM(is_bullish)                           AS bullish_days,
        COUNT(*)                                  AS total_trading_days,
        ROUND(SUM(is_bullish) * 100.0 / COUNT(*), 2) AS bullish_pct
    FROM silver_stocks
    WHERE daily_return_pct IS NOT NULL
    GROUP BY symbol
    ORDER BY avg_daily_return_pct DESC
""")

(stock_performance.write.format("delta").mode("overwrite")
                  .option("overwriteSchema", "true").saveAsTable(GOLD_PERFORMANCE))
display(stock_performance)

# COMMAND ----------

# COMMAND ----------

# Gold 2: month-wise summary of each stock
# Note: monthly_total_return_pct is the simple sum of the daily returns (an approximation)

monthly_returns = spark.sql("""
    SELECT
        symbol,
        DATE_FORMAT(trade_date, 'yyyy-MM')    AS month,
        ROUND(AVG(close), 2)                  AS avg_monthly_close,
        ROUND(SUM(daily_return_pct), 4)       AS monthly_total_return_pct,
        ROUND(MAX(close), 2)                  AS monthly_high,
        ROUND(MIN(close), 2)                  AS monthly_low,
        SUM(volume)                           AS total_monthly_volume
    FROM silver_stocks
    WHERE daily_return_pct IS NOT NULL
    GROUP BY symbol, DATE_FORMAT(trade_date, 'yyyy-MM')
    ORDER BY symbol, month
""")

(monthly_returns.write.format("delta").mode("overwrite")
                .option("overwriteSchema", "true").saveAsTable(GOLD_MONTHLY))
display(monthly_returns)

# COMMAND ----------

# COMMAND ----------

# Gold 3: how risky (volatile) is each stock

volatility_analysis = spark.sql("""
    SELECT
        symbol,
        ROUND(STDDEV(daily_return_pct), 4)    AS volatility,
        ROUND(MAX(daily_return_pct), 4)       AS max_single_day_gain,
        ROUND(MIN(daily_return_pct), 4)       AS max_single_day_loss,
        COUNT(CASE WHEN daily_return_pct > 2  THEN 1 END) AS days_up_over_2pct,
        COUNT(CASE WHEN daily_return_pct < -2 THEN 1 END) AS days_down_over_2pct
    FROM silver_stocks
    WHERE daily_return_pct IS NOT NULL
    GROUP BY symbol
    ORDER BY volatility DESC
""")

(volatility_analysis.write.format("delta").mode("overwrite")
                    .option("overwriteSchema", "true").saveAsTable(GOLD_VOLATILITY))
display(volatility_analysis)

# COMMAND ----------

# COMMAND ----------

# Gold 4: moving average signal for each stock and day
# 7-day average above 20-day average = upward trend (bullish), below = downward trend (bearish)

mov_avg_signals = spark.sql("""
    SELECT
        symbol, trade_date, close, mov_avg_7, mov_avg_20,
        CASE
            WHEN mov_avg_7 > mov_avg_20 THEN 'Bullish Signal'
            WHEN mov_avg_7 < mov_avg_20 THEN 'Bearish Signal'
            ELSE 'Neutral'
        END AS ma_signal,
        daily_return_pct
    FROM silver_stocks
    WHERE mov_avg_7 IS NOT NULL AND mov_avg_20 IS NOT NULL
    ORDER BY symbol, trade_date
""")

(mov_avg_signals.write.format("delta").mode("overwrite")
                .option("overwriteSchema", "true").saveAsTable(GOLD_MA_SIGNALS))
display(mov_avg_signals)

# COMMAND ----------

# COMMAND ----------

# Export the Gold tables as CSV files for Tableau

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.stocks_exports")

exports = {
    "stock_performance":   GOLD_PERFORMANCE,
    "monthly_returns":     GOLD_MONTHLY,
    "volatility_analysis": GOLD_VOLATILITY,
    "mov_avg_signals":     GOLD_MA_SIGNALS,
}

for file_name, table_name in exports.items():
    spark.table(table_name).toPandas().to_csv(f"{EXPORT_PATH}/{file_name}.csv", index=False)
    print(f"Exported {file_name}.csv")