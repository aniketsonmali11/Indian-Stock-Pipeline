# Databricks notebook source
# MAGIC %run ./01_API_Config
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# STEP 1: read Bronze and convert the text columns to proper types
# try_cast gives NULL (instead of failing) when a value cannot be converted

typed_df = spark.sql(f"""
    SELECT symbol,
           try_cast(trade_date AS DATE) AS trade_date,
           try_cast(open   AS DOUBLE)   AS open,
           try_cast(high   AS DOUBLE)   AS high,
           try_cast(low    AS DOUBLE)   AS low,
           try_cast(close  AS DOUBLE)   AS close,
           try_cast(volume AS BIGINT)   AS volume,
           source_file
    FROM {BRONZE_TABLE}
""")

# COMMAND ----------


# STEP 2: data quality checks
# Each row gets a reject_reason. If the reject reason empty, the row is good.

checked_df = typed_df.withColumn("reject_reason", F.expr("""
    CASE
        WHEN symbol IS NULL OR trade_date IS NULL OR open IS NULL OR high IS NULL
             OR low IS NULL OR close IS NULL OR volume IS NULL
            THEN 'missing or invalid value'
        WHEN open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
            THEN 'price not positive'
        WHEN volume < 0
            THEN 'negative volume'
        WHEN high < low
            THEN 'high is below low'
        WHEN high < open OR high < close OR low > open OR low > close
            THEN 'open/close outside the high-low range'
    END
"""))

good_df = checked_df.filter("reject_reason IS NULL").drop("reject_reason")
bad_df = checked_df.filter("reject_reason IS NOT NULL")

# Rejected rows are kept in a separate table (rebuilt on every run)
(bad_df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(QUARANTINE_TABLE))

print(f"Good rows: {good_df.count()}")
print(f"Rejected rows: {bad_df.count()}")

# COMMAND ----------

# COMMAND ----------



# STEP 3: transformations (same logic as before)

# Window for each stock: rows of one symbol, in date order
w_stock = Window.partitionBy("symbol").orderBy("trade_date")

silver_df = good_df

# 1. Daily range: highest price minus lowest price of the day
silver_df = silver_df.withColumn("daily_range", F.round(F.col("high") - F.col("low"), 2))

# 2. Previous close: close of the previous trading day (helper for the return)
silver_df = silver_df.withColumn("prev_close", F.lag("close", 1).over(w_stock))

# 3. Daily return %: (today's close - yesterday's close) / yesterday's close * 100
silver_df = silver_df.withColumn(
    "daily_return_pct",
    F.round((F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100, 4))

# Helper: day number of each row within its stock (1, 2, 3, ...)
silver_df = silver_df.withColumn("day_number", F.row_number().over(w_stock))

# 4. 7-day moving average (only once 7 days of data exist, otherwise NULL)
silver_df = silver_df.withColumn(
    "mov_avg_7",
    F.when(F.col("day_number") >= 7,
           F.round(F.avg("close").over(w_stock.rowsBetween(-6, 0)), 2)))

# 5. 20-day moving average (only once 20 days of data exist, otherwise NULL)
silver_df = silver_df.withColumn(
    "mov_avg_20",
    F.when(F.col("day_number") >= 20,
           F.round(F.avg("close").over(w_stock.rowsBetween(-19, 0)), 2)))

# 6. Is bullish: 1 if the stock closed higher than it opened, else 0
silver_df = silver_df.withColumn("is_bullish", F.when(F.col("close") > F.col("open"), 1).otherwise(0))

# 7. Previous volume: volume of the previous trading day (helper)
silver_df = silver_df.withColumn("prev_volume", F.lag("volume", 1).over(w_stock))

# 8. Is high volume: 1 if today's volume is more than 1.5x yesterday's, else 0
silver_df = silver_df.withColumn(
    "is_high_volume",
    F.when(F.col("volume") > F.col("prev_volume") * 1.5, 1).otherwise(0))

silver_df = silver_df.drop("day_number")

# COMMAND ----------

# COMMAND ----------

# STEP 4: save the Silver table
# Silver is rebuilt from Bronze on every run, so re-running gives the same result.

(silver_df.write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true").saveAsTable(SILVER_TABLE))

# COMMAND ----------

# COMMAND ----------

# Quick checks
display(spark.sql(f"""
    SELECT symbol, COUNT(*) AS days, MIN(trade_date) AS first_day, MAX(trade_date) AS last_day
    FROM {SILVER_TABLE}
    GROUP BY symbol
    ORDER BY symbol
"""))

spark.table(SILVER_TABLE).printSchema()