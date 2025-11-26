# Databricks notebook source
catalog = "nyc_project"

df = spark.table(f"{catalog}.silver.yellow_trips_clean")

display(df.limit(5))


# COMMAND ----------

catalog = "nyc_project"

from pyspark.sql.functions import col


df_silver = spark.table(f"{catalog}.silver.yellow_trips_clean")

df_fact_trips = df_silver.select(
    "VendorID",
    "payment_type",
    "RatecodeID",
    col("tpep_pickup_datetime").alias("pickup_timestamp"),
    col("PULocationID").alias("pickup_location_id"),
    col("DOLocationID").alias("dropoff_location_id"),
    "trip_distance",
    "fare_amount",
    "total_amount",
    "_source_file",
    "ingested_at"
)

display(df_fact_trips.limit(10))


# COMMAND ----------

df_fact_trips.count()


# COMMAND ----------

df_fact_trips.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.fact_trips")


# COMMAND ----------

spark.table(f"{catalog}.gold.fact_trips").count()
