# Databricks notebook source
catalog = 'nyc_project'
schema = 'gold'



df_gold_stage = spark.table(f"{catalog}.silver.yellow_trips_clean")
df_gold_stage.show(5)


# COMMAND ----------

from pyspark.sql import Row

catalog = "nyc_project"

rate_data = [
    Row(rate_id=1, rate_name="Standard"),
    Row(rate_id=2, rate_name="JFK"),
    Row(rate_id=3, rate_name="Newark"),
    Row(rate_id=4, rate_name="Nassau or Westchester"),
    Row(rate_id=5, rate_name="Negotiated Fare"),
    Row(rate_id=6, rate_name="Group Ride"),
    Row(rate_id=99, rate_name="Unknown")
]

df_dim_rate = spark.createDataFrame(rate_data)
display(df_dim_rate)


# COMMAND ----------

df_gold_stage = spark.table(f"{catalog}.silver.yellow_trips_clean")
df_gold_stage.select("RatecodeID").distinct().orderBy("RatecodeID").show()


# COMMAND ----------

catalog = "nyc_project"

df_dim_rate.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_rate")


# COMMAND ----------

spark.table(f"{catalog}.gold.dim_rate").show()
