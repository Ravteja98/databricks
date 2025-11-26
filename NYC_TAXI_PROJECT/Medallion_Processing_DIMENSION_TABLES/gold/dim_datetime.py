# Databricks notebook source
catalog = 'nyc_project'
schema = 'gold'



df_gold_stage = spark.table(f"{catalog}.silver.yellow_trips_clean")
df_gold_stage.show(5)


# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql import functions as F

catalog = "nyc_project"

df = spark.table(f"{catalog}.silver.yellow_trips_clean")

display(df.select("tpep_pickup_datetime").limit(5))


# COMMAND ----------

df_dim_time = (
    df
    .select(col("tpep_pickup_datetime").alias("pickup_timestamp"))
    .dropna()
    .dropDuplicates()
    .withColumn("date", to_date("pickup_timestamp"))
    .withColumn("year", year("pickup_timestamp"))
    .withColumn("month", month("pickup_timestamp"))
    .withColumn("day", dayofmonth("pickup_timestamp"))
    .withColumn("hour", hour("pickup_timestamp"))
    .withColumn("day_name", date_format("pickup_timestamp", "EEEE"))
    .withColumn("week_number", weekofyear("pickup_timestamp"))
    .withColumn("is_weekend", when(dayofweek("pickup_timestamp").isin([1,7]), True).otherwise(False))
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("ingested_at", F.current_timestamp())
)
display(df_dim_time)


# COMMAND ----------

df_dim_time.count()


# COMMAND ----------

df_dim_time = df_dim_time \
    .withColumn("_source", lit("yellow_trips_clean")) \
    .withColumn("created_at", current_timestamp())


# COMMAND ----------

display(df_dim_time)

# COMMAND ----------

df_dim_time.drop("_source_file").write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.dim_time")

# COMMAND ----------

df_dim_time = df_dim_time.drop("_source_file", "ingested_at")


# COMMAND ----------

df_dim_time.printSchema()


# COMMAND ----------

df_dim_time.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_time")


# COMMAND ----------

spark.table(f"{catalog}.gold.dim_time").show(5)
spark.table(f"{catalog}.gold.dim_time").printSchema()


# COMMAND ----------

df_dim_time = df_dim_time.drop("ingested_at")


# COMMAND ----------

df_dim_time.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_time")


# COMMAND ----------

df_dim_time.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_time")


# COMMAND ----------

df_dim_time.show(5)