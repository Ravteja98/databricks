# Databricks notebook source
catalog = "nyc_project"

zone_path = "/Volumes/nyc_project/source_data/raw/taxi_zone_lookup/"

df_zone = spark.read.option("header", "true").csv(zone_path)

display(df_zone)


# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F

df_dim_location = (df_zone
    .withColumnRenamed("LocationID", "location_id")
    .withColumnRenamed("Zone", "zone_name")
    .withColumnRenamed("Borough", "borough")
    .withColumnRenamed("service_zone", "service_zone")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("ingested_at", F.current_timestamp())
)

display(df_dim_location)


# COMMAND ----------

df_trips = spark.table(f"{catalog}.silver.yellow_trips_clean")

df_trips.select("PULocationID").distinct().count()
df_dim_location.select("location_id").distinct().count()


# COMMAND ----------

df_dim_location.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{catalog}.gold.dim_location")


# COMMAND ----------

spark.table(f"{catalog}.gold.dim_location").show(10)
