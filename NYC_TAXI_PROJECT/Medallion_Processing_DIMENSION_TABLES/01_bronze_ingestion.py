# Databricks notebook source
spark.sql("USE CATALOG nyc_project")
spark.sql("USE SCHEMA bronze")

raw_path = "/Volumes/nyc_project/source_data/raw/data/2025/"

trips_raw_df = spark.read.option('header', "true").parquet(raw_path)

display(trips_raw_df.limit(10))


# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# ADDED THE BELOW COLUMNS FOR AUDIT 
trips_raw_df = (
    trips_raw_df
      .withColumn("_source_file", F.col("_metadata.file_path"))
      .withColumn("ingested_at", F.current_timestamp())
)


# COMMAND ----------

display(trips_raw_df.limit(10))

# COMMAND ----------

trips_raw_df.count()

# COMMAND ----------

trips_raw_df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("yellow_trips_bronze")


# COMMAND ----------

# Checking the source file distribution
%sql
SELECT 
  _source_file,
  COUNT(*) AS rows_per_file
FROM yellow_trips_bronze
GROUP BY _source_file
ORDER BY rows_per_file DESC;


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM nyc_project.bronze.yellow_trips_bronze
# MAGIC LIMIT 10;
# MAGIC