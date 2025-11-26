# Databricks notebook source
catalog = "nyc_project"
schema = 'Silver'

# COMMAND ----------

df_bronze = spark.table(f'{catalog}.bronze.yellow_trips_bronze')
df_bronze.show(10)

# COMMAND ----------

cols_to_keep = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "RatecodeID",
    "_source_file",
    "ingested_at"
]

df_silver_stage = df_bronze.select(*cols_to_keep)

display(df_silver_stage)


# COMMAND ----------

df_silver_stage.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col

df_silver_clean = df_silver_stage.filter(
    (col("fare_amount") > 0) &
    (col("fare_amount") < 300) &
    (col("trip_distance") > 0) &
    (col("trip_distance") < 150) &
    (col("VendorID").isNotNull()) &
    (col("PULocationID").isNotNull()) &
    (col("DOLocationID").isNotNull())
)

display(df_silver_clean)


# COMMAND ----------

print("Bronze count:", df_silver_stage.count())
print("Silver clean count:", df_silver_clean.count())


# COMMAND ----------

df_silver_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.silver.yellow_trips_clean")


# COMMAND ----------

spark.table(f"{catalog}.silver.yellow_trips_clean").count()
