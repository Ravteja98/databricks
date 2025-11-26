# Databricks notebook source
catalog = 'nyc_project'
schema = 'gold'

# COMMAND ----------

df_gold_stage = spark.table(f'{catalog}.silver.yellow_trips_clean')
df_gold_stage.show(5)

# COMMAND ----------

# VENDOR_DIM_TABLE

from pyspark.sql import Row

vendor_data = [
    Row(VendorID=1, vendor_name="Creative Mobile Technologies"),
    Row(VendorID=2, vendor_name="Curb Mobility"),
    Row(VendorID=6, vendor_name="Myle Technologies"),
    Row(VendorID=7, vendor_name="Helix")
]

df_dim_vendor = spark.createDataFrame(vendor_data)
display(df_dim_vendor)


# COMMAND ----------

df_gold_stage.select("VendorID").distinct().orderBy("VendorID").show()


# COMMAND ----------

df_dim_vendor.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_vendor")


# COMMAND ----------

spark.table(f"{catalog}.gold.dim_vendor").show()
