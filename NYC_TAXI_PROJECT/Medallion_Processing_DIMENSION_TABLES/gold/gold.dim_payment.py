# Databricks notebook source
catalog = 'nyc_project'
schema = 'gold'



df_gold_stage = spark.table(f"{catalog}.silver.yellow_trips_clean")
df_gold_stage.show(5)


# COMMAND ----------

from pyspark.sql import Row

payment_data = [
    Row(payment_type=0, payment_name="Flex Fare Trip"),
    Row(payment_type=1, payment_name="Credit Card"),
    Row(payment_type=2, payment_name="Cash"),
    Row(payment_type=3, payment_name="No Charge"),
    Row(payment_type=4, payment_name="Dispute"),
    Row(payment_type=5, payment_name="Unknown"),
    Row(payment_type=6, payment_name="Voided Trip")
]

df_dim_payment = spark.createDataFrame(payment_data)
display(df_dim_payment)


# COMMAND ----------

df_gold_stage.select("payment_type").distinct().orderBy("payment_type").show()


# COMMAND ----------

catalog = "nyc_project"

df_dim_payment.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.gold.dim_payment")


# COMMAND ----------

spark.table(f"{catalog}.gold.dim_payment").show()
