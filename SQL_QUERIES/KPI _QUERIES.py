# Databricks notebook source
# MAGIC %md
# MAGIC # Busiest hour of the day (demand pattern)
# MAGIC # Goal: When are taxis most used in NYC?

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG nyc_project;
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dt.hour,
# MAGIC   COUNT(*) AS total_trips
# MAGIC FROM fact_trips ft
# MAGIC JOIN dim_time dt
# MAGIC   ON ft.pickup_timestamp = dt.pickup_timestamp
# MAGIC GROUP BY dt.hour
# MAGIC ORDER BY total_trips DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Top 10 Pickup Zones

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dl.zone_name,
# MAGIC   dl.borough,
# MAGIC   COUNT(*) AS total_trips
# MAGIC FROM fact_trips ft
# MAGIC JOIN dim_location dl
# MAGIC   ON ft.pickup_location_id = dl.location_id
# MAGIC GROUP BY dl.zone_name, dl.borough
# MAGIC ORDER BY total_trips DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Revenue by Borough

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dl.borough,
# MAGIC   SUM(ft.total_amount) AS total_revenue
# MAGIC FROM fact_trips ft
# MAGIC JOIN dim_location dl
# MAGIC   ON ft.pickup_location_id = dl.location_id
# MAGIC GROUP BY dl.borough
# MAGIC ORDER BY total_revenue DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Average Fare by Payment Type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dp.payment_name,
# MAGIC   ROUND(AVG(ft.total_amount),2) AS avg_fare
# MAGIC FROM fact_trips ft
# MAGIC JOIN dim_payment dp
# MAGIC   ON ft.payment_type = dp.payment_type
# MAGIC GROUP BY dp.payment_name
# MAGIC ORDER BY avg_fare DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Trips Trend Over Time (Monthly)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dt.year,
# MAGIC   dt.month,
# MAGIC   COUNT(*) AS total_trips
# MAGIC FROM fact_trips ft
# MAGIC JOIN dim_time dt
# MAGIC   ON ft.pickup_timestamp = dt.pickup_timestamp
# MAGIC WHERE dt.year = 2025
# MAGIC   AND dt.month <= 5
# MAGIC GROUP BY dt.year, dt.month
# MAGIC ORDER BY dt.month;
# MAGIC