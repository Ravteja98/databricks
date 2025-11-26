# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create catalog if not exists nyc_project;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use catalog nyc_project;

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Schema if not exists nyc_project.bronze;
# MAGIC Create Schema if not exists nyc_project.silver;
# MAGIC Create Schema if not exists nyc_project.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW DATABASES from nyc_project;