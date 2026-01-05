# Databricks notebook source
# MAGIC %md
# MAGIC ###Silver Layer Data Processing

# COMMAND ----------

# MAGIC %run /Workspace/Users/abhijitawscert2023@gmail.com/util_mod

# COMMAND ----------


tables = ['customers', 'geolocation', 'order_items', 'order_payments', 'order_reviews', 'orders', 'products', 'sellers']
target_db = 'silver_db'
upsert_latest_partition_data(tables, target_db)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")