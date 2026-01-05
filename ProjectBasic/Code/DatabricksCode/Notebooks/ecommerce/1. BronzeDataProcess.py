# Databricks notebook source
# MAGIC %md
# MAGIC ### Raw data load to Bronze Layer - Bronze Layer Data Processing

# COMMAND ----------

# MAGIC %run /Workspace/Users/abhijitawscert2023@gmail.com/util_mod

# COMMAND ----------

folders = [
    # '/mnt/databricks/gcsmount_ecommerce/category_name/'
    'dbfs:/mnt/databricks/gcsmount_ecommerce/customers/',
    'dbfs:/mnt/databricks/gcsmount_ecommerce/geolocation/',
    'dbfs:/mnt/databricks/gcsmount_ecommerce/order_items/',
    'dbfs:/mnt/databricks/gcsmount_ecommerce/order_payments/',
    'dbfs:/mnt/databricks/gcsmount_ecommerce/order_reviews/',
    'dbfs:/mnt/databricks/gcsmount_ecommerce/orders/',
    'dbfs:/mnt/databricks/gcsmount_ecommerce/products/',
    'dbfs:/mnt/databricks/gcsmount_ecommerce/sellers/'
]

load_raw_data(folders)

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")