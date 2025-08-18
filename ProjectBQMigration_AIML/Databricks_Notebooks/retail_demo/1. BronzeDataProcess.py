# Databricks notebook source
# MAGIC %md
# MAGIC ### Raw data load to Bronze Layer - Bronze Layer Data Processing

# COMMAND ----------

# MAGIC %run /Workspace/Users/datamatricwithabhijit@gmail.com/util_mod

# COMMAND ----------

folders = [
    # '/mnt/databricks/gcsmount_ecommerce/category_name/'
    'dbfs:/mnt/databricks/gcsmount_retails/customers/customers.csv',
    'dbfs:/mnt/databricks/gcsmount_retails/inventory/inventory.csv',
    'dbfs:/mnt/databricks/gcsmount_retails/order_items/order_items.csv',
    'dbfs:/mnt/databricks/gcsmount_retails/orders/orders.csv',
    'dbfs:/mnt/databricks/gcsmount_retails/products/products.csv',
    'dbfs:/mnt/databricks/gcsmount_retails/stores/stores.csv',
]

load_raw_data(folders)


# COMMAND ----------

dbutils.notebook.exit("SUCCESS")