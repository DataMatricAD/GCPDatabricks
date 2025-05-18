# Databricks notebook source
# MAGIC %md
# MAGIC ### GCS Databricks Mount

# COMMAND ----------

bucket_name = "e-commerce-raw-data"
mount_name = "gcsmount_ecommerce"
dbutils.fs.mount(
  source = f"gs://{bucket_name}",
  mount_point = f"/mnt/databricks/{mount_name}",
  extra_configs = {"fs.gs.project.id":"gcp-databricks-project-xxxxx"}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/databricks/gcsmount_ecommerce/customers/olist_customers_dataset.csv")

# COMMAND ----------

df_csv = spark.read.format("csv").option("header", "true").load("/mnt/databricks/gcsmount_ecommerce/customers/olist_customers_dataset.csv")
display(df_csv)

# COMMAND ----------

# Create Bronze Layer Database
spark.sql("CREATE DATABASE IF NOT EXISTS bronze_db")

# Create Silver Layer Database
spark.sql("CREATE DATABASE IF NOT EXISTS silver_db")

# Create Gold Layer Database
spark.sql("CREATE DATABASE IF NOT EXISTS gold_db")