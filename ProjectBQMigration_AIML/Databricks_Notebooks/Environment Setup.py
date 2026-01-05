# Databricks notebook source
# MAGIC %md
# MAGIC ### GCS Databricks Mount

# COMMAND ----------

bucket_name = "retail-demo-data-abhijit"
mount_name = "gcsmount_retails"
dbutils.fs.mount(
  source = f"gs://{bucket_name}",
  mount_point = f"/mnt/databricks/{mount_name}",
  extra_configs = {"fs.gs.project.id":"gcp-dbsbq-migration"}
)

# COMMAND ----------

df_csv = spark.read.format("csv").option("header", "true").load("/mnt/databricks/gcsmount_retails/stores/stores.csv")
display(df_csv)

# COMMAND ----------

# Create Bronze Layer Database
spark.sql("CREATE DATABASE IF NOT EXISTS bronze_db")

# Create Silver Layer Database
spark.sql("CREATE DATABASE IF NOT EXISTS silver_db")

# Create Gold Layer Database
spark.sql("CREATE DATABASE IF NOT EXISTS gold_db")

# COMMAND ----------

# dbutils.fs.unmount("/mnt/databricks/gcsmount_retails")

# COMMAND ----------

# dbutils.fs.ls("/mnt/databricks/gcsmount_retails/customers/customers.csv")