# Databricks notebook source
table = "bigquery-public-data.samples.shakespeare"
df = spark.read.format("bigquery").option("table", table).load()
df.createOrReplaceTempView("shakespeare")

# COMMAND ----------

df.write.format("bigquery").option("table", "gcp-databricks-project-xxxxx.ecomdata.shakespeare").option("temporaryGcsBucket", "e-commerce-raw-data").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table bigquery.`your-project-id.dataset.table` as
# MAGIC select * from shakespeare limit 10