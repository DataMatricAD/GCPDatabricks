# Databricks notebook source
table = "bigquery-public-data.samples.shakespeare"
df = spark.read.format("bigquery").option("table", table).load()
df.createOrReplaceTempView("shakespeare")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from shakespeare limit 10

# COMMAND ----------

df.write.format("bigquery").option("table", "gcp-dbsbq-migration.ecomdata.shakespeare").option("temporaryGcsBucket", "retail-demo-data-abhijit").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table bigquery.`your-project-id.dataset.table` as
# MAGIC select * from shakespeare limit 10