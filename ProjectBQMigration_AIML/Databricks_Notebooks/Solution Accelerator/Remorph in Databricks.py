# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### 🔄 Remorph: Databricks Labs Migration Toolkit
# MAGIC
# MAGIC **Remorph** is an open-source toolkit developed by **Databricks Labs** to simplify and accelerate migration and onboarding to Databricks. It offers two core capabilities:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### ⚙️ 1. Transpile  
# MAGIC A SQL code converter that automatically translates SQL scripts from other dialects—especially **Snowflake**—into **Databricks SQL**, using the `SQLGlot` parser and built-in validation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 📊 2. Reconcile  
# MAGIC A data reconciliation tool that compares data between a **source system** (like **Snowflake** or **Oracle**) and **Databricks**, identifying mismatches in data or schema to ensure migration accuracy.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Use Remorph to speed up SQL code conversion, validate transformations, and ensure data consistency during platform migrations.
# MAGIC

# COMMAND ----------

# MAGIC %pip install databricks-labs-remorph

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
print(sys.version)

# COMMAND ----------

# MAGIC %pip show -f databricks-labs-remorph

# COMMAND ----------

# MAGIC %pip install sqlglot

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sqlglot

converted = sqlglot.transpile(
    "SELECT DATE_DIFF(CURRENT_DATE(), DATE(order_date), DAY) AS days_since_order FROM orders",
    read="bigquery",
    write="databricks"
)

print(converted[0])


# COMMAND ----------

# Step 1: Import the core Remorph modules
from remorph import DialectConverter
from remorph.enums import SqlDialect

# Step 2: Initialize the converter from BigQuery to Databricks SQL
converter = DialectConverter(
    source_dialect=SqlDialect.BIGQUERY,
    target_dialect=SqlDialect.DATABRICKS
)

# Step 3: Sample BigQuery SQL
bigquery_sql = """
SELECT DATE(order_date) AS order_day,
       COUNT(DISTINCT order_id) AS total_orders,
       SUM(total_price) AS revenue
FROM orders
GROUP BY order_day
"""

# Step 4: Convert the SQL
converted_sql = converter.convert(bigquery_sql)

# Step 5: Print the result
print(converted_sql)
