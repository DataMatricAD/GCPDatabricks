# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Lakebridge in Databricks
# MAGIC
# MAGIC **Lakebridge** is an open-source toolkit developed by **Databricks Labs** to simplify and accelerate **data platform migration** to the Databricks Lakehouse. It supports transitioning from systems such as **Snowflake**, **Oracle**, **SQL Server**, **Hive**, and **Presto**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Capabilities
# MAGIC
# MAGIC - **Analyze**: Profiles SQL workloads to assess readiness and complexity for migration.
# MAGIC - **Convert**: Translates SQL scripts from source dialects (e.g., Snowflake, Oracle) into **Databricks SQL**.
# MAGIC - **Validate**: Compares data between source and Databricks to ensure correctness after migration.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Usage
# MAGIC
# MAGIC - Lakebridge is executed via the **Databricks Labs CLI**.
# MAGIC - Requires YAML configuration files.
# MAGIC - Can be run from:
# MAGIC   - **Databricks Web Terminal** (on supported clusters)
# MAGIC   - **Local machines** with Databricks CLI access
# MAGIC
# MAGIC > **Note:** Lakebridge is not designed for direct use within notebooks. It does not expose public Python APIs for in-notebook execution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Resources
# MAGIC
# MAGIC - GitHub: [github.com/databrickslabs/lakebridge](https://github.com/databrickslabs/lakebridge)
# MAGIC - Blog: [Databricks Lakebridge Announcement](https://www.databricks.com/blog/introducing-lakebridge-free-open-data-migration-databricks-sql)
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# Install lakebridge 0.10.6 specifically
%pip install databricks-labs-lakebridge==0.10.6

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from lakebridge.validator import Validator

# Set up table validation
validator = Validator(
    source_table="gcp_dbsbq_migration_demo.bronze_db.orders",
    target_table="gcp_dbsbq_migration_demo.bronze_db.orders_val_lb",
    join_columns=["order_id"],
    compare_columns=["customer_id", "status"]
)

# Run the validation
result = validator.validate()

# Show mismatches (if any)
display(result)


# COMMAND ----------

config_yaml = """
source:
  type: delta
  table: gcp_dbsbq_migration_demo.bronze_db.orders
target:
  type: delta
  table: gcp_dbsbq_migration_demo.bronze_db.orders_val_lb
comparison:
  join_columns=["order_id"],
  compare_columns=["customer_id", "status"]
"""

with open("/tmp/lakebridge_validator_config.yaml", "w") as f:
    f.write(config_yaml)


# COMMAND ----------

import subprocess

try:
    result = subprocess.run(
        ["python", "-m", "databricks.labs.lakebridge", "validate", "--config", "/tmp/lakebridge_validator_config.yaml"],
        capture_output=True,
        text=True
    )
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)
except Exception as e:
    print("Error running validation:", e)


# COMMAND ----------

# MAGIC %sh
# MAGIC databricks labs lakebridge validate --config /dbfs/tmp/lakebridge_config.yaml