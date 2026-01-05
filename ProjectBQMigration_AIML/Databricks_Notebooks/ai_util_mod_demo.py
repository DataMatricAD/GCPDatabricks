# Databricks notebook source
#Install Required Libraries
%pip install openai langchain tiktoken chromadb databricks-sql-connector

# COMMAND ----------

# MAGIC %pip install --upgrade langchain langchain-openai langchain-community

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import databricks.sql

conn = databricks.sql.connect(
    server_hostname="xxxxxxxx.gcp.databricks.com",  # Replace with your workspace hostname
    http_path="/sql/1.0/warehouses/xxxxxxxxxxx",            # Replace with your SQL warehouse path
    access_token="dapixxxxxxxx511"              # Use personal access token or notebook-scoped
)

cursor = conn.cursor()
cursor.execute("SELECT current_date()")
print(cursor.fetchall())

# COMMAND ----------

import os

# Set OpenAI key
os.environ["OPENAI_API_KEY"] = "sk-proj-xxxxxxxxxxxxxxxxxxxxxx"  # Replace with your key

# Set Databricks SQL connector config
DATABRICKS_CONFIG = {
    "server_hostname": "xxxxxxxxxxx.gcp.databricks.com",  # Replace with your workspace hostname
    "http_path": "/sql/1.0/warehouses/xxxxxxxxx",            # Replace with your SQL warehouse path
    "access_token": "dapiexxxxxxxxxxx511"              # Use personal access token or notebook-scoped
}
