# Databricks notebook source
# import re

# def convert_bq_to_delta(query: str, source_db: str = "gcp-dbsbq-migration.retail_silver", target_db: str = "gcp-dbsbq-migration.retail_gold"):
#     output = []

#     # Step 1: Convert CREATE SCHEMA
#     schema_match = re.search(r"CREATE SCHEMA IF NOT EXISTS `(.+?)`;", query)
#     if schema_match:
#         schema = schema_match.group(1)
#         output.append(f'spark.sql("CREATE DATABASE IF NOT EXISTS `{schema}`")')

#     # Step 2: Split the rest of the query into individual table CTAS statements
#     statements = re.findall(r"CREATE OR REPLACE TABLE (.+?) AS\s+SELECT(.+?);(?=\s*CREATE|\s*$)", query, re.DOTALL)

#     for full_table_name, select_block in statements:
#         full_table_name = full_table_name.strip()
#         table_short_name = full_table_name.split(".")[-1]
#         output.append(f"# Create {table_short_name}")
#         output.append(f"""df_{table_short_name} = spark.sql(\"\"\"\nSELECT{select_block.strip()}\n\"\"\")""")
#         output.append(f'df_{table_short_name}.write.format("delta").mode("overwrite").saveAsTable("{full_table_name}")\n')

#     return "\n".join(output)


# COMMAND ----------

import re

#df_csv = spark.read.format("csv").option("header", "true").load("/mnt/databricks/gcsmount_retails/stores/stores.csv")

def convert_bq_to_delta(query: str) -> str:
    output = []

    # Map old database names to new ones
    db_mapping = {
        "gcp-dbsbq-migration.retail_bronze": "gcp_dbsbq_migration_demo.bronze_db",
        "gcp-dbsbq-migration.retail_silver": "gcp_dbsbq_migration_demo.silver_db",
        "gcp-dbsbq-migration.retail_gold": "gcp_dbsbq_migration_demo.gold_db"
    }

    # Replace all schema names based on mapping
    for old_db, new_db in db_mapping.items():
        query = query.replace(old_db, new_db)

    # Step 1: Convert CREATE SCHEMA to spark.sql
    schema_matches = re.findall(r"CREATE SCHEMA IF NOT EXISTS `(.+?)`;", query)
    for schema in schema_matches:
        output.append(f'spark.sql("CREATE DATABASE IF NOT EXISTS `{schema}`")')

    # Step 2: Extract CREATE TABLE AS SELECT blocks
    statements = re.findall(
        r"CREATE OR REPLACE TABLE (.+?) AS\s+SELECT(.+?);(?=\s*CREATE|\s*$)",
        query,
        re.DOTALL
    )

    for full_table_name, select_block in statements:
        full_table_name = full_table_name.strip()
        table_short_name = full_table_name.split(".")[-1]
        output.append(f"\n# Create {table_short_name}")
        output.append(f"""df_{table_short_name} = spark.sql(\"\"\"\nSELECT{select_block.strip()}\n\"\"\")""")
        output.append(f'df_{table_short_name}.write.format("delta").mode("overwrite").saveAsTable("{full_table_name}")\n')

    return "\n".join(output)


# COMMAND ----------

# import re

# def convert_bq_to_delta_with_csv_autoload(query: str) -> str:
#     output = []

#     # 1. Replace all schema names
#     db_mapping = {
#         "gcp-dbsbq-migration.retail_bronze": "gcp_dbsbq_migration_demo.bronze_db",
#         "gcp-dbsbq-migration.retail_silver": "gcp_dbsbq_migration_demo.silver_db",
#         "gcp-dbsbq-migration.retail_gold": "gcp_dbsbq_migration_demo.gold_db"
#     }

#     for old_db, new_db in db_mapping.items():
#         query = query.replace(old_db, new_db)

#     # 2. Find and convert any references to tables from gcp-dbsbq-migration.retails_db.<table>
#     csv_table_matches = re.findall(r"gcp-dbsbq-migration\.retails_db\.([a-zA-Z_0-9]+)", query)
#     csv_table_matches = list(set(csv_table_matches))  # remove duplicates

#     for table_name in csv_table_matches:
#         df_name = f"df_{table_name}"
#         mount_path = f"/mnt/databricks/gcsmount_retails/{table_name}/{table_name}.csv"

#         output.append(f'# Load {table_name} table from CSV')
#         output.append(f'{df_name} = spark.read.format("csv")\\')
#         output.append(f'    .option("header", "true")\\')
#         output.append(f'    .load("{mount_path}")\n')

#         # Replace SQL table reference with df_<table_name>
#         full_bq_table = f"gcp-dbsbq-migration.retails_db.{table_name}"
#         query = query.replace(full_bq_table, df_name)

#     # 3. Convert CREATE SCHEMA
#     schema_matches = re.findall(r"CREATE SCHEMA IF NOT EXISTS `(.+?)`;", query)
#     for schema in schema_matches:
#         output.append(f'spark.sql("CREATE DATABASE IF NOT EXISTS `{schema}`")')

#     # 4. Convert each CREATE OR REPLACE TABLE block
#     statements = re.findall(
#         r"CREATE OR REPLACE TABLE (.+?) AS\s+SELECT(.+?);(?=\s*CREATE|\s*$)",
#         query,
#         re.DOTALL
#     )

#     for full_table_name, select_block in statements:
#         full_table_name = full_table_name.strip()
#         table_short_name = full_table_name.split(".")[-1]
#         output.append(f"\n# Create {table_short_name}")
#         output.append(f"""df_{table_short_name} = spark.sql(\"\"\"\nSELECT{select_block.strip()}\n\"\"\")""")
#         output.append(f'df_{table_short_name}.write.format("delta").mode("overwrite").saveAsTable("{full_table_name}")\n')

#     return "\n".join(output)


# COMMAND ----------

bq_query_1 = """
--Create gold tables with schema
CREATE SCHEMA IF NOT EXISTS `gcp-dbsbq-migration.retail_gold`;
"""

converted_code_1 = convert_bq_to_delta(bq_query_1)
print(converted_code_1)


# COMMAND ----------

bq_query_2 = """
--Revenue and Sales Summary (Per Day + Store)
CREATE OR REPLACE TABLE gcp-dbsbq-migration.retail_gold.gold_daily_store_sales AS
SELECT 
  DATE(o.order_date) AS order_date,
  o.store_id,
  o.store_name,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(oi.total_price) AS revenue
FROM gcp-dbsbq-migration.retail_silver.orders o
JOIN gcp-dbsbq-migration.retail_silver.order_items oi USING (order_id)
WHERE o.status = 'completed'
GROUP BY 1, 2, 3;
"""
converted_code_2 = convert_bq_to_delta(bq_query_2)
print(converted_code_2)


# COMMAND ----------

bq_query_3 = """
--Top Selling Products
CREATE OR REPLACE TABLE gcp-dbsbq-migration.retail_gold.gold_top_products AS
SELECT 
  product_id,
  product_name,
  category,
  SUM(quantity) AS total_quantity_sold,
  SUM(total_price) AS total_revenue
FROM gcp-dbsbq-migration.retail_silver.order_items
GROUP BY 1, 2, 3
ORDER BY total_revenue DESC
LIMIT 20;
"""
converted_code_3 = convert_bq_to_delta(bq_query_3)
print(converted_code_3)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Silver Query Conversion

# COMMAND ----------

bq_query_4 = """
--orders with proper dates & joins
CREATE OR REPLACE TABLE gcp-dbsbq-migration.retail_silver.orders AS
SELECT 
  o.order_id,
  o.customer_id,
  c.first_name,
  c.last_name,
  o.store_id,
  s.store_name,
  o.order_date,
  o.status
FROM gcp-dbsbq-migration.retail_bronze.orders_tbl o
JOIN gcp-dbsbq-migration.retail_bronze.customers_tbl c USING (customer_id)
JOIN gcp-dbsbq-migration.retail_bronze.stores_tbl s USING (store_id);

"""

converted_code_4 = convert_bq_to_delta(bq_query_4)
print(converted_code_4)


# COMMAND ----------


# Create orders
df_orders = spark.sql("""
SELECT o.order_id,
  o.customer_id,
  c.first_name,
  c.last_name,
  o.store_id,
  s.store_name,
  o.order_date,
  o.status
FROM gcp_dbsbq_migration_demo.bronze_db.orders o
JOIN gcp_dbsbq_migration_demo.bronze_db.customers c USING (customer_id)
JOIN gcp_dbsbq_migration_demo.bronze_db.stores s USING (store_id)
""")
df_orders.write.format("delta").mode("overwrite").saveAsTable("gcp_dbsbq_migration_demo.silver_db.orders")


# COMMAND ----------

bq_query_5 ="""
--Join order items with products
CREATE OR REPLACE TABLE gcp-dbsbq-migration.retail_silver.order_items AS
SELECT 
  oi.order_id,
  oi.product_id,
  p.name AS product_name,
  p.category,
  oi.quantity,
  oi.unit_price,
  oi.total_price
FROM gcp-dbsbq-migration.retail_bronze.order_items_tbl oi
JOIN gcp-dbsbq-migration.retail_bronze.products_tbl p USING (product_id);

"""

converted_code_5 = convert_bq_to_delta(bq_query_5)
print(converted_code_5)


# COMMAND ----------


# Create order_items
df_order_items = spark.sql("""
SELECT oi.order_id,
  oi.product_id,
  p.name AS product_name,
  p.category,
  oi.quantity,
  oi.unit_price,
  oi.total_price
FROM gcp_dbsbq_migration_demo.bronze_db.order_items oi
JOIN gcp_dbsbq_migration_demo.bronze_db.products p USING (product_id)
""")
df_order_items.write.format("delta").mode("overwrite").saveAsTable("gcp_dbsbq_migration_demo.silver_db.order_items")


# COMMAND ----------

bq_query_6 = """
--Inventory cleaned
CREATE OR REPLACE TABLE gcp-dbsbq-migration.retail_silver.inventory AS
SELECT 
  i.store_id,
  s.store_name,
  i.product_id,
  p.name AS product_name,
  i.stock
FROM gcp-dbsbq-migration.retail_bronze.inventory_tbl i
JOIN gcp-dbsbq-migration.retail_bronze.stores_tbl s USING (store_id)
JOIN gcp-dbsbq-migration.retail_bronze.products_tbl p USING (product_id);

"""

converted_code_6 = convert_bq_to_delta(bq_query_6)
print(converted_code_6)

# COMMAND ----------

# Create inventory
df_inventory = spark.sql("""
SELECT i.store_id,
  s.store_name,
  i.product_id,
  p.name AS product_name,
  i.stock
FROM gcp_dbsbq_migration_demo.bronze_db.inventory i
JOIN gcp_dbsbq_migration_demo.bronze_db.stores s USING (store_id)
JOIN gcp_dbsbq_migration_demo.bronze_db.products p USING (product_id)
""")
df_inventory.write.format("delta").mode("overwrite").saveAsTable("gcp_dbsbq_migration_demo.silver_db.inventory")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Gold Query Conversion

# COMMAND ----------

bq_query_7 = """
--Revenue and Sales Summary (Per Day + Store)
CREATE OR REPLACE TABLE gcp-dbsbq-migration.retail_gold.gold_daily_store_sales AS
SELECT 
  DATE(o.order_date) AS order_date,
  o.store_id,
  o.store_name,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(oi.total_price) AS revenue
FROM gcp-dbsbq-migration.retail_silver.orders o
JOIN gcp-dbsbq-migration.retail_silver.order_items oi USING (order_id)
WHERE o.status = 'completed'
GROUP BY 1, 2, 3;
"""

converted_code_7 = convert_bq_to_delta(bq_query_7)
print(converted_code_7)

# COMMAND ----------

# Create gold_daily_store_sales
df_gold_daily_store_sales = spark.sql("""
SELECT DATE(o.order_date) AS order_date,
  o.store_id,
  o.store_name,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(oi.total_price) AS revenue
FROM gcp_dbsbq_migration_demo.silver_db.orders o
JOIN gcp_dbsbq_migration_demo.silver_db.order_items oi USING (order_id)
WHERE o.status = 'completed'
GROUP BY 1, 2, 3
""")
df_gold_daily_store_sales.write.format("delta").mode("overwrite").saveAsTable("gcp_dbsbq_migration_demo.gold_db.gold_daily_store_sales")


# COMMAND ----------

bq_query_8 = """
--Top Selling Products
CREATE OR REPLACE TABLE gcp-dbsbq-migration.retail_gold.gold_top_products AS
SELECT 
  product_id,
  product_name,
  category,
  SUM(quantity) AS total_quantity_sold,
  SUM(total_price) AS total_revenue
FROM gcp-dbsbq-migration.retail_silver.order_items
GROUP BY 1, 2, 3
ORDER BY total_revenue DESC
LIMIT 20;
"""

converted_code_8 = convert_bq_to_delta(bq_query_8)
print(converted_code_8)

# COMMAND ----------

# Create gold_top_products
df_gold_top_products = spark.sql("""
SELECT product_id,
  product_name,
  category,
  SUM(quantity) AS total_quantity_sold,
  SUM(total_price) AS total_revenue
FROM gcp_dbsbq_migration_demo.silver_db.order_items
GROUP BY 1, 2, 3
ORDER BY total_revenue DESC
LIMIT 20
""")
df_gold_top_products.write.format("delta").mode("overwrite").saveAsTable("gcp_dbsbq_migration_demo.gold_db.gold_top_products")


# COMMAND ----------

bq_query_9 = """
--Inventory Health Report
CREATE OR REPLACE TABLE gcp-dbsbq-migration.retail_gold.gold_inventory_health AS
SELECT 
  store_id,
  store_name,
  product_id,
  product_name,
  stock,
  CASE 
    WHEN stock = 0 THEN 'Out of Stock'
    WHEN stock < 10 THEN 'Low Stock'
    ELSE 'In Stock'
  END AS stock_status
FROM gcp-dbsbq-migration.retail_silver.inventory;

"""

converted_code_9 = convert_bq_to_delta(bq_query_9)
print(converted_code_9)

# COMMAND ----------

# Create gold_inventory_health
df_gold_inventory_health = spark.sql("""
SELECT store_id,
  store_name,
  product_id,
  product_name,
  stock,
  CASE 
    WHEN stock = 0 THEN 'Out of Stock'
    WHEN stock < 10 THEN 'Low Stock'
    ELSE 'In Stock'
  END AS stock_status
FROM gcp_dbsbq_migration_demo.silver_db.inventory
""")
df_gold_inventory_health.write.format("delta").mode("overwrite").saveAsTable("gcp_dbsbq_migration_demo.gold_db.gold_inventory_health")


# COMMAND ----------

bq_query_10 = """
--customer feature
CREATE OR REPLACE TABLE gcp-dbsbq-migration.retail_gold.gold_customer_features AS
SELECT 
  o.customer_id,
  ANY_VALUE(c.first_name) AS first_name,
  ANY_VALUE(c.last_name) AS last_name,
  COUNT(DISTINCT o.order_id) AS total_orders,
  COUNT(DISTINCT o.store_id) AS unique_stores_visited,
  SUM(oi.total_price) AS lifetime_value,
  AVG(oi.total_price) AS avg_order_value,
  MIN(o.order_date) AS first_order_date,
  MAX(o.order_date) AS last_order_date,
  --DATE_DIFF(CURRENT_DATE(), MAX(o.order_date), DAY) AS days_since_last_order,
  DATE_DIFF(CURRENT_DATE(), DATE(MAX(o.order_date)), DAY) AS days_since_last_order,
  APPROX_QUANTILES(oi.total_price, 5)[OFFSET(3)] AS median_order_value
FROM gcp-dbsbq-migration.retail_silver.orders o
JOIN gcp-dbsbq-migration.retail_silver.order_items oi USING (order_id)
LEFT JOIN gcp-dbsbq-migration.retails_db.customer_tbl c USING (customer_id)
WHERE o.status = 'completed'
GROUP BY o.customer_id;
"""

converted_code_10 = convert_bq_to_delta(bq_query_10)
print(converted_code_10)

# COMMAND ----------

# Create gold_customer_features
df_gold_customer_features = spark.sql("""
SELECT o.customer_id,
  ANY_VALUE(c.first_name) AS first_name,
  ANY_VALUE(c.last_name) AS last_name,
  COUNT(DISTINCT o.order_id) AS total_orders,
  COUNT(DISTINCT o.store_id) AS unique_stores_visited,
  SUM(oi.total_price) AS lifetime_value,
  AVG(oi.total_price) AS avg_order_value,
  MIN(o.order_date) AS first_order_date,
  MAX(o.order_date) AS last_order_date,
  --DATE_DIFF(CURRENT_DATE(), MAX(o.order_date), DAY) AS days_since_last_order,
  DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) AS days_since_last_order,
  percentile_approx(oi.total_price, 0.5) AS median_order_value
FROM gcp_dbsbq_migration_demo.silver_db.orders o
JOIN gcp_dbsbq_migration_demo.silver_db.order_items oi USING (order_id)
LEFT JOIN gcp_dbsbq_migration_demo.bronze_db.customers c USING (customer_id)
WHERE o.status = 'completed'
GROUP BY o.customer_id
""")
df_gold_customer_features.write.format("delta").mode("overwrite").saveAsTable("gcp_dbsbq_migration_demo.gold_db.gold_customer_features")
