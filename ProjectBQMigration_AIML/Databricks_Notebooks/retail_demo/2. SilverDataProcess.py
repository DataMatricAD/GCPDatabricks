# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze To Silver - Silver Layer Data Processing

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

dbutils.notebook.exit("SUCCESS")