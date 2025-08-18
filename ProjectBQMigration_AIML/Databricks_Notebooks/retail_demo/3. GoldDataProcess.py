# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver To Gold - Gold Layer Data Processing

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


# COMMAND ----------

dbutils.notebook.exit("SUCCESS")