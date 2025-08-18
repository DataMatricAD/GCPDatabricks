from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "gcp-dbsbq-migration"
LOCATION = "us-west1"

# SQL for Bronze
SQL_QUERY_Bronze = """
--Create bronze tables with schema
CREATE SCHEMA IF NOT EXISTS `gcp-dbsbq-migration.retail_bronze`;


-- customers
CREATE OR REPLACE TABLE `gcp-dbsbq-migration.retail_bronze.customers_tbl`
AS SELECT * FROM `gcp-dbsbq-migration.retails_db.customer_tbl`;

--inventory
CREATE OR REPLACE TABLE `gcp-dbsbq-migration.retail_bronze.inventory_tbl`
AS SELECT * FROM `gcp-dbsbq-migration.retails_db.inventory_tbl`;

-- order_items
CREATE OR REPLACE TABLE `gcp-dbsbq-migration.retail_bronze.order_items_tbl`
AS SELECT * FROM `gcp-dbsbq-migration.retails_db.order_items_tbl`;

--orders
CREATE OR REPLACE TABLE `gcp-dbsbq-migration.retail_bronze.orders_tbl`
AS SELECT * FROM `gcp-dbsbq-migration.retails_db.orders_tbl`;


-- products
CREATE OR REPLACE TABLE `gcp-dbsbq-migration.retail_bronze.products_tbl`
AS SELECT * FROM `gcp-dbsbq-migration.retails_db.products_tbl`;

--stores
CREATE OR REPLACE TABLE `gcp-dbsbq-migration.retail_bronze.stores_tbl`
AS SELECT * FROM `gcp-dbsbq-migration.retails_db.stores_tbl`;

"""

# SQL for Silver
SQL_QUERY_Silver = """
--Create silver tables with schema

CREATE SCHEMA IF NOT EXISTS `gcp-dbsbq-migration.retail_silver`;

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

# SQL for Gold
SQL_QUERY_Gold = """
--Create gold tables with schema
CREATE SCHEMA IF NOT EXISTS `gcp-dbsbq-migration.retail_gold`;

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


# SQL for Gold
SQL_QUERY_MLModel = """
CREATE OR REPLACE MODEL `gcp-dbsbq-migration.retail_gold.ml_churn_model`
OPTIONS(model_type='logistic_reg') AS
SELECT
  *,
  CASE WHEN days_since_last_order > 60 THEN 1 ELSE 0 END AS label
FROM `gcp-dbsbq-migration.retail_gold.gold_customer_features`;
"""

with models.DAG(
    dag_id="run_retails_data_sqls",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["bigquery", "sequential", "bronze-silver-gold"],
) as dag:

    bronze_task = BigQueryInsertJobOperator(
        task_id="process_bronze",
        configuration={
            "query": {
                "query": SQL_QUERY_Bronze,
                "useLegacySql": False
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID,
    )

    silver_task = BigQueryInsertJobOperator(
        task_id="process_silver",
        configuration={
            "query": {
                "query": SQL_QUERY_Silver,
                "useLegacySql": False
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID,
    )

    gold_task = BigQueryInsertJobOperator(
        task_id="process_gold",
        configuration={
            "query": {
                "query": SQL_QUERY_Gold,
                "useLegacySql": False
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID,
    )

    ml_model_task = BigQueryInsertJobOperator(
        task_id="build_model",
        configuration={
            "query": {
                "query": SQL_QUERY_MLModel,
                "useLegacySql": False
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID,
    )

    # Set execution order: Bronze -> Silver -> Gold
    bronze_task >> silver_task >> gold_task >> ml_model_task
