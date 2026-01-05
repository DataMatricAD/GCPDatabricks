# Databricks notebook source
# MAGIC %md
# MAGIC ###Gold Layer Data Processing

# COMMAND ----------

# MAGIC %run /Workspace/Users/abhijitawscert2023@gmail.com/util_mod

# COMMAND ----------

tables = ['orders', 'order_items', 'sellers', 'products', 'customers','geolocation','order_reviews','order_payments']
prepare_dataframes_from_tables(tables)

# COMMAND ----------

full_orders_df = orders_df.join(order_items_df, 'order_id', 'inner') \
    .join(products_df, 'product_id', 'inner') \
    .join(sellers_df, 'seller_id', 'inner') \
    .join(customers_df, 'customer_id', 'inner')

full_orders_df = full_orders_df.join(geolocation_df, full_orders_df.customer_zip_code_prefix == geolocation_df.geolocation_zip_code_prefix, 'left') \
    .join(order_reviews_df, 'order_id', 'left') \
    .join(order_payments_df, 'order_id', 'left')

full_orders_df = full_orders_df.dropDuplicates(['order_id', 'customer_id', 'seller_id', 'product_id', 'order_item_id', 'customer_unique_id', 'review_id']).na.drop().orderBy(full_orders_df.price.desc())

# full_orders_df = full_orders_df.repartition(100, 'order_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Load complete order detail table to gold layer

# COMMAND ----------

load_to_gold_db(full_orders_df, 'order_details')
## 4 mins load time with normal duplicate removal process selecting all columns.
## 5 mins load time with normal duplicate removal process selecting all id columns.

# COMMAND ----------

# MAGIC %md
# MAGIC #####Aggregation 
# MAGIC - #######Total Revenues Per Seller
# MAGIC - #######Total Orders Per Custoemr
# MAGIC - #######Average Review Score Per Seller
# MAGIC - #######Most Sold Products ( Top 10 )
# MAGIC - #######Top Custoemrs By Spending

# COMMAND ----------

# full_orders_df.count()

#54,442,883
#1,042,785 - 2 mins to count without repartition
#1,042,785 - 2 mins to count with repartition
#10,061 - 2 mins to count with repartition and duplicate removal

# COMMAND ----------

full_orders_df = spark.read.table('gold_db.order_details')
load_to_bigquery(full_orders_df, 'orders_details')

# COMMAND ----------

# Total Revenues Per Seller

from pyspark.sql.functions import sum, desc

seller_revenue_df = full_orders_df.groupBy('seller_id').agg(sum('price').alias('total_revenue')).orderBy(desc('total_revenue')).limit(100)

# load_to_gold_db(seller_revenue_df, 'seller_revenue')
load_to_bigquery(seller_revenue_df, 'seller_revenue')

# COMMAND ----------

# Total Orders Per Customer

from pyspark.sql.functions import count, desc

customer_order_count_df = full_orders_df.groupBy('customer_id')\
.agg(count('order_id').alias('total_orders'))\
.orderBy(desc('total_orders'))\

# load_to_gold_db(customer_order_count_df, 'customer_order_count')
load_to_bigquery(customer_order_count_df, 'customer_order_count')
# display(customer_order_count_df.limit(5))

# COMMAND ----------

#Average Review Score Per Seller

from pyspark.sql.functions import avg, desc
seller_review_df = full_orders_df.groupBy('seller_id')\
.agg(avg('review_score').alias('avg_review_score'))\
.orderBy(desc('avg_review_score')).cache()

# load_to_gold_db(seller_review_df, 'seller_review')
load_to_bigquery(seller_review_df, 'seller_review')


# seller_review_df.show()

# COMMAND ----------

# Top 10 Most Sold Products

top_products_df = full_orders_df.groupBy('product_id')\
.agg(count('order_id').alias('total_sold'))\
.orderBy(desc('total_sold'))\
.limit(100)

# load_to_gold_db(top_products_df, 'top_products')
load_to_bigquery(top_products_df, 'top_products')

# top_products_df.show()

# COMMAND ----------

# Top Customers By Spending

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col


# Dense Rank for Sellers Based on Revenue

window_spec = Window.partitionBy('seller_id').orderBy(desc('price'))

# Rank Top Selling Products Per seller

top_seller_products_df = full_orders_df.withColumn('rank', rank().over(window_spec)).filter(col('rank') <= 5)

# load_to_gold_db(top_seller_products_df, 'top_seller_products')
load_to_bigquery(top_seller_products_df, 'top_seller_products')

# top_seller_products_df.select('seller_id', 'price', 'rank').show()