# Databricks notebook source
#Bronze data loader
def load_raw_data(folders):
    from pyspark.sql.functions import lit, current_date
    dataframes = {}
    load_date = current_date()
    for folder in folders:
        df_name = folder.split('/')[-2]
        df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(folder)
        df = df.withColumn("load_date", load_date)
        df.write.mode("overwrite").partitionBy("load_date").saveAsTable(f"bronze_db.{df_name}")
        
        # dataframes[df_name] = df
        print(f"Data loaded successfully for {df_name} in bronze_db")
    print("All data loaded successfully")
    # return dataframes


# COMMAND ----------

#Silver data loader
def upsert_latest_partition_data(tables, target_db):
    from pyspark.sql.functions import col, row_number, current_timestamp
    from pyspark.sql.window import Window

    for table in tables:
        bronze_table = f"bronze_db.{table}"
        target_table = f"{target_db}.{table}"
        
        # Load the latest partition data
        latest_partition = spark.sql(f"SELECT MAX(load_date) as max_date FROM {bronze_table}").collect()[0]['max_date']
        latest_data = spark.table(bronze_table).filter(col("load_date") == latest_partition)
        
        # Add insert_dt and update_dt columns
        latest_data = latest_data.withColumn("insert_dt", current_timestamp()).withColumn("update_dt", current_timestamp())
        
        # Create the table if it doesn't exist using the latest_data schema
        if not spark.catalog.tableExists(target_table):
            latest_data.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(target_table)
        
        # Remove duplicates
        window_spec = Window.partitionBy(*[col for col in latest_data.columns if col not in ["load_date", "insert_dt", "update_dt"]]).orderBy(col("load_date").desc())
        deduped_data = latest_data.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
        
        # Create dynamic merge condition
        merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in deduped_data.columns if col not in ["load_date", "insert_dt", "update_dt"]])
        
        # Upsert using merge
        deduped_data.createOrReplaceTempView("source")
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING source
        ON {merge_condition}
        WHEN MATCHED THEN
          UPDATE SET {", ".join([f"target.{col} = source.{col}" for col in deduped_data.columns if col != "update_dt"])}, target.update_dt = current_timestamp()
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(deduped_data.columns)}) VALUES ({", ".join([f"source.{col}" for col in deduped_data.columns])})
        """
        spark.sql(merge_sql)
        print(f"Upsert completed for {table} in {target_db}")
    print("All silver data loaded successfully")

# COMMAND ----------

#Gold data loader
def prepare_dataframes_from_tables(tables, db_name='silver_db'):
    for table in tables:
        df_name = f"{table}_df"
        df = spark.table(f"{db_name}.{table}").drop("insert_dt", "update_dt", "load_date")
        globals()[df_name] = df

def load_to_gold_db(df,table_name):
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"gold_db.{table_name}")


def load_to_bigquery(df, table_name):
    table_name = "gcp-databricks-project-xxxxx.ecomdata" + "." + table_name
    df.write.format("bigquery").option("table", table_name).option("temporaryGcsBucket", "e-commerce-raw-data").mode("overwrite").save()
