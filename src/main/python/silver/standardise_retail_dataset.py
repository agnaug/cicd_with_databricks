# Databricks notebook source
# MAGIC %md
# MAGIC In this code, we've added standardization functions to
# MAGIC clean and standardize the name, address, city, and state columns
# MAGIC in the customer data. We've also added transformation functions
# MAGIC to add the customer and order details to the sales data,
# MAGIC and to convert the sales data to different currencies using exchange rates.
# MAGIC Finally, we've written the transformed data to delta lake in the Silver layer.

# COMMAND ----------

import sys
import os

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# this is needed to be able to import from relative paths
sys.path.append(os.path.abspath('..'))
from utils.utils import get_user, get_username

username = get_username(dbutils)
user = get_user(username)
output_db = f"{user}_silver_db"

# COMMAND ----------

def transform_bronze_orders(df: DataFrame) -> DataFrame:
    # Apply standardizations to order data
    df = (
        orders_bronze_df.withColumn("order_date", F.col("order_date").cast("Timestamp"))
        .withColumn("order_status", F.when(F.col("order_status") == "shipped", "completed").otherwise(F.col("order_status")))
        .withColumn("customer_id", F.col("customer_id").cast("Integer"))
        .select("order_id", "order_date", "customer_id", "order_status")
    )
    return df


def transform_bronze_sales(df: DataFrame) -> DataFrame:
    # Apply standardizations to sales data
    df = (
        df.withColumn("sale_date", F.to_date(F.col("sale_date").cast("Date")))
        .withColumn("sale_amount",F.round(F.col("sale_amount").cast("Double") * 0.9, 2))
        .withColumn("currency", F.lit("USD"))
        .withColumn("product_id", F.col("product_id").cast("Integer"))
        .select("sale_id", "product_id", "sale_date", "sale_amount", "currency")
    )
    return df

    
def transform_bronze_products(df: DataFrame) -> DataFrame:
      # Replace null values in "product_category" column with "Unknown"
      df = (
          df.withColumn("product_id", F.col("product_id").cast("Integer"))
          .withColumn("product_start_date", F.col("product_start_date").cast("Timestamp"))
          .withColumn("product_category", F.coalesce(F.col("product_category"), F.lit("Unknown")))
          .select("product_id", "product_category", "product_start_date")
      )
  
      return df
