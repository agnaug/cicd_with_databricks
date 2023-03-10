# Databricks notebook source
# MAGIC %run ../../python/silver/standardise_retail_dataset

# COMMAND ----------

# "Third Party"
from pyspark.sql import functions as F
from pyspark.sql import types as T


def test_transform_bronze_orders():
    # Create input data
    input_data = [("1", "2022-01-01T08:11:14.000Z", "100", "shipped")]
    input_schema = T.StructType(
        [
            T.StructField("order_id", T.StringType(), True),
            T.StructField("order_date", T.StringType(), True),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("order_status", T.StringType(), True),
        ]
    )
    input_df = spark.createDataFrame(input_data, input_schema)
    
    # Run the transformation function
    actual_df = transform_bronze_orders(input_df)

    # Define expected output data
    expected_data = [("1", "2022-01-01T08:11:14.000Z", "100", "completed")]
    expected_schema = T.StructType(
        [
            T.StructField("order_id", T.StringType(), True),
            T.StructField("order_date", T.StringType(), True),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("order_status", T.StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Verify the result
    assert expected_df.select("order_status").collect() == actual_df.select("order_status").collect()

# COMMAND ----------


def test_transform_bronze_sales():
    # Create input data
    input_data = [("1", "100", "2022-12-11T04:03:39.000Z", "50", "US", "AUD")]
    input_schema = StructType(
        [
            T.StructField("sale_id", T.StringType(), True),
            T.StructField("product_id", T.StringType(), True),
            T.StructField("sale_date", T.StringType(), True),
            T.StructField("sale_amount", T.StringType(), True),
            T.StructField("state", T.StringType(), True),
            T.StructField("currency", T.StringType(), True),
        ]
    )
    input_df = spark.createDataFrame(input_data, input_schema)

    # Define expected output data
    expected_data = [("1", "100", "50", "US", "USD")]
    expected_schema = T.StructType(
        [
            T.StructField("sale_id", T.StringType(), True),
            T.StructField("product_id", T.StringType(), True),
            T.StructField("sale_amount", T.StringType(), True),
            T.StructField("state", T.StringType(), True),
            T.StructField("currency", T.StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    expected_df = expected_df.withColumn("sale_date", F.to_date(F.lit("2022-12-11")))
    # Run the transformation function
    actual_df = transform_bronze_sales(input_df)

    # Verify the result
    assert expected_df.select("sale_date").collect() == actual_df.select("sale_date").collect()
    assert expected_df.select("currency").collect() == actual_df.select("currency").collect()


# COMMAND ----------

test_transform_bronze_orders()
test_transform_bronze_sales()
