# Databricks notebook source
import sys
import os
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from dataclasses import dataclass
from enum import Enum

# this is needed to be able to import from relative paths
sys.path.append(os.path.abspath('..'))
from utils.utils import get_user, get_username

username = get_username(dbutils)
user = get_user(username)

# COMMAND ----------

# Define the start and end dates for each record in the dimension table
start_date = F.to_date(F.lit("2022-01-01"))
end_date = F.to_date(F.lit("9999-12-31"))

class PipelineMode(str, Enum):
  TEST = "test"
  PROD = "prod"

def transform_to_scd2(spark: SparkSession, customer_data: DataFrame, mode: str = PipelineMode.PROD) -> None:
    # Generate SCD Type 2 table

    if mode == PipelineMode.TEST:
        output_path = f"/FileStore/{user}_silver_db_test/"
        spark.sql(
            f"""
            CREATE DATABASE IF NOT EXISTS {user}_silver_db_test
            LOCATION '{output_path}'
            """
        )

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {user}_silver_db_test.silver_customers
              (
              customer_id INT,
              customer_name STRING,
              state STRING,
              company STRING,
              phone_number STRING,
              start_date TIMESTAMP,
              end_date TIMESTAMP
              )
            USING DELTA
            """
        )

        silver_customers = DeltaTable.forName(spark, f"{user}_silver_db_test.silver_customers")

    else:
        silver_customers = DeltaTable.forName(spark, f"{user}_silver_db.silver_customers")

    effective_date = F.lit(F.current_date())
    scd2_data = (
        customer_data.select("customer_id", "customer_name", "state", "company", "phone_number")
        .distinct()
        .withColumn("start_date", effective_date)
        .withColumn("end_date", F.to_date(F.lit("9999-12-31")))
    )

    # Merge SCD Type 2 table with existing Delta Lake table
    merge_condition = "scd2.customer_id = source.customer_id"
    set_values = {"end_date": F.date_sub(F.current_date(), 1)}
    insert_values = {
        "customer_id": F.col("source.customer_id"),
        "customer_name": F.col("source.customer_name"),
        "state": F.col("source.state"),
        "company": F.col("source.company"),
        "phone_number": F.col("source.phone_number"),
        "start_date": F.col("source.start_date"),
        "end_date": F.col("source.end_date"),
    }

    (
        silver_customers.alias("scd2")
        .merge(scd2_data.alias("source"), merge_condition)
        .whenMatchedUpdate(set=set_values)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
