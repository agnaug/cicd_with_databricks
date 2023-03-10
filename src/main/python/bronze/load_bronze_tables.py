# Databricks notebook source
dbutils.widgets.text("env", "")
dbutils.widgets.text("source_dataset", "customers")

env = dbutils.widgets.get("env")
dataset = dbutils.widgets.get("source_dataset")

# COMMAND ----------

# MAGIC %run ./load_data_into_bronze $env=env

# COMMAND ----------

# Set the target location for the delta table
target_path = f"/FileStore/{username}_bronze_db/"
load_data_to_bronze(dataset, target_path, env)
