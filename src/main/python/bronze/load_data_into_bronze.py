# Databricks notebook source
from pyspark.sql.functions import *


username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace(".", "_")
user = username[: username.index("@")]

# COMMAND ----------

dbfs_path = f"/FileStore/{username}/retail_dataset/"
bronze_db = f"{user}_bronze_db"

# Define the options for the autoloader
bronze_options = {"mode": "DROPMALFORMED", "header": True}

# COMMAND ----------

def load_data_to_bronze(source_dataset: str, target_path: str, env: str) -> None:
    # Ingest the data into the bronze layer
    schema_location = target_path + "_checkpoints/" + source_dataset
    checkpoint_location = target_path + "_checkpoints/" + source_dataset

    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("header", "true")
        .load(dbfs_path + env + "/" + source_dataset)
        .writeStream.option("checkpointLocation", checkpoint_location)
        .trigger(once=True)
        .start(target_path + "bronze_" + source_dataset)
        .awaitTermination()
    )
