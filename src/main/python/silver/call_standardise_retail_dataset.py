# Databricks notebook source
# MAGIC %run ./standardise_retail_dataset

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace(".", "_")
user = username[: username.index("@")]
input_db = f"{user}_bronze_db"

# COMMAND ----------

# Read the order and sales data from bronze layer
orders_bronze_df = spark.read.table(f"{input_db}.bronze_orders")
sales_bronze_df = spark.read.table(f"{input_db}.bronze_sales")

# COMMAND ----------

orders_silver_df = transform_bronze_orders(orders_bronze_df)
sales_silver_df = transform_bronze_sales(sales_bronze_df)

# COMMAND ----------

# Write the standardized data into the silver layer

# output_db is defined and imported from %run ./standardise_retail_dataset
orders_silver_df.write.format("delta").mode("overwrite").saveAsTable(f"{output_db}.silver_orders")

# Write the standardized data into the silver layer
sales_silver_df.write.format("delta").mode("overwrite").saveAsTable(f"{output_db}.silver_sales")

# COMMAND ----------

product_bronze_df = spark.read.table(f"{input_db}.bronze_products")
product_silver_df = transform_bronze_products(product_bronze_df)

product_silver_df.write.format("delta").mode("overwrite").saveAsTable(f"{output_db}.silver_products")
