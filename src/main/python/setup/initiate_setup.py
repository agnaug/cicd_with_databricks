# Databricks notebook source
dbutils.widgets.text("env", "")
dbutils.widgets.text("num_rows", "10")

# COMMAND ----------

pip install faker==17.6.0

# COMMAND ----------

# MAGIC %run ./cleanup

# COMMAND ----------

# MAGIC %run ./generate_retail_data

# COMMAND ----------

num_of_rows = int(dbutils.widgets.get("num_rows"))

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

generate_orders_data(num_of_rows, env)

# COMMAND ----------

generate_sales_data(num_of_rows, env)

# COMMAND ----------

generate_product_data(num_of_rows, env)

# COMMAND ----------

generate_customer_data_day_0(num_of_rows, env)

# COMMAND ----------

# MAGIC %run ./create_ddl
