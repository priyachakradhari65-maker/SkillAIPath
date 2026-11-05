# Databricks notebook source
# Join Delta Lake + External MySQL (No ETL needed)

# Read from Delta (Databricks)
sales_delta = spark.table("sap_dev.silver.fact_sales")

# Read from MySQL (External)
customers_mysql = spark.table("mysql_catalog.production.customers")

# Hybrid join
result = sales_delta.join(
    customers_mysql,
    sales_delta.customer_id == customers_mysql.id,
    "inner"
).select(
    sales_delta.sale_id,
    sales_delta.amount,
    customers_mysql.customer_name,
    customers_mysql.email
)

display(result.limit(100))