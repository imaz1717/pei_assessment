# Databricks notebook source
# DBTITLE 1,Instruction
# Data Transformation and Processing:

# Your task is to process the raw sales data using Databricks notebooks and PySpark. You need to clean up the data and transform it into structured formats suitable for analysis. Specifically, you should create a master table and perform aggregations based on the requirements provided.

# Note: Write appropriate Test Cases (Unit Tests) to ensure the correctness for the given scenarios. Use PySpark (not SQL) for this task.   

# Task

# Create raw tables for each source dataset
# Create an enriched table for customers and products 
# Create an enriched table which has 
    # order information 
        # Profit rounded to 2 decimal places
    # Customer name and country
    # Product category and sub category

# Create an aggregate table that shows profit by 
    # Year
    # Product Category
    # Product Sub Category
    # Customer
# Using SQL output the following aggregates
    # Profit by Year
    # Profit by Year + Product Category
    # Profit by Customer
    # Profit by Customer + Year

# Notes:
# Ensure you understand the task requirements thoroughly before starting.
# Pay attention to specific details and expectations outlined in the task descriptions.
# Use a test-driven development approach to validate the correctness of your implementations.
# Write comprehensive test cases to cover different scenarios and edge cases.
# Ensure your solution handles data quality issues and implements robust error-handling mechanisms.
# Document your code and assumptions clearly to make it understandable for others.
# Consider performance implications and optimize your code for efficiency and scalability.

# COMMAND ----------

# MAGIC %run "../commons/commons"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import json
%pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC **Raw Tables**

# COMMAND ----------

# DBTITLE 1,Order Raw Table
#Order table/DataFrame
orders_df = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(f"{base_path}/Orders.json")
raw_orders = (clean_column_names(orders_df)
                .withColumn("order_year", year(to_date(col("order_date"), "d/M/yyyy"))))

raw_orders_df = save_table(raw_orders, "raw_orders", partition_by=["order_year"])

# COMMAND ----------

# DBTITLE 1,Customer Raw Table
#Customer table/DataFrame
customer_pdf = pd.read_excel(f"{base_path}/Customer.xlsx", engine="openpyxl", dtype=str)
customer_pdf = customer_pdf.astype(object).where(customer_pdf.notna(), None) 
customer_schema = StructType([StructField(c, StringType()) for c in customer_pdf.columns])
customer_df = spark.createDataFrame(customer_pdf.values.tolist(), schema=customer_schema)
raw_customers_df = save_table_scd_2(clean_column_names(customer_df), "raw_customers",["customer_id"])

# COMMAND ----------

# DBTITLE 1,Product Raw Table
#Product table/DataFrame
products_df = spark.read.option("header", "true").option("inferSchema", "true").option("mode", "PERMISSIVE").csv(f"{base_path}/Products.csv")
raw_products_df = save_table_scd_2(clean_column_names(products_df), "raw_products",["product_id","category","sub_category","product_name","state","price_per_product"])

# COMMAND ----------

# MAGIC %md
# MAGIC **Enriched Tables**

# COMMAND ----------

# DBTITLE 1,Customer Enriched Table
enriched_customers_df = trim_strings(raw_customers_df)
enriched_customers_df = enriched_customers_df.filter(col("customer_id").isNotNull())
enriched_customers_df = enriched_customers_df.withColumn("customer_name", clean_column_values(col("customer_name")))
enriched_customers_df = enriched_customers_df.withColumn("email", lower("email"))
customers_df = save_table_scd_2(enriched_customers_df, "enriched_customers",["customer_id"])

# COMMAND ----------

# DBTITLE 1,Product Enriched Table
enriched_products_df = trim_strings(raw_products_df)
enriched_products_df = enriched_products_df.filter(col("product_id").isNotNull())
products_df = save_table_scd_2(enriched_products_df, "enriched_products",["product_id","category","sub_category","product_name","state","price_per_product"])

# COMMAND ----------

# Create an enriched table which has 
    # order information 
        # Profit rounded to 2 decimal places
    # Customer name and country
    # Product category and sub category

raw_orders_df = (raw_orders_df
                    .withColumn("order_date",to_date(col("order_date"),"d/M/yyyy"))
                    .withColumn("ship_date",to_date(col("ship_date"),"d/M/yyyy"))
                    .withColumn("profit", round(col("profit"), 2)))

enriched_orders_df = trim_strings(raw_orders_df)
# Unique Customers
customers = customers_df.select("customer_id", "customer_name", "country", "start_date", "end_date").distinct()
# Unique Products
products = products_df.select("product_id", "category", "sub_category", "start_date", "end_date").distinct()

enriched_orders_df = (
    raw_orders_df.alias("o")
    .join(broadcast(customers).alias("c"),
        (col("o.customer_id") == col("c.customer_id")) & (col("o.order_date") >= col("c.start_date")) &(col("o.order_date") <= col("c.end_date")), "left")
    .join(broadcast(products).alias("p"),
        (col("o.product_id") == col("p.product_id")) & (col("o.order_date") >= col("p.start_date")) & (col("o.order_date") <= col("p.end_date")), "left")
)

enriched_orders_df = enriched_orders_df.fillna("Unknown", subset=["customer_name", "country", "category", "sub_category"])
enriched_orders_df = enriched_orders_df.select("row_id", "order_id", "order_date", "ship_date", "order_year", "ship_mode", "c.customer_id", "customer_name", "country", "p.product_id", "category", "sub_category", "quantity", "price", "discount", "profit",
    )
orders_df = save_table(clean_column_names(enriched_orders_df), "enriched_order", partition_by=["order_year"])

# COMMAND ----------

# Create an aggregate table that shows profit by 
    # Year
    # Product Category
    # Product Sub Category
    # Customer
agrregate_orders_df = (orders_df
                            .groupBy("category", "sub_category", "customer_name","order_year")
                            .agg(sum("profit").cast("decimal(18,2)").alias("total_profit")))
save_table(clean_column_names(agrregate_orders_df), "agrregate_orders",partition_by=["order_year"])

# COMMAND ----------

# Using SQL output the following aggregates
    # Profit by Year
    # Profit by Year + Product Category
    # Profit by Customer
    # Profit by Customer + Year

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_year, sum(total_profit) as total_profit from pei_assesment.agrregate_orders group by order_year order by order_year

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_year, category as product_category, sum(total_profit) as total_profit from pei_assesment.agrregate_orders group by order_year, category order by order_year, category
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_name, sum(total_profit) as total_profit from pei_assesment.agrregate_orders group by customer_name order by customer_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_year, customer_name, sum(total_profit) as total_profit from pei_assesment.agrregate_orders group by customer_name, order_year order by order_year, customer_name

# COMMAND ----------

tables = ["raw_orders", "raw_customers", "raw_products",
          "enriched_customers", "enriched_products",
          "enriched_order", "agrregate_orders"]

summary = {t: spark.table(f"pei_assesment.{t}").count() for t in tables}
dbutils.notebook.exit(json.dumps(summary))