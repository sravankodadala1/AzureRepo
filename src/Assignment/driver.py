# Databricks notebook source
# MAGIC %run '/Users/sravankumar04032001@gmail.com/bronze_to_silver/Utils'

# COMMAND ----------

# DBTITLE 1,Customer file

from pyspark.sql.types import *
from pyspark.sql.functions import *

options={'header':'true','delimiter':',','inferSchema':'True'}
# Reading the CSV file into a DataFrame using the specified options
df_customer=read("csv", "dbfs:/mnt/assign/Bronze/sales_view/customer/20240107_sales_customer.csv", options )

a=udf(snake_case,StringType())
lst = list(map(lambda x:a(x),df_customer.columns))
df_customer = df_customer.toDF(*lst)
# Formatting the 'date' column to a consistent date format
df_customer = format_date_column(df_customer, 'date', input_format='dd-MM-yyyy', output_format='yyyy-MM-dd')
# Splitting the 'name' column into 'first_name' and 'last_name'
df_customer = split_name_column(df_customer, 'name', 'first_name', 'last_name')
# Extracting the 'domain' from the 'email_id' column using specified delimiters
df_customer = extract_domain_column(df_customer, 'email_id', 'domain', '@', '\.')
# Creating a 'gender' column based on the values in the 'gender' column
df_customer = create_gender_column(df_customer, 'gender', 'gender')
# Splitting the 'joining_date' column into 'date' and 'time' columns
df_customer = split_joining_date_column(df_customer, 'joining_date', 'date', 'time')
# Creating an 'expenditure_status' column based on the 'spent' column values
df_customer = create_expenditure_status_column(df_customer, 'spent', 'expenditure_status')
# Formatting the 'date' column
df_customer = format_date_column(df_customer, 'date', input_format='dd-MM-yyyy', output_format='yyyy-MM-dd')

save_as_delta(df_customer, 'dbfs:/mnt/assign/Silver', 'sales_view', 'customer', ['employee_id'])


# COMMAND ----------

# DBTITLE 1,Product file
options={'header':'true','delimiter':',','inferSchema':'True'}
df_product=read("csv", "dbfs:/mnt/assign/Bronze/sales_view/product/20240105_sales_product.csv", options )

a=udf(snake_case,StringType())
lst = list(map(lambda x:a(x),df_product.columns))
df_product = df_product.toDF(*lst)

# Adding a 'sub_category' column to the product DataFrame based on 'category_id'
category_column_name = 'category_id'
sub_category_column_name = 'sub_category'
df_product = add_sub_category_column(df_product, category_column_name, sub_category_column_name)

save_as_delta(df_product, 'dbfs:/mnt/assign/Silver', 'sales_view', 'store', ['product_id'])


# COMMAND ----------

# DBTITLE 1,Store file
options={'header':'true','delimiter':',','inferSchema':'True'}
df_store=read("csv", "dbfs:/mnt/assign/Bronze/sales_view/store/20240105_sales_store.csv", options )

a=udf(snake_case,StringType())
lst = list(map(lambda x:a(x),df_store.columns))
df_store = df_store.toDF(*lst)
# Extracting the domain from the 'email_address' column using specified delimiters
df_store = extract_domain_column(df_store, 'email_address', '@','\.')
# Formatting the 'created_at' column to a date format
df_store = format_date_column(df_store, 'created_at', input_format='dd-MM-yyyy', output_format='yyyy-MM-dd')

save_as_delta(df_store, 'dbfs:/mnt/assign/Silver', 'sales_view', 'store', ['store_id'])


# COMMAND ----------

# DBTITLE 1,Sales file
options={'header':'true','delimiter':',','inferSchema':'True'}
df_sales=read("csv", "dbfs:/mnt/assign/Bronze/sales_view/sales/20240107_sales_data.csv", options )

a=udf(snake_case,StringType())
lst = list(map(lambda x:a(x),df_sales.columns))
df_store = df_sales.toDF(*lst)

save_as_delta(df_sales, 'dbfs:/mnt/assign/Silver', 'sales_view', 'sales', ['sales_id'])
