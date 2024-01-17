# Databricks notebook source
from pyspark.sql.functions import col, split, substring, when, date_format, to_date

# Function to mount a layer
def mounting_layer_json(source,mountpoint,key,value):
    dbutils.fs.mount(
    source=source,
    mount_point= mountpoint,
    extra_configs={key:value})

# Function to convert a string to snake_case
def snake_case(x):
    a = x.lower()
    return a.replace(' ','_')

# Function to split a name column into first and last names
def split_name_column(df, name_column, first_name_col, last_name_col, delimiter=" "):
    return df.withColumn(first_name_col, split(col(name_column), delimiter)[0]) \
             .withColumn(last_name_col, split(col(name_column), delimiter)[1])

# Function to extract domain from an email column
def extract_domain_column(df, email_column, domain_col, split_char1, split_char2):
    return df.withColumn(domain_col, split(col(email_column), split_char1)[1]).withColumn(domain_col, split(col(domain_col), split_char2)[0])

# Function to create a gender column based on a specified column
def create_gender_column(df, gender_column, gender_col, male_value="male", female_value="female"):
    return df.withColumn(gender_col, when(col(gender_column) == male_value, "M").otherwise("F"))

# Function to split and format a joining date column
def split_joining_date_column(df, joining_date_column, date_col, time_col, date_delimiter=" ", time_delimiter=" "):
    return df.withColumn(date_col, split(col(joining_date_column), date_delimiter)[0]) \
             .withColumn(time_col, split(col(joining_date_column), time_delimiter)[1])

# Function to create an expenditure status column based on a specified spent column
def create_expenditure_status_column(df, spent_column, expenditure_status_col, value=200, minimum_label="MINIMUM", maximum_label="MAXIMUM"):
    return df.withColumn(expenditure_status_col, when(col(spent_column) < value, minimum_label).otherwise(maximum_label))

# Function to format a date column
def format_date_column(df, date_column, input_format, output_format):
    return df.withColumn(date_column, date_format(to_date(col(date_column), input_format), output_format))

# Function to add a 'sub_category' column based on values in the 'category_col'
def add_sub_category_column(df, category_col, sub_category_col):
    return df.withColumn(sub_category_col,
                        when(col(category_col) == 1, 'phone').
                        when(col(category_col) == 2, 'laptop').
                        when(col(category_col) == 3, 'playstation').
                        when(col(category_col) == 4, 'e-device').
                        otherwise('unknown')
                        )
    
def save_as_delta(res_df, path, db_name, tb_name, mergecol):
    base_path = path + f"{db_name}/{tb_name}"
    mappedCol = " AND ".join(list(map((lambda x: f"old.{x} = new.{x} "),mergecol)))
    if not DeltaTable.isDeltaTable(spark, f"{base_path}"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        res_df.write.mode("overwrite").format("delta").option("path", base_path).saveAsTable(f"{db_name}.{tb_name}")
    else:
            deltaTable = DeltaTable.forPath(spark, f"{base_path}")
            deltaTable.alias("old").merge(res_df.alias("new"),mappedCol).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
