# Databricks notebook source
# MAGIC %md
# MAGIC # ABAC - Row Filtering and Column Masking
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop schema if exists dkushari_uc.fgac_abac cascade;
# MAGIC create schema if not exists dkushari_uc.fgac_abac managed location "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/dkushari/f_g_a_c/fgac_abac/";

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dkushari_uc;
# MAGIC use fgac_abac;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_schema();

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS customer_pii_data_delta_1;
# MAGIC DROP TABLE IF EXISTS customer_pii_data_delta_2;

# COMMAND ----------

from pyspark.sql.functions import expr, lit, rand
from pyspark.sql.types import IntegerType

# Generate a DataFrame with 50 records
df = spark.range(50).withColumnRenamed("id", "record_id")

# Add PII information (dummy data for illustration)
df = df.withColumn("first_name", expr("CASE WHEN rand() < 0.2 THEN 'John' " +
                                      "WHEN rand() < 0.4 THEN 'Jane' " +
                                      "WHEN rand() < 0.6 THEN 'Doe' " +
                                      "WHEN rand() < 0.8 THEN 'Alice' " +
                                      "ELSE 'Bob' END")) \
      .withColumn("last_name", expr("CASE WHEN rand() < 0.2 THEN 'Smith' " +
                                    "WHEN rand() < 0.4 THEN 'Johnson' " +
                                    "WHEN rand() < 0.6 THEN 'Williams' " +
                                    "WHEN rand() < 0.8 THEN 'Brown' " +
                                    "ELSE 'Jones' END")) \
      .withColumn("date_of_birth", expr("CASE WHEN rand() < 0.2 THEN '1980-01-01' " +
                                        "WHEN rand() < 0.4 THEN '1990-01-01' " +
                                        "WHEN rand() < 0.6 THEN '2000-01-01' " +
                                        "WHEN rand() < 0.8 THEN '1985-01-01' " +
                                        "ELSE '1995-01-01' END")) \
      .withColumn("age", (lit(2023) - expr("substring(date_of_birth, 1, 4)")).cast(IntegerType())) \
      .withColumn("gender", expr("CASE WHEN rand() < 0.5 THEN 'M' ELSE 'F' END")) \
      .withColumn("address", expr("CASE WHEN rand() < 0.2 THEN '123 Main St' " +
                                  "WHEN rand() < 0.4 THEN '456 Elm St' " +
                                  "WHEN rand() < 0.6 THEN '789 Pine St' " +
                                  "WHEN rand() < 0.8 THEN '101 Oak St' " +
                                  "ELSE '202 Maple St' END")) \
      .withColumn("ssn", expr("CASE WHEN rand() < 0.2 THEN '111-11-1111' " +
                              "WHEN rand() < 0.4 THEN '222-22-2222' " +
                              "WHEN rand() < 0.6 THEN '333-33-3333' " +
                              "WHEN rand() < 0.8 THEN '444-44-4444' " +
                              "ELSE '555-55-5555' END")) \
      .withColumn("region", expr("CASE WHEN rand() < 0.2 THEN 'Northeast' " +
                                 "WHEN rand() < 0.4 THEN 'Midwest' " +
                                 "WHEN rand() < 0.6 THEN 'South' " +
                                 "WHEN rand() < 0.8 THEN 'West' " +
                                 "ELSE 'Southwest' END"))

# Display the DataFrame
# display(df)

# Write the DataFrame to a Unity Catalog table
df.write.format("delta").mode("overwrite").option("path","s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/dkushari/f_g_a_c/fgac_abac/external-table/customer_pii_data_delta_1").saveAsTable("customer_pii_data_delta_1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_1 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into customer_pii_data_delta_1
# MAGIC select * from customer_pii_data_delta_1 limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into customer_pii_data_delta_1
# MAGIC select * from customer_pii_data_delta_1 where first_name = 'Doe' and last_name = 'Williams'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from customer_pii_data_delta_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists customer_pii_data_delta_2 deep clone customer_pii_data_delta_1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Table with PII data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, COUNT(*) AS total_customers
# MAGIC FROM customer_pii_data_delta_1
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, COUNT(*) AS total_customers
# MAGIC FROM customer_pii_data_delta_2
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dkushari_uc;
# MAGIC use fgac_abac;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create ABAC Row Level Filtering UC functions 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add tags to the table columns

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER  TABLE customer_pii_data_delta_1
# MAGIC ALTER COLUMN ssn
# MAGIC SET TAGS('sensitive_pii' = 'ssn');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER  TABLE customer_pii_data_delta_2
# MAGIC ALTER COLUMN ssn
# MAGIC SET TAGS('sensitive_pii' = 'ssn');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER  TABLE customer_pii_data_delta_1
# MAGIC ALTER COLUMN address
# MAGIC SET TAGS('sensitive_pii' = 'address');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER  TABLE customer_pii_data_delta_2
# MAGIC ALTER COLUMN address
# MAGIC SET TAGS('sensitive_pii' = 'address');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER  TABLE customer_pii_data_delta_1
# MAGIC ALTER COLUMN region
# MAGIC SET TAGS('geo_region');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER  TABLE customer_pii_data_delta_2
# MAGIC ALTER COLUMN region
# MAGIC SET TAGS('geo_region');

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define the row filter function

# COMMAND ----------

# MAGIC %sql
# MAGIC drop function if exists abac_row_filter;
# MAGIC CREATE FUNCTION abac_row_filter (column_1 STRING, columne_value_1 STRING) 
# MAGIC RETURNS BOOLEAN
# MAGIC   RETURN column_1 <> columne_value_1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Define the Policy at the Schema level using the row filter function for group_1

# COMMAND ----------

# MAGIC %sql
# MAGIC drop policy hide_west_region_customers on schema dkushari_uc.fgac_abac;

# COMMAND ----------

# MAGIC %md
# MAGIC # Policy to Hide customers from West region for sensitive tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY hide_west_region_customers 
# MAGIC ON SCHEMA dkushari_uc.fgac_abac
# MAGIC COMMENT 'Hide customers from West region for sensitive tables'
# MAGIC ROW FILTER abac_row_filter
# MAGIC TO demo_fgac_group_1
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS 
# MAGIC   hasTag('geo_region') AS region  
# MAGIC USING COLUMNS(region, 'West');
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### User is part of group_1, which is only allowed to see the data after filter based on the function definition

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT is_account_group_member('demo_fgac_group_1') as group_1, is_account_group_member('demo_fgac_group_2') as group_2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_1 where region='West' limit 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_2 where region='West' limit 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_1 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_2 where region='Midwest' limit 3;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create ABAC column masking UC functions 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS abac_col_mask;
# MAGIC CREATE OR REPLACE FUNCTION abac_col_mask (column_1 STRING) 
# MAGIC RETURN '❌❌❌❌❌' ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop policy  mask_ssn on schema dkushari_uc.fgac_abac;
# MAGIC drop policy mask_address on schema dkushari_uc.fgac_abac;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE POLICY mask_ssn 
# MAGIC ON SCHEMA dkushari_uc.fgac_abac
# MAGIC COMMENT 'mask ssn value'
# MAGIC COLUMN MASK abac_col_mask
# MAGIC TO demo_fgac_group_1
# MAGIC EXCEPT demo_fgac_group_2
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS 
# MAGIC   hasTagValue('sensitive_pii','ssn') AS ssn  
# MAGIC ON COLUMN ssn;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE POLICY mask_address
# MAGIC ON SCHEMA dkushari_uc.fgac_abac
# MAGIC COMMENT 'mask the address'
# MAGIC COLUMN MASK abac_col_mask
# MAGIC TO demo_fgac_group_1
# MAGIC EXCEPT demo_fgac_group_2
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS 
# MAGIC   hasTagValue('sensitive_pii','address') AS address  
# MAGIC ON COLUMN address;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### User is part of group_1, hence the ssn and address data will be masked

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT is_account_group_member('demo_fgac_group_1') as group_1, is_account_group_member('demo_fgac_group_2') as group_2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_1 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_2 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_2 where region='West' limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### User is now part of both group_1 and group_2, so ssn and address data will not be masked, since **_EXCEPT_** takes precedence

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT is_account_group_member('demo_fgac_group_1') as group_1, is_account_group_member('demo_fgac_group_2') as group_2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_1 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_2 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_2 where region='West' limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Now lets update the ABAC row filter policy to include EXCEPT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY hide_west_region_customers 
# MAGIC ON SCHEMA dkushari_uc.fgac_abac
# MAGIC COMMENT 'Hide customers from West region for sensitive tables'
# MAGIC ROW FILTER abac_row_filter
# MAGIC TO demo_fgac_group_1
# MAGIC EXCEPT demo_fgac_group_2
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS 
# MAGIC   hasTag('geo_region') AS region  
# MAGIC USING COLUMNS(region, 'West');
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC -- Original Policy with no Except Clause
# MAGIC -- %sql
# MAGIC -- CREATE or REPLACE POLICY hide_west_region_customers 
# MAGIC -- ON SCHEMA dkushari_uc.fgac_abac
# MAGIC -- COMMENT 'Hide customers from West region for sensitive tables'
# MAGIC -- ROW FILTER abac_row_filter
# MAGIC -- TO demo_fgac_group_1
# MAGIC -- FOR TABLES
# MAGIC -- MATCH COLUMNS 
# MAGIC --   hasTag('geo_region') AS region  
# MAGIC -- USING COLUMNS(region, 'West');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_2 where region='West' limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history customer_pii_data_delta_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_1@v1 limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Removing the EXCEPT clause

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE POLICY hide_west_region_customers 
# MAGIC ON SCHEMA dkushari_uc.fgac_abac
# MAGIC COMMENT 'Hide customers from West region for sensitive tables'
# MAGIC ROW FILTER abac_row_filter
# MAGIC TO demo_fgac_group_1
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS 
# MAGIC   hasTag('geo_region') AS region  
# MAGIC USING COLUMNS(region, 'West');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_1@v1 limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS abac_col_mask_complex;
# MAGIC CREATE OR REPLACE FUNCTION abac_col_mask_complex (column_1 STRING, column_2 STRING, column_2_value STRING) 
# MAGIC RETURN IF (column_2=column_2_value, column_1, '❌❌❌❌❌') ;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop policy mask_ssn on schema dkushari_uc.fgac_abac;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE POLICY mask_ssn 
# MAGIC ON SCHEMA dkushari_uc.fgac_abac
# MAGIC COMMENT 'mask ssn value'
# MAGIC COLUMN MASK abac_col_mask_complex
# MAGIC TO demo_fgac_group_1
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS 
# MAGIC   hasTagValue('sensitive_pii','ssn') AS ssn
# MAGIC  ,hasTag('gender') as gender
# MAGIC ON COLUMN ssn
# MAGIC using columns(gender,'M');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_1 limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE POLICY mask_ssn 
# MAGIC ON SCHEMA dkushari_uc.fgac_abac
# MAGIC COMMENT 'mask ssn value'
# MAGIC COLUMN MASK abac_col_mask_complex
# MAGIC TO demo_fgac_group_1
# MAGIC FOR TABLES
# MAGIC MATCH COLUMNS 
# MAGIC   hasTagValue('sensitive_pii','ssn') AS ssn
# MAGIC  ,hasTag('gender') as gender
# MAGIC ON COLUMN ssn
# MAGIC using columns(gender,'');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_pii_data_delta_1 limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog dkushari_uc;
# MAGIC
# MAGIC use fgac_abac;
# MAGIC
# MAGIC drop table if exists customers_iceberg;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customers_iceberg (
# MAGIC   c_custkey BIGINT,
# MAGIC   c_name STRING,
# MAGIC   c_address STRING,
# MAGIC   c_nationkey BIGINT,
# MAGIC   c_phone STRING,
# MAGIC   c_acctbal DECIMAL(18, 2),
# MAGIC   c_mktsegment STRING,
# MAGIC   c_comment STRING
# MAGIC ) USING iceberg;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended customers_iceberg;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_iceberg;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into customers_iceberg select * from samples.tpch.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table samples.tpch.customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from samples.tpch.customer limit 10;