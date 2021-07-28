# Databricks notebook source
# MAGIC %run ./utilities

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### define shared params

# COMMAND ----------

# source_bucket = "s3://jarvis-test-us-dv1/" 
# # dest_bucket = "s3://jarvis-databricks-delta-tables-bucket-dv1/" 
# source_env = "jarvis_bi/DEV/DEV/"
# dest_env = "dv1"
# dest_privacy  = 'green'
# s3_mnt_bucket_source = 's3a://jarvis-test-us-dv1/'
# # s3_mnt_bucket_dest = 's3a://jarvis-databricks-delta-tables-bucket-dv1/'

# src_file_delimiter =  "|"
mnt_alias =  '/mnt/'


# COMMAND ----------

# MAGIC %sql
# MAGIC --data base for config tables
# MAGIC -- create database if not exists dev_config;
# MAGIC 
# MAGIC --tables for schema config and run date
# MAGIC 
# MAGIC --  CREATE TABLE  IF NOT EXISTS  dev_config.tablesSchema
# MAGIC  
# MAGIC --   (topic STRING , 
# MAGIC --   table_name STRING,
# MAGIC --   write_mode STRING,
# MAGIC --   table_schema STRING,
# MAGIC --   last_run_date timestamp )
# MAGIC --   USING delta 
# MAGIC -- LOCATION 's3://jarvis-databricks-delta-tables-bucket-dv1/dev/config/tablesSchema_1';
# MAGIC 
# MAGIC 
# MAGIC --  CREATE TABLE  IF NOT EXISTS  dev_config.processUpdate
# MAGIC --   (process_name STRING , 
# MAGIC --    last_run_date timestamp )
# MAGIC --    USING delta 
# MAGIC -- LOCATION 's3://jarvis-databricks-delta-tables-bucket-dv1/dev/config/processUpdate'

# COMMAND ----------


