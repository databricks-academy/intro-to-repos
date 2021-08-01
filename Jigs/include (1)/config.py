# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration 

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------

def mount_exists(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
      print("mount exists, removing the mount point")
      dbutils.fs.unmount(str_path)

# COMMAND ----------

file_path_delta_scannerlogs = 's3://jarvis-databricks-delta-tables-bucket-dv1/dev/bronze/scannerlogs/'


# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# dbutils.fs.mount(f's3a://jarvis-elasticdump-us-dv1/elasticdump', f'/mnt/scannerlogs')
# dbutils.fs.mount(f's3a://jarvis-databricks-delta-tables-bucket-dv1', f'/mnt/delta')




# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS dev_bronze
# MAGIC ----create DATABASE Silver
# MAGIC ---DROP { DATABASE | SCHEMA } [ IF EXISTS ] dbname [ RESTRICT | CASCADE ]
# MAGIC --- create table 
# MAGIC ---DROP TABLE IF EXISTS sandboxM.oneLog

# COMMAND ----------


