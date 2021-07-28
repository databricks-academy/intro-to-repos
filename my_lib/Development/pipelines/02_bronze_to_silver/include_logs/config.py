# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration 

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------


logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)
executionId = str(uuid.uuid4())
#logging.warning("ExecutionId : {}".format(silver_scannerlogsexecutionId))

# COMMAND ----------

def mount_exists(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
      print("mount exists, removing the mount point")
      dbutils.fs.unmount(str_path)

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, IntegerType, DateType, StringType, DecimalType , BooleanType , StructField , LongType ,TimestampType
from pyspark.sql.functions import lower, col

# COMMAND ----------


#elastic_schema = (StructType().add("@timestamp", StringType()).add("agent", StructType()).add("application", StringType()).add("log", StructType()).add("processed", StructType()))



# COMMAND ----------

#elastic_schema = (StructType().add("@timestamp", StringType()).add("agent", StringType()).add("application", StringType()).add("log", StringType()).add("processed", StringType()))


# COMMAND ----------

#file_path_delta_scannerlogs = 's3://jarvis-databricks-delta-tables-bucket-dv1/dev/bronze/scannerlogs/'


# COMMAND ----------

#file_path_delta_scannerlogs = '/mnt/scannerlogs_bronz'
#scannerlogs_bronz = '/mnt/scannerlogs_bronz'
src_bucket_region = 'us-east-1'
File_path_getlogs ='s3://jarvis-elasticdump-us-dv1/elasticdump/' #  s3://jarvis-elasticdump-us-dv1/elasticdump/'  why is mount mot working ? '/mnt/scannerlogs/', can 
#file_path_delta = '/mnt/delta/'

# COMMAND ----------

#dbutils.fs.mount(f's3a://jarvis-databricks-db-red-use1-lab/lab_red.db/', f'/mnt/red_lab_db')


# COMMAND ----------

db_name='lab_red'
table_name_bronze='bronze_scannerlogs'
table_name_silver='silver_scannerlogs'
table_bronze=db_name+'.'+table_name_bronze
table_silver=db_name+'.'+table_name_silver

dest_bucket='/mnt/red_lab_db'
checkpoint_path=dest_bucket+'/scannerlogs/checkpoint/'
checkpoint_table_path_bronze=checkpoint_path+table_name_bronze
checkpoint_table_path_silver=dest_bucket+'/'+table_name_silver+'/_checkpoint/'

value_schema = (StructType().add("value", StringType()))

#silver_scannerlogs

# COMMAND ----------

jsonSchema="StructType(List(StructField(@timestamp,StringType,true),StructField(agent,StructType(List(StructField(hostname,StringType,true))),true),StructField(application,StringType,true),StructField(log,StructType(List(StructField(file,StructType(List(StructField(path,StringType,true))),true))),true),StructField(processed,StructType(List(StructField(DateTime,StringType,true),StructField(LineNumber,StringType,true),StructField(Severity,StringType,true),StructField(Source,StringType,true),StructField(Thread,StringType,true),StructField(message,StringType,true))),true)))"

# COMMAND ----------

table_schema_quarantine=' StructType(List(StructField(exec_Id,StringType,true),StructField(load_Time,TimestampType,true),StructField(P_loadDate,DateType,true),StructField(timestamp,TimestampType,true),StructField(agent_host,StringType,true),StructField(application,StringType,true),StructField(log_file,StringType,true),StructField(processed_DateTime,TimestampType,true),StructField(processed_LineNumber,IntegerType,true),StructField(processed_Severity,StringType,true),StructField(processed_Source,StringType,true),StructField(processed_Thread,StringType,true),StructField(processed_message,StringType,true),StructField(loadDate,DateType,true),StructField(P_logDate,DateType,true),StructField(_corrupted,StringType,true)))'

# COMMAND ----------

#dbutils.fs.mounts()

# COMMAND ----------

# dbutils.fs.mount(f's3a://jarvis-elasticdump-us-dv1/elasticdump', f'/mnt/scannerlogs')
# dbutils.fs.mount(f's3a://jarvis-databricks-delta-tables-bucket-dv1', f'/mnt/delta')
#dbutils.fs.mount(f's3a://jarvis-databricks-db-bronze-use1-lab/bronze/lab/scannerlogs', f'/mnt/scannerlogs_bronz')



# COMMAND ----------

#%fs mounts

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --CREATE DATABASE IF NOT EXISTS dev_bronze
# MAGIC ----create DATABASE Silver
# MAGIC ---DROP { DATABASE | SCHEMA } [ IF EXISTS ] dbname [ RESTRICT | CASCADE ]
# MAGIC --- create table 
# MAGIC ---DROP TABLE IF EXISTS sandboxM.oneLog

# COMMAND ----------


