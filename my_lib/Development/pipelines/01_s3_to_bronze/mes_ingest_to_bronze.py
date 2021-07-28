# Databricks notebook source
# MAGIC %md
# MAGIC ## data flow from MES s3 --> bronze table:<br />
# MAGIC this process uses the generic process of data from s3 --> bronze
# MAGIC with relevant paramteres 

# COMMAND ----------

# MAGIC %run ./include/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### define parameters relevant to the MES process

# COMMAND ----------

dbutils.widgets.combobox("File_Format", 'csv', ['csv', 'json','parquet','text'])
topic = "mes"
data_layer = "bronze"
process_name = "{}_to_bronze".format(topic)
src_file_format = dbutils.widgets.get("File_Format") #insert the desired file format
src_bucket_region = 'us-east-1'
dest_privacy  = 'green'
dest_env = "lab" 


source_bucket = "s3://jarvis-test-us-dv1/" 
source_env = "jarvis_bi/DEV/DEV/"
s3_mnt_bucket_source = 's3a://jarvis-test-us-dv1/'
dest_bucket = "s3://jarvis-databricks-db-bronze-use1-lab/" 
s3_mnt_bucket_dest = 's3a://jarvis-databricks-db-bronze-use1-lab/'





logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)
executionId = str(uuid.uuid4())
logging.warning("ExecutionId : {}".format(executionId))

# COMMAND ----------

# MAGIC %run ./include/s3_to_bronze

# COMMAND ----------


