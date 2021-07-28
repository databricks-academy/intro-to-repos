# Databricks notebook source
# MAGIC %md
# MAGIC ## data flow from JIGS s3 --> bronze table:<br />
# MAGIC this process uses the generic process of data from s3 --> bronze
# MAGIC with relevant paramteres for JIGS to bring the data to bronze tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### define parameters relevant to the JIGS process

# COMMAND ----------

dbutils.widgets.combobox("File_Format", 'csv', ['csv', 'json','parquet'])
topic = "jigs"
data_layer = "bronze"
process_name = "{}_to_bronze".format(topic)
src_file_format = dbutils.widgets.get("File_Format") #insert the desired file format
src_bucket_region = 'us-east-1'
dest_privacy  = 'green'
dest_env = "lab" 

#for jigs
source_bucket = "s3://jarvis-manufacturing-us-dv1/" 
source_env = "Data_From_Jigs/"
s3_mnt_bucket_source = 's3a://jarvis-manufacturing-us-dv1/'
dest_bucket = "s3://jarvis-databricks-db-bronze-use1-lab/" 
s3_mnt_bucket_dest = 's3a://jarvis-databricks-db-bronze-use1-lab/'


logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)
executionId = str(uuid.uuid4())
logging.warning("ExecutionId : {}".format(executionId))

# COMMAND ----------

# MAGIC %md
# MAGIC ## END OF PROCESS

# COMMAND ----------

# MAGIC %run ./include_logs/config

# COMMAND ----------

#dbutils.fs.ls("/databricks-datasets/")
with open("/dbfs/databricks-datasets/README.md") as f:
    x = ''.join(f.readlines())
print(x)

# COMMAND ----------

# dbutils.fs.mount("s3a://jarvis-manufacturing-us-dv1/Data_From_Jigs", "/mnt/data-from-jigs")
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data-from-jigs/

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data-from-jigs/Projector_Tester/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/data-from-jigs/Projector_Tester/BA4800XX/BA480002/2021-02-07T12-22-15+02/result/

# COMMAND ----------

from pyspark.sql.functions import input_file_name
Projector_Tester = spark.read. \
  option("pathGlobFilter","mes.json"). \
  option("recursiveFileLookup","true"). \
  option("multiLine","true"). \
  json("/mnt/data-from-jigs/Projector_Tester/"). \
  withColumn("filename", input_file_name())

Projector_Tester.printSchema()

# COMMAND ----------

display(Projector_Tester.limit(2))

# COMMAND ----------


