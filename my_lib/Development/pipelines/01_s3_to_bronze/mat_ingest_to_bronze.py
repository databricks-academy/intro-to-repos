# Databricks notebook source
# MAGIC %md
# MAGIC ## data flow from MAT s3 --> bronze table:<br />
# MAGIC this process uses the generic process of data from s3 --> bronze
# MAGIC with relevant paramteres 

# COMMAND ----------

# MAGIC %run ./include/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### define parameters relevant to the MAT process

# COMMAND ----------

dbutils.widgets.combobox("File_Format", 'csv', ['csv', 'json','parquet'])
topic = "mat"
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

dbutils.widgets.help()

# COMMAND ----------

table = 'contact'
spark.sql('select load_Time,count(1) from lab_bronze.mat_{} group by 1 order by 1'.format(table)).show()
spark.sql('select count(1) from lab_bronze.mat_{}'.format(table)).show()

# COMMAND ----------

path='s3://jarvis-test-us-dv1/jarvis_bi/DEV/DEV/MAT/Resources/'

spark.read.option('format','parquet').option('mergeSchema','true').option('recursiveFileLookup','true').load(path).count()

# COMMAND ----------

def read_stream(src_file_format,src_bucket_region,table,topic,src_file_delimiter=","):
  print("loading {} from {} using {} ".format(table,topic,src_file_format))
  if src_file_format == "csv":
    df=   spark.readStream.\
      format('cloudFiles').\
      option('recursiveFileLookup','true').\
      option("delimiter", src_file_delimiter).\
      option("header", "true").\
      option('cloudFiles.format', src_file_format).\
      option('cloudFiles.region', src_bucket_region).\
      option('cloudFiles.maxFilesPerTrigger', 1).\
      option('cloudFiles. useNotifications','true').\
      option("cloudFiles.validateOptions", "false").\
      option('cloudFiles.includeExistingFiles','true').\
      schema(table_schema).\
      load(source_path)

  elif src_file_format == "parquet":
    df=   spark.readStream.\
      format('cloudFiles').\
      option('recursiveFileLookup','true').\
      option('cloudFiles.format', src_file_format).\
      option('cloudFiles.region', src_bucket_region).\
      option('cloudFiles.maxFilesPerTrigger', 1).\
      option('cloudFiles. useNotifications','true').\
      option("cloudFiles.validateOptions", "false").\
      option('cloudFiles.includeExistingFiles','true').\
      schema(table_schema).\
      load(source_path)

  elif src_file_format == "text":
    df=   spark.readStream.\
        format('cloudFiles').\
        option('recursiveFileLookup','true').\
        option('pathGlobFilter',file_name). \
        option('multiLine','true'). \
        option('wholetext','true') .\
        option('cloudFiles.maxFilesPerTrigger', 1).\
        option('cloudFiles.format', src_file_format).\
        option("cloudFiles.validateOptions", "false").\
        option('cloudFiles.includeExistingFiles','true').\
        option('cloudFiles.region', src_bucket_region).\
        option('cloudFiles. useNotifications','true').\
        schema(table_schema).\
        load(source_path)

  elif src_file_format == "json":
    print("not used")
  else:
    print("no valid file format was presented")
  return df
  
  
  
def enrich_write_schema(df):
  logging.warning("enrich dataframe with create date and exec_id and file path")
  transform = df.select("*").\
                    withColumn("exec_Id",F.lit(executionId).cast(StringType())).\
                    withColumn("load_Time",F.current_timestamp()).\
                    withColumn("filename", input_file_name())
  return transform

def write_stream(checkpoint_path,table_name,transform,topic='',file_name=''):
  if topic == 'jigs':
    logging.warning("write data to destination path")
    transform.\
            writeStream.\
            format('delta').\
            option('mergeSchema', 'true') .\
            option('checkpointLocation',checkpoint_path+'_'+file_name).\
            table(table_name).\
            awaitTermination()
  else:
    logging.warning("write data to destination path")
    transform.\
            writeStream.\
            format('delta').\
            option('mergeSchema', 'true') .\
            option('checkpointLocation',checkpoint_path).\
            table(table_name).\
            awaitTermination()

# COMMAND ----------


