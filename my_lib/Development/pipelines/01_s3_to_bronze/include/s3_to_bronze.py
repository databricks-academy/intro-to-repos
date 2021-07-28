# Databricks notebook source
# MAGIC %md
# MAGIC ## data flow from s3 --> bronze table:<br />
# MAGIC 
# MAGIC generic porcess to bring all small tables from s3 bucket to bronze by the following steps:<br />
# MAGIC  1.bring the data from s3 buckets with selected format to dataframe<br /> 
# MAGIC  2.enforce predifined schema for each table(table schema can be found under **lab_config.tableschema** ) <br />
# MAGIC  3.the data will be ingested as raw data with some minor technical enrichment<br />
# MAGIC  4.append table to existing bronze table<br />
# MAGIC  5.update process update time and table update time in **lab_config.tableschema** and **lab_config.processUpdate**

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

dbutils.widgets.combobox("File_Format", 'csv', ['csv', 'json','parquet','text'])


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### define local params

# COMMAND ----------

data_layer = "bronze"
process_name = "{}_to_bronze".format(topic)
src_file_format = dbutils.widgets.get("File_Format") #insert the desired file format
src_bucket_region = 'us-east-1'
dest_env = "lab" #to discuss with ps databricks regarding moving to prod (CI/CD)


logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)
executionId = str(uuid.uuid4())
logging.warning("ExecutionId : {}".format(executionId))

# COMMAND ----------

# dbutils.widgets.combobox("File_Format", 'csv', ['csv', 'json','parquet'])
# topic = "mat"
# data_layer = "bronze"
# process_name = "{}_to_bronze".format(topic)
# src_file_format = dbutils.widgets.get("File_Format") #insert the desired file format
# src_bucket_region = 'us-east-1'
# dest_privacy  = 'green'
# dest_env = "lab" 


# source_bucket = "s3://jarvis-test-us-dv1/" 
# source_env = "jarvis_bi/DEV/DEV/"
# s3_mnt_bucket_source = 's3a://jarvis-test-us-dv1/'
# dest_bucket = "s3://jarvis-databricks-db-bronze-use1-lab/" 
# s3_mnt_bucket_dest = 's3a://jarvis-databricks-db-bronze-use1-lab/'





# logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)
# executionId = str(uuid.uuid4())
# logging.warning("ExecutionId : {}".format(executionId))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### mount source and target

# COMMAND ----------

if topic == "jigs":
  #mount source
  mnt_source = mnt_alias+topic+'_source'
  path_source = s3_mnt_bucket_source+source_env #for jigs
#   mount_exists(mnt_source) #check mount
#   dbutils.fs.mount(path_source, mnt_source)

  
else:
  #mount source
  mnt_source = mnt_alias+topic+'_source'
  path_source = s3_mnt_bucket_source+source_env+topic.upper() #for VM
#   mount_exists(mnt_source) #check mount
#   dbutils.fs.mount(path_source, mnt_source)

  
#mount target
mnt_dest=mnt_alias+topic+'_dest'
path_dest=s3_mnt_bucket_dest+data_layer+'/'+dest_env+'/'+topic
# mount_exists(mnt_dest) 
# dbutils.fs.mount(path_dest, mnt_dest)
  






# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### read data from s3 and write to bronze table
# MAGIC by using structured streaming 

# COMMAND ----------

# paths = source_table_list(source_path)
# print(x)

# for path in paths:
#   print(path)
#   spark.read.parquet(source_path+'/'+ path).printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import lit

tables = source_table_list(mnt_source) #manage table that will keep all topic and tables


for table in tables:
  
  spark.sql("create database if not exists {}_bronze".format(dest_env))
  source_path = mnt_source+'/'+table
  checkpoint_path = "{}{}_{}.db/checkpoint/{}_{}/".format(dest_bucket,dest_env,data_layer,topic,table)
  dest_path = "{}{}_bronze.db/{}_{}/".format(dest_bucket,dest_env,topic,table.lower())
  table_name = "{}_bronze.{}_{}".format(dest_env,topic,table)
  table_schema = determineSchema(topic,table,src_file_format,source_path,dest_path)
  
  try:
    #stream function can be found in utils
    print(table_name)
    df=read_stream(src_file_format,src_bucket_region,table,topic)
    transform = enrich_write_schema(df)
#     transform = df.select("*").\
#                     withColumn("exec_Id",F.lit(executionId).cast(StringType())).\
#                     withColumn("load_Time",F.current_timestamp()).\
#                     withColumn("filename", input_file_name())
    
#     transform_1 = transform.select("*").\
#                         withColumn("extract_Time", str(extract_datetime_from_filename(F.lit("filename"))))
    

    write_stream(checkpoint_path,table_name,transform)




  except Exception as err:
    print(err)
      

    logging.warning("update process in {}_config".format(dest_env))
    try:
      spark.sql("select last_run_date from  {}_config.tablesSchema WHERE topic = '{}' and table_name = '{}'".format(dest_env,topic,table)).collect()[0][0]
      spark.sql("UPDATE {}_config.tablesSchema SET last_run_date = current_timestamp() WHERE topic = '{}' and table_name = '{}'".format(dest_env,topic,table))


    except Exception as err:
      print("process doesnt exist in {}_config.tablesSchema".format(dest_env))
      logging.warning("insert new table: {} from {} source into {}_config.tablesSchema".format(dest_env,table,topic))
      spark.sql("INSERT INTO {}_config.tablesSchema VALUES ('{}', '{}','append',\'',\'')".format(dest_env,topic,table))
      spark.sql("UPDATE {}_config.tablesSchema SET last_run_date = current_timestamp() WHERE topic = '{}' and table_name = '{}'".format(dest_env,topic,table))


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### update process end time

# COMMAND ----------

try:
  spark.sql("select process_name from  {}_config.processUpdate where process_name = '{}' ".format(dest_env,process_name)).collect()[0][0]
  spark.sql("UPDATE {}_config.processUpdate SET last_run_date = current_timestamp() WHERE process_name = '{}' ".format(dest_env,process_name))  
except Exception as err:
  print("process {} doesnt exist in {}_config.processUpdate".format(process_name,dest_env))
  logging.warning("insert new process: {} into {}_config.processUpdate".format(dest_env,process_name))
  spark.sql("INSERT INTO  {}_config.processUpdate VALUES ('{}',\'')".format(dest_env,process_name))
  spark.sql("UPDATE {}_config.processUpdate SET last_run_date = current_timestamp() WHERE process_name = '{}' ".format(dest_env,process_name))

# COMMAND ----------

# import re
# import string
# from datetime import datetime

# text = '/mnt/mat_source/Case_CaseTypes/20210405_112523/Case_CaseTypes_20210405_112523.parquet_ 2020-15-05'
# # match = re.search(r'\d{8}_\d{6}', text)
# # match = match.group()
# # print(match)
# # test = datetime.strptime(match, '%Y%m%d_%H%M%S')
# # print(test)

# def extract_datetime_from_filename(file_name):
#   file_datetime = '' 
#   try:
#     match = re.search(r'\d{8}_\d{6}', file_name)
#     match = match.group()
#     file_datetime = datetime.strptime(match, '%Y%m%d_%H%M%S')
#   except Exception as err:
#     print(err)   
#   return file_datetime



# testtime = extract_datetime_from_filename(text)
# print(testtime)


    
    
    

# COMMAND ----------

# table = 'Case_ScanInfo/'
# table = 'Contact/'
# # spark.read.option('format', 'parquet').option("s3://jarvis-test-us-dv1/jarvis_bi/DEV/DEV/MAT/Case_ScanInfo//").printSchema()
# spark.read.option('format', 'parquet').option("mergeSchema", "true").option('recursiveFileLookup','true').load(mnt_source+'/'+table+"/").printSchema()


# COMMAND ----------


