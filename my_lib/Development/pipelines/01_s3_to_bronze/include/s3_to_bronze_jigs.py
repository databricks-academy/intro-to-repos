# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC ## data flow from s3 --> bronze table:<br />
# MAGIC 
# MAGIC generic porcess to bring all JIGS data from s3 bucket to bronze by the following steps:<br />
# MAGIC  1.bring the data from s3 buckets with selected format to dataframe<br /> 
# MAGIC  3.the data will be ingested as whole (insert the entire json to string ) with some minor technical enrichment<br />
# MAGIC  4.append data to existing bronze table<br />
# MAGIC  5.update process update time and table update time in **dev_config.tableschema** and **lab_config.processUpdate**

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------


dbutils.widgets.combobox("File_Format", 'csv', ['csv', 'json','parquet','text'])


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### define local params

# COMMAND ----------

topic = "jigs"
data_layer = "bronze"
process_name = "{}_to_bronze".format(topic)
# src_file_format = dbutils.widgets.get("File_Format") #insert the desired file format
src_file_format = 'text' #insert the desired file format

src_bucket_region = 'us-east-1'
src_env = "lab"
dest_env = "lab" #to discuss with ps databricks regarding moving to prod (CI/CD)
file_name_source = ["mes.json","grade_sheet.json"] #uses each file name to create a diffrent table

#for jigs
source_bucket = "s3://jarvis-manufacturing-us-dv1/" 
source_env = "Data_From_Jigs/"
s3_mnt_bucket_source = 's3a://jarvis-manufacturing-us-dv1/'
dest_bucket = "s3://jarvis-databricks-db-bronze-use1-lab/" 
s3_mnt_bucket_dest = 's3a://jarvis-databricks-db-bronze-use1-lab/'
file_name = ['mes.json','grade_sheet.json']


logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)
executionId = str(uuid.uuid4())
logging.warning("ExecutionId : {}".format(executionId))

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

tables = source_table_list(mnt_source) #manage table that will keep all topic and tables


for table in tables: 
  for file_name in file_name_source:
  

    spark.sql("create database if not exists {}_bronze".format(dest_env))
    #define variables
    source_path = mnt_source+'/'+table
    short_file_name = file_name.rsplit('.', 1)[0]
    checkpoint_path = "{}{}_{}.db/checkpoint/{}_{}/".format(dest_bucket,dest_env,data_layer,topic,table)
    dest_path = "{}{}_bronze.db/{}_{}/".format(dest_bucket,dest_env,topic,table.lower())
#     table_name = "{}_bronze.{}_{}_{}".format(dest_env,make_first_capital(topic),make_first_capital(table),make_first_capital(short_file_name))
    table_name = "{}_bronze.{}_{}_{}".format(dest_env,make_first_capital(topic),table,make_first_capital(short_file_name))
    table_schema = determineSchema(topic,table,src_file_format,source_path,dest_path)
    
    try:
      #stream function can be found in utils
      run_process = data_exist(table_name,source_path,file_name)
      if run_process:
        df=read_stream(src_file_format,src_bucket_region,table,topic)
        transform = enrich_write_schema(df)
        write_stream(checkpoint_path,table_name,transform,topic,file_name)
      else:
        logging.warning("no table {} or data exits in S3".format(dest_env))
        




    except Exception as err:
      print(err)
      
    logging.warning("update process in {}_config".format(dest_env))
    if run_process:
      try:
        spark.sql("select last_run_date from  {}_config.tablesSchema WHERE topic = '{}' and table_name = '{}'".format(dest_env,topic,table)).collect()[0][0]
        spark.sql("UPDATE {}_config.tablesSchema SET last_run_date = current_timestamp() WHERE topic = '{}' and table_name = '{}'".format(dest_env,topic,table))


      except Exception as err:
        print("process doesnt exist in {}_config.tablesSchema".format(dest_env))
        logging.warning("insert new table: {} from {} source into {}_config.tablesSchema".format(dest_env,table,topic))
        spark.sql("INSERT INTO {}_config.tablesSchema VALUES ('{}', '{}','append',\'',\'')".format(dest_env,topic,table))
        spark.sql("UPDATE {}_config.tablesSchema SET last_run_date = current_timestamp() WHERE topic = '{}' and table_name = '{}'".format(dest_env,topic,table))
      else:
        logging.warning("no table {} or data exits in S3".format(dest_env))

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
  logging.warning("insert new process: {} into {}_config.processUpdate".format(process_name,dest_env))
  spark.sql("INSERT INTO  {}_config.processUpdate VALUES ('{}',\'')".format(dest_env,process_name))
  spark.sql("UPDATE {}_config.processUpdate SET last_run_date = current_timestamp() WHERE process_name = '{}' ".format(dest_env,process_name))

# COMMAND ----------


