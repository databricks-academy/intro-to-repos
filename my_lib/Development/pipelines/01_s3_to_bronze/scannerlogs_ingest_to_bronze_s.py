# Databricks notebook source
# MAGIC %md
# MAGIC ## run config and all linked functiones

# COMMAND ----------

# MAGIC %run ./include_logs/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## run  readStream  

# COMMAND ----------

logging.warning(" run readstream from s3 files - from elastic  ")
print("run readstream from s3 files - from elastic")

df=   spark.readStream.\
            format('cloudFiles').\
            option('recursiveFileLookup','true').\
            option('cloudFiles.format', 'text').\
            option('cloudFiles.includeExistingFiles','false').\
            schema(value_schema).\
            load(File_path_getlogs)


# COMMAND ----------

# MAGIC %md
# MAGIC ## transformations to change the dataframe   

# COMMAND ----------

# DBTITLE 1,transformations 

logging.warning("enrich dataframe with create date and exec_id + Partition  ")
print ("enrich dataframe with create date and exec_id + Partition  ")
transfrom_df = df.select("value").\
                  withColumn("exec_Id",F.lit(executionId).cast(StringType())).\
                  withColumn("load_Time",F.current_timestamp()).\
                   withColumn("P_loadDate", F.lit(F.current_date()).cast(DateType()))                  


# COMMAND ----------

# MAGIC %md
# MAGIC ## more transformations to add the partition date  - col P_logDate  

# COMMAND ----------

# MAGIC %md
# MAGIC ## write the stream to the table  

# COMMAND ----------

logging.warning("write data  queryName - write_scanerlog_to_bronze ")
write = transfrom_df.\
          writeStream.\
          trigger(once=True).\
          format('delta').\
          option('checkpointLocation',checkpoint_table_path_bronze).\
          queryName("write_scanerlog_to_bronze").\
          table (table_bronze).\
          awaitTermination()
