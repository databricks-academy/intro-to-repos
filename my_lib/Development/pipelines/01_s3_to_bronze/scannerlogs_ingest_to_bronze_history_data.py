# Databricks notebook source
# MAGIC %md
# MAGIC ## run config and all linked functiones

# COMMAND ----------

# MAGIC %run ./include_logs/config

# COMMAND ----------

# MAGIC %sql 
# MAGIC --use lab_red;
# MAGIC --CREATE TABLE if not EXISTS bronze_scannerlogs  ( `value` STRING, `exec_Id` STRING, `load_Time` TIMESTAMP , `P_loadDate` DATE) USING delta PARTITIONED BY (P_loadDate);

# COMMAND ----------

# DBTITLE 1,set up dates to load from
running_date= date_range('2021-05-04', '2021-05-06')

# COMMAND ----------

# DBTITLE 1,Insert log data to string by range of dates
for date_logs in running_date:
  print("running logs for {}".format(date_logs))
  date_log_str = date_logs.strftime('%Y-%m-%d')
#   date_log_count = 0
    
  file_path = File_path_getlogs+'date='+date_log_str+'/'
  print("reading data from : {}".format(file_path))
  try:
    df_logs=spark.read.format("text").load(file_path)
    transfrom_df = df_logs.select("value").\
                  withColumn("exec_Id",F.lit(executionId).cast(StringType())).\
                  withColumn("load_Time",F.current_timestamp()).\
                  withColumn("P_loadDate", F.lit(date_logs).cast(DateType()))
                  
                  

    transfrom_df.write.mode("append").format("delta").saveAsTable("lab_red.bronze_scannerlogs")

    
  except Exception as err:
    print(err)

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from lab_red.bronze_scannerlogs limit (10)

# COMMAND ----------

#df_logs_test=spark.read.format("json").option("inferschema", "true").load(File_path_getlogs+'/date=2021-01-18/hour=00/minute=00-00/')
#df_logs_test.show(1)


# COMMAND ----------

#df_logs_test.printSchema()

