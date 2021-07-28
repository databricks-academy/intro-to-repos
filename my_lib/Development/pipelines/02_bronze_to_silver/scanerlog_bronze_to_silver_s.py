# Databricks notebook source
# DBTITLE 1,Stream Data from table lab_red.bronze_scannerlogs to table lab_red.silver_scannerlogs


# COMMAND ----------

# MAGIC %run ./include_logs/config

# COMMAND ----------

#print(checkpoint_table_path_silver)

# COMMAND ----------

# DBTITLE 1,get schema for the json data
df_logs_test=spark.read.format("json").option("inferschema", "true").load(File_path_getlogs+'/date=2021-01-18/hour=00/minute=00-00/')

jsonSchema = df_logs_test.schema

# COMMAND ----------

#df_logs= spark.sql("select * from lab_red.bronze_scannerlogs where P_loadDate >'2021-05-09' ")

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC --insert into lab_config.tablesschema VALUES ('scannerlogs' , 'bronze_scannerlogs', 'value' , 'StructType(List(StructField(@timestamp,StringType,true),StructField(agent,StructType(List(StructField(hostname,StringType,true))),true),StructField(application,StringType,true),StructField(log,StructType(List(StructField(file,StructType(List(StructField(path,StringType,true))),true))),true),StructField(processed,StructType(List(StructField(DateTime,StringType,true),StructField(LineNumber,StringType,true),StructField(Severity,StringType,true),StructField(Source,StringType,true),StructField(Thread,StringType,true),StructField(message,StringType,true))),true)))', current_timestamp()	, 'bronze' )

# COMMAND ----------


#table_schema_df = spark.sql("select table_schema from lab_config.tablesschema where topic='scannerlogs' and  table_name ='bronze_scannerlogs'")

# COMMAND ----------

#table_schema_row= table_schema_df.select("table_schema").collect()

# COMMAND ----------

#table_schema = table_schema_row[0][0]

# COMMAND ----------

#print(jsonSchema)

# COMMAND ----------

#print(table_schema)

# COMMAND ----------

#print(jsonSchema)

# COMMAND ----------

#df_logs= spark.sql("select * from lab_red.bronze_scannerlogs")

# COMMAND ----------

# DBTITLE 1,run readStream
bronze_df=spark.readStream.format("delta").option("startingTimestamp", "2021-05-15").table("lab_red.bronze_scannerlogs")

# COMMAND ----------

# bronze_df_with_value=

# COMMAND ----------

# DBTITLE 1,transformations flat and drop
bronze_df_parsed = bronze_df.withColumn("value_json",from_json("value",jsonSchema)).\
                  withColumn("timestamp", col("value_json.@timestamp").cast(TimestampType())).\
                  withColumn("agent_host", col("value_json.agent.hostname")).\
                  withColumn("application", col("value_json.application")).\
                  withColumn("log_file", col("value_json.log.file.path")).\
                  withColumn("processed_DateTime", col("value_json.processed.DateTime").substr(0,23).cast(TimestampType())).\
                  withColumn("processed_LineNumber", col("value_json.processed.LineNumber").cast(IntegerType())).\
                  withColumn("processed_Severity", col("value_json.processed.Severity")).\
                  withColumn("processed_Source", col("value_json.processed.Source")).\
                  withColumn("processed_Thread", col("value_json.processed.Thread")).\
                  withColumn("processed_message", col("value_json.processed.message")).\
                  withColumn("loadDate", col("P_loadDate")).\
                  withColumn("P_logDate",col("timestamp").cast(DateType())).\
                  drop("value_json").\
                  drop("value")

# COMMAND ----------

#display(bronze_df_parsed)

# COMMAND ----------

# DBTITLE 1,write the stream to the table
write = (bronze_df_parsed.writeStream.format("delta").queryName("silver").trigger(once=True).option("checkpointLocation", checkpoint_table_path_silver).table("lab_red.silver_scannerlogs"))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE lab_red.silver_scannerlogs  ZORDER BY (agent_host);

# COMMAND ----------

#p_date = spark.sql('select max(P_logDate) as date from lab_red.silver_scannerlogs' )

# COMMAND ----------

#date = p_date.select("date").collect().row


# COMMAND ----------

#print(date)

# COMMAND ----------

#%sql
#OPTIMIZE lab_red.silver_scannerlogs  where P_logDate = date  ZORDER BY (agent_host);

# COMMAND ----------


