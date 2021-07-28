# Databricks notebook source
# MAGIC %run ./include_logs/config

# COMMAND ----------

df_logs_test=spark.read.format("json").option("inferschema", "true").load(File_path_getlogs+'/date=2021-01-18/hour=00/minute=00-00/')

jsonSchema = df_logs_test.schema

# COMMAND ----------

# schema = StructType([
#  StructField("country", StringType(), True),
#  StructField("count", StringType(), True)
# ])

# COMMAND ----------

testschema = StructType(List(StructField(@timestamp,StringType,true),StructField(agent,StructType(List(StructField(hostname,StringType,true))),true),StructField(application,StringType,true),StructField(log,StructType(List(StructField(file,StructType(List(StructField(path,StringType,true))),true))),true),StructField(processed,StructType(List(StructField(DateTime,StringType,true),StructField(LineNumber,StringType,true),StructField(Severity,StringType,true),StructField(Source,StringType,true),StructField(Thread,StringType,true),StructField(message,StringType,true))),true)))

# COMMAND ----------

#df_logs= spark.sql("select * from lab_red.bronze_scannerlogs where P_loadDate >'2021-05-09' ")

# COMMAND ----------

print(jsonSchema)

# COMMAND ----------

type (jsonSchema)


# COMMAND ----------

# jsonSchema_c= StructType(List(StructField(@timestamp,StringType,true),StructField(agent,StructType(List(StructField(hostname,StringType,true))),true),StructField(application,StringType,true),StructField(log,StructType(List(StructField(file,StructType(List(StructField(path,StringType,true))),true))),true),StructField(processed,StructType(List(StructField(DateTime,StringType,true),StructField(LineNumber,StringType,true),StructField(Severity,StringType,true),StructField(Source,StringType,true),StructField(Thread,StringType,true),StructField(message,StringType,true))),true)),StructField(corrupted,StringType,true))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --create table lab_red.mluzon_bronze_scannerlogs_quarantine as select * from lab_red.silver_scannerlogs where 1=2
# MAGIC --alter table lab_red.mluzon_bronze_scannerlogs_quarantine  ADD COLUMNS    ( _corrupted string)

# COMMAND ----------

schema = spark.read.table('lab_red.mluzon_bronze_scannerlogs_quarantine').schema   
# print (schema)



# COMMAND ----------

df_logs= spark.sql("select * from  lab_red.bronze_scannerlogs where P_loadDate='2021-06-21'")

# COMMAND ----------

test_df = df_logs.withColumn("value_json",from_json("value",jsonSchema))
test_df.display(3)


# COMMAND ----------

test_df.write.mode("append").format('delta').saveAsTable('lab_red.mluzon_bronze_check_json')


# COMMAND ----------

# MAGIC %sql 
# MAGIC desc TABLE  lab_red.mluzon_bronze_check_json

# COMMAND ----------

rdd = df_logs.rdd.map(lambda x: x.asDict()["value"])

#print("first :  "+str(rdd.first()))
#top
print("top : "+str(rdd.top(2)))
print("take : "+str(rdd.takeSample(4)))

# COMMAND ----------

df_clean =  processMicrobatch_check_value (df_logs, jsonSchema_c , 'lab_red.mluzon_bronze_scannerlogs_check'  )

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from 
# MAGIC lab_red.mluzon_bronze_scannerlogs_check_quarantine limit 10

# COMMAND ----------

df_clean.display()

# COMMAND ----------

test_df = df_logs.withColumn("value_json",from_json("value",jsonSchema))

# COMMAND ----------

# from pyspark.sql.functions import from_json
# from pyspark.sql.functions import col
# from pyspark.sql.functions import expr
# from pyspark.sql.types import StructType, IntegerType, DateType, StringType, DecimalType , BooleanType , StructField , LongType ,TimestampType
# from pyspark.sql.functions import lower, col

# parsed_df = df_logs.withColumn("value_json",from_json("value",jsonSchema)).\
#                   withColumn("timestamp", col("value_json.@timestamp").cast(TimestampType())).\
#                   withColumn("agent_host", col("value_json.agent.hostname")).\
#                   withColumn("application", col("value_json.application")).\
#                   withColumn("log_file", col("value_json.log.file.path")).\
#                   withColumn("processed_DateTime", col("value_json.processed.DateTime").cast(TimestampType())).\
#                   withColumn("processed_LineNumber", col("value_json.processed.LineNumber").cast(IntegerType())).\
#                   withColumn("processed_Severity", col("value_json.processed.Severity")).\
#                   withColumn("processed_Source", col("value_json.processed.Source")).\
#                   withColumn("processed_Thread", col("value_json.processed.Thread")).\
#                   withColumn("processed_message", col("value_json.processed.message")).\
#                   withColumn("loadDate", col("P_loadDate")).\
#                   withColumn("P_logDate",col("timestamp").cast(DateType())).\
#                   drop("value_json").\
#                   drop("value")


# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, IntegerType, DateType, StringType, DecimalType , BooleanType , StructField , LongType ,TimestampType
from pyspark.sql.functions import lower, col

# parsed_df = df_logs.withColumn("value_json",from_json("value",jsonSchema)).\
#                   withColumn("timestamp", col("value_json.@timestamp").cast(TimestampType())).\
#                   withColumn("agent_host", col("value_json.agent.hostname")).\
#                   withColumn("application", col("value_json.application")).\
#                   withColumn("log_file", col("value_json.log.file.path")).\
#                   withColumn("processed_DateTime", col("value_json.processed.DateTime").cast(TimestampType())).\
#                   withColumn("processed_LineNumber", col("value_json.processed.LineNumber").cast(IntegerType())).\
#                   withColumn("processed_Severity", col("value_json.processed.Severity")).\
#                   withColumn("processed_Source", col("value_json.processed.Source")).\
#                   withColumn("processed_Thread", col("value_json.processed.Thread")).\
#                   withColumn("processed_message", col("value_json.processed.message")).\
#                   withColumn("loadDate", col("P_loadDate")).\
#                   withColumn("P_logDate",col("timestamp").cast(DateType()))#.\
#                 #  drop("value_json").\
#                #   drop("value")

parsed_df = df_logs.withColumn("value_json",from_json("value",jsonSchema)).\
                  withColumn("timestamp", col("value_json.@timestamp")).\
                  withColumn("agent_host", col("value_json.agent.hostname")).\
                  withColumn("application", col("value_json.application")).\
                  withColumn("log_file", col("value_json.log.file.path")).\
                  withColumn("processed_DateTime", col("value_json.processed.DateTime")).\
                  withColumn("processed_LineNumber", col("value_json.processed.LineNumber")).\
                  withColumn("processed_Severity", col("value_json.processed.Severity")).\
                  withColumn("processed_Source", col("value_json.processed.Source")).\
                  withColumn("processed_Thread", col("value_json.processed.Thread")).\
                  withColumn("processed_message", col("value_json.processed.message")).\
                  withColumn("loadDate", col("P_loadDate")).\
                  withColumn("P_logDate",col("timestamp"))


# COMMAND ----------

bronze_df_parsed = df_logs.withColumn("value_json",from_json("value",jsonSchema)).\
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

parsed_df_length=parsed_df.withColumn("processed_DateTime", processed_length_df.processed_DateTime.substr(0,23).cast(TimestampType()))
                            

# COMMAND ----------

bronze_df_parsed.display()

# COMMAND ----------

processed_length_df = bronze_df_parsed.where(length(col("processed_DateTime")) > 24) 

# COMMAND ----------

processed_length_sub = processed_length_df.withColumn("processed_DateTime", processed_length_df.processed_DateTime.substr(0,23).cast(TimestampType()))


# COMMAND ----------

processed_length_df.display(3)

# COMMAND ----------

processed_length_sub.display(3)

# COMMAND ----------

parsed_df.schema

# COMMAND ----------

parsed_df.printSchema()

# COMMAND ----------

parsed_df.display(4)

# COMMAND ----------

quarntine = parsed_df.where("P_logDate is null or timestamp is null")

# COMMAND ----------

quarntine.display()

# COMMAND ----------

# write = parsed_df.write.format("delta").partitionBy("P_logDate").saveAsTable("lab_red.silver_scannerlogs")

# COMMAND ----------

#write = parsed_df.write.format("delta").partitionBy("P_logDate").saveAsTable("lab_red.mluzon_silver_scannerlogs0621_before_cast")

write = parsed_df.write.format("delta").saveAsTable("lab_red.mluzon_silver_scannerlogs0621_before_cast")


# COMMAND ----------

spark.read.table('lab_red.mluzon_silver_scannerlogs0621_before_cast').printSchema()

# COMMAND ----------

spark.read.table('lab_red.silver_scannerlogs').printSchema()

# COMMAND ----------


