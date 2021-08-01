# Databricks notebook source
import logging
import uuid
import json
import sys
from pyspark.sql.types import StructType, IntegerType, DateType, StringType, DecimalType , BooleanType , StructField , LongType ,TimestampType
from pyspark.sql import functions as F
from datetime import date,datetime, timedelta
import os
from pyspark.sql.functions import input_file_name
import re
from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.functions import lower, col


logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

## use structured streaming with foreachbatch
def processMicrobatch(df,epoch_id):
  #schema derived from target table
  schema_table = table.replace(datalayer_source,datalayer_target)
  schema = spark.read.table(schema_table).schema
  #read source table and adjust the scehma
  parsed_df = df.withColumn("nested_json", from_json("value", schema)) .\
                    withColumn("analysis", col("nested_json.analysis")) .\
                    withColumn("header", col("nested_json.header")) .\
                    withColumn("result", col("nested_json.result")) .\
                    drop("nested_json").drop("value")
#   write the data to the predefined table
  valid = parsed_df.where("analysis is not null  and  header is not null and  result is not null")
  valid.write.mode("append").format('delta').saveAsTable(schema_table)
  quarntine = parsed_df.where("analysis is null or header is null or result is null")
  quarntine.write.mode("append").format('delta').saveAsTable(schema_table+"_quarantine")


def projector_tester_schema(df):
  schema = StructType([
    StructField("color",StringType()),
    StructField("comment",StringType()),
    StructField("manufacturer_MLA",StringType())])

  full_schema = df.select(col("Projector_details").cast(schema2), \
     col("analysis"), col("header"),col("result"),col("_corrupted")) \
  
  return full_schema
  
def processMicrobatch_1(df,epoch_id):
  #schema derived from target table
  schema_table = table.replace(datalayer_source,datalayer_target)
  schema = spark.read.table(schema_table).schema
  #build the rdd with mapping ptrocess of  json value
  rdd = df.rdd.map(lambda x: x.asDict()["value"])
  test_df = spark.read.option('multiLine','true').option('columnNameOfCorruptRecord','_corrupted').schema(schema).json(rdd)
  #write data to 2 seperate tables
  quarntine = test_df.where("_corrupted is not null")
  valid = test_df.where("_corrupted is  null")
  valid.write.mode("append").format('delta').saveAsTable(schema_table)
  quarntine.write.mode("append").format('delta').saveAsTable(schema_table+"_quarantine")

# COMMAND ----------

# the write table combination is combined of the following: <br />
# 1.read_stream = defines the paramters for reading the data from S3 <br />
# 2.enrich_write_schema = all the data enrichment we desire for bronze tables <br />
# 3.write_stream = the desired table we write the data to

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
            trigger(once=True).\
            format('delta').\
            option('mergeSchema', 'true') .\
            option('checkpointLocation',checkpoint_path+'_'+file_name).\
            table(table_name).\
            awaitTermination()
  else:
    logging.warning("write data to destination path")
    transform.\
            writeStream.\
            trigger(once=True).\
            format('delta').\
            option('mergeSchema', 'true') .\
            option('checkpointLocation',checkpoint_path).\
            table(table_name).\
            awaitTermination()



# COMMAND ----------


