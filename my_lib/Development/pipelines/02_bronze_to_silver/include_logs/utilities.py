# Databricks notebook source
import logging
import uuid
import json
import sys
from pyspark.sql.types import StructType, IntegerType, DateType, StringType, DecimalType , BooleanType , StructField , LongType ,TimestampType
from pyspark.sql import functions as F
from datetime import date,datetime, timedelta
import os
from pyspark.sql.functions import col, size, length


logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

# MAGIC %md
# MAGIC ### flat json

# COMMAND ----------

import pyspark.sql.functions as F  # By Hanan Segal


def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df
  


# COMMAND ----------

# MAGIC %md
# MAGIC #### flat two layers - solution for now that handles another nesting layer (2 instead of 1 ) which should be fine for this situation, will add a more dynamic solution later on

# COMMAND ----------

def flatten_df2(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']


    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    flat_cols_1 = [c[0] for c in flat_df.dtypes if c[1][:6] != 'struct']
    nested_cols_1 = [c[0] for c in flat_df.dtypes if c[1][:6] == 'struct']
    
    flat_df_1 = flat_df.select(flat_cols_1 +
                           [F.col(nc+'.'+c).alias(nc+'_'+c)
                            for nc in nested_cols_1
                            for c in flat_df.select(nc+'.*').columns])
    return flat_df_1

# COMMAND ----------

# MAGIC %md
# MAGIC ### create date for partitionBy

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, DateType, StringType, DecimalType , BooleanType , StructField , LongType ,TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from datetime import date,datetime, timedelta

# df_logs_1 = df_logs.withColumn("logDate",F.to_date(F.col("@timestamp"),"yyyy-MM-dd"))
# df_logs_1.printSchema()
# df_logs_1.createOrReplaceTempView("xxx")
def getPaDate(df):
     dfWDate = df.withColumn("P_logDate",F.to_date(F.substring(F.col("@timestamp"),0,10)))

     return dfWDate
#df_logs_1.printSchema()
#df_logs_1.createOrReplaceTempView("xxx")

# substring(str, pos, len)
# left(str, len)

# COMMAND ----------

# MAGIC %md
# MAGIC ## example for adding metadata

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp, lit

# raw_health_tracker_data_df = raw_health_tracker_data_df.select(
#     "value",
#     lit("files.training.databricks.com").alias("datasource"),
#     current_timestamp().alias("ingesttime"),
#     lit("new").alias("status"),
#     current_timestamp().cast("date").alias("ingestdate"),
# )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC date functions for running on date range or specific date <br/>
# MAGIC load time add to dataframe

# COMMAND ----------

from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import functions as F

#range list between 00 and 23 for hours
def define_hour(first_hour=0,last_hour=24):
  hour_list = []
  hours= list(range(first_hour, last_hour))
  for i in hours:
    if i < 10:
      hour_list.append('0'+str(i))
    else:
      hour_list.append(str(i))
  return hour_list

#returns yesterday date 
def run_day(days=1):
  run_day = []
  yesterday = datetime.now() - timedelta(days)
  yesterday = yesterday.strftime('%Y-%m-%d')
  run_day.append(yesterday)
  return run_day

#user pre_defined date list
def date_range(start_date,end_date):
  date_range= []
  date_range = pd.date_range(start=start_date,end=end_date).tolist()
  return date_range

def SetColumnsNullable( df, column_list, nullable=True):
  for struct_field in df.schema:
    if struct_field.name in column_list:
      struct_field.nullable = nullable
      df_mod = spark.createDataFrame(df.rdd, df.schema)
  return df_mod

def AddLoadTime(df):
  print("adding column loadtime with current␣,→timestamp to dataframe")
  df = df.withColumn("loadTime",F.current_timestamp())
  df = SetColumnsNullable( df, ["loadTime"],nullable=True)
  return df

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
    
  elif src_file_format == "elactic_text":
    df=   spark.readStream.\
        format('cloudFiles').\
        option('recursiveFileLookup','true').\
        option('cloudFiles.format', src_file_format).\
        option('cloudFiles.includeExistingFiles','true').\
        option('cloudFiles.region', src_bucket_region).\
        option('cloudFiles.useNotifications','true').\
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


def processMicrobatch_check_value (df,schemc_df , table_name):
  #schema derived from target table
#   schema_table = table.replace(datalayer_source,datalayer_target)
  #schema = spark.read.table(schema_table).schema
  #build the rdd with mapping ptrocess of  json value
  rdd = df.rdd.map(lambda x: x.asDict()["value"])
  #rdd.take(20)
  print("test_df")
  test_df = spark.read.option('multiLine','true').option('columnNameOfCorruptRecord','_corrupted').schema(schemc_df).json(rdd)
  #write data to 2 seperate tables
  quarntine = test_df.where("_corrupted is not null")
  valid = test_df.where("_corrupted is  null")
  valid.write.mode("append").format('delta').saveAsTable(table_name)
  quarntine.write.mode("append").format('delta').saveAsTable(table_name+"_quarantine")
  return valid 

# COMMAND ----------


