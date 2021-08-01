# Databricks notebook source
import logging
import uuid
import json
import sys
from pyspark.sql.types import StructType, IntegerType, DateType, StringType, DecimalType , BooleanType , StructField , LongType ,TimestampType, ByteType
from pyspark.sql import functions as F
from datetime import date,datetime, timedelta
import os
from pyspark.sql.functions import input_file_name,col,lit
import re
import time
import requests
import pandas as pd
from pyspark.sql.functions import max, min  






logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

def mount_exists(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
      print("mount exists, removing the mount point")
      dbutils.fs.unmount(str_path)


# converting a string type to spark column type (e.g. from "int" --> integerType)
def shGetColumnTypeFromString(colType):
  if(colType=="string"):
    return StringType()
  if(colType=="float"):
    return FloatType()
  if(colType=="double"):
    return DoubleType()
  if(colType=="timestamp"):
    return TimestampType()
  if(colType=="date"):
    return DateType()
  if(colType=="long"):
    return LongType()
  if(colType=="int"):
    return IntegerType()
  if(colType=="boolean"):
    return BooleanType()
  if(colType=="short"):
    return ShortType()
  raise Exception("{} is not supported".format(colType))



def write_mode(topic,table_name,src_env="lab"):
  write_mode = ''
  schema_topic=spark.sql("select topic from {}_config.tablesSchema where topic='{}' group by 1".format(src_env,topic))
  if schema_topic is None:
    logging.warning("no topic = {} was found is schema object... ".format(topic))
  else:
    write_mode = spark.sql("select write_mode from {}_config.tablesSchema where topic='{}' and  table_name = '{}'".format(src_env,topic,table_name)).collect()[0][0]
    if write_mode == '' :
      logging.warning("no {} was found for {} table under {} topic ".format(write_mode,table_name,topic))
    else:
      logging.warning("write_mode = {} was found for {} table under {} topic ".format(write_mode,table_name,topic))
  return write_mode

def SetColumnsNullable( df, column_list, nullable=True):
  for struct_field in df.schema:
    if struct_field.name in column_list:
      struct_field.nullable = nullable
      df_mod = spark.createDataFrame(df.rdd, df.schema)
  return df_mod

def AddLoadTime(df):
  print("adding column 'loadTime' with current timestamp to dataframe")
  df = df.withColumn("loadTime",F.current_timestamp())
  df = SetColumnsNullable( df, ["loadTime"],nullable=True)
  return df

def batch_AddLoadTime(df, epoch_id):
  df = df.withColumn("loadTime",F.current_timestamp())
  df = SetColumnsNullable( df, ["loadTime"],nullable=True)
  pass
  
def StringFromDate(dateToFormat,format="%Y-%m-%d"):
  return dateToFormat.strftime(format)

def SaveExecutionDateToFile(lastExecutionDatesFileName,date):
  content = {"lastExecutionDate":StringFromDate(date)}
  with open(lastExecutionDatesFileName, 'w') as f:
    json.dump(content, f)
    
    
def UploadSingleFileToStorage(awsPath,fileTypes="gz"):
  command = "dbutil -m cp *.{} {}".format(fileTypes,awsPath)
  print("load file to S3 bucket - executing command: {}".format(command))
  os.system(command)
  
def datesToRun(start_date=None,end_date=None):
  date_list=[]
  return date_list

def mount_exists(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
      print("mount exists, removing the mount point")
      dbutils.fs.unmount(str_path)
    else:
      print("mount doesnt exit")
      
      
def get_current_timestamp(interval_minutes=0):
  ts = time.time() - interval_minutes*60
#   readble_ts = datetime.datetime.fromtimestamp(ts).isoformat()
  readble_ts = datetime.fromtimestamp(ts).isoformat()
  return readble_ts



def column_list_no_changes(df,str_to_rmv=None):
  column_list_df = df.dtypes
  column_list =[]
  column_str = ''
  
  for col in column_list_df:
    column_list.append(col[0])
  
  if str_to_rmv is not None:
    try:
      column_list.remove(str_to_rmv)
    except Exception as err:
      print(err)
  else:
    pass
    
  num = len(column_list)
  for i in range(num):
    if i == 0:
      column_str+= '(' + column_list[i]
    elif i == len(column_list)-1:
      column_str+= ',' + column_list[i] + ')'
    else:
      column_str+= ',' + column_list[i] 
  return column_str

