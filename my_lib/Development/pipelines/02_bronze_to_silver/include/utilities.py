# Databricks notebook source
# MAGIC %md
# MAGIC ## python imports

# COMMAND ----------

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





logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## flat json

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
# MAGIC ## flat two layers - solution for now that handles another nesting layer (2 instead of 1 ) which should be fine for this situation, will add a more dynamic solution later on

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
# MAGIC ## create date for partitionBy

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, DateType, StringType, DecimalType , BooleanType , StructField , LongType ,TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from datetime import date,datetime, timedelta

# df_logs_1 = df_logs.withColumn("logDate",F.to_date(F.col("@timestamp"),"yyyy-MM-dd"))
# df_logs_1.printSchema()
# df_logs_1.createOrReplaceTempView("xxx")
def getPaDate(df):
     dfWDate = df.withColumn("logDate",F.to_date(F.substring(F.col("@timestamp"),0,10)))

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

def get_table_list(data_source,env_source,database_source):
  table_list=[]
  tbl_df = spark.sql("""SHOW TABLES IN {}_{}""".format(env_source,database_source))
  tbl_df.registerTempTable("tables_in_db")
  table_list = spark.sql("select concat(database,'.',tableName) as fulltablename from tables_in_db where tableName like '{}%' and isTemporary = false".format(data_source))
  full_table_list = table_list.select('fulltablename').collect()
  table_list = [row.fulltablename for row in full_table_list]
  return table_list

# COMMAND ----------

# change unvalid chars in df col_names
# " ,;{}()\n\t=" are unvaild chars within df col name (when trying to save it as table) and need to be changed
replace_dict = {
  ' ':'_',
  ',':'_',
  ';':'_',
  '{':'[',
  '}':']',
  '(':'[',
  ')':']',
  '\n':'_',
  '\t':'_',
  '=':'_'
}


def change_unvalid_chars_in_df_col_names(df):
  for col in df.columns:
    if any((c in replace_dict.keys()) for c in col):
      new_col = col
      for c, replacement in replace_dict.items():
        new_col = new_col.replace(c, replacement)
      #print(col, '-->', new_col)
      df = df.withColumnRenamed(col, new_col)
  return df

# COMMAND ----------


