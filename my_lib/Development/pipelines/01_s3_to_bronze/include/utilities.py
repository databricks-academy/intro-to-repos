# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## python imports

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## pyhon general

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


# COMMAND ----------

def determineSchema(topic,table,src_file_format,source_path=None,dest_path=None,src_env = "lab"):
  relevant_schema= None
  schema_topic=spark.sql("select topic from {}_config.tablesSchema where topic='{}' group by 1".format(src_env,topic))
  if src_file_format == "csv":
    if schema_topic is None:
      logging.warning("no topic = {} was found is schema object... using schema from file ".format(topic))
    else:
      relevant_schema = eval(spark.sql("select table_schema from {}_config.tablesSchema where topic='{}' and  table_name = '{}'".format(src_env,topic,table)).collect()[0][0])
      if relevant_schema is None:
        logging.warning("no schema was found for {} table under {} topic ".format(table,topic))
      else:
        logging.warning("schema was found for {} table under {} topic ".format(table,topic))
  elif src_file_format == "parquet":
    try:
      spark.sql("select table_name from {}_config.tablesSchema where topic='{}' and  table_name = '{}'".format(src_env,topic,table)).collect()[0][0]
      logging.warning("using schema from existing table from {}".format(dest_path))
      relevant_schema = spark.read.option('format', 'parquet').load(dest_path).schema      

    except Exception as err:
      logging.warning("no table {} was found in {} schema ... using schema from file ".format(table,topic))
      relevant_schema = spark.read.option('format', 'parquet').load(source_path+"/*/").schema
  elif (src_file_format == "text" and topic == 'jigs'):
    relevant_schema = "value STRING"    
  else:
    print("no relevant schema")
  return relevant_schema

def source_table_list(source_path):
  table_list=[]
  folder_table_info = dbutils.fs.ls(source_path)
  for table_folder in folder_table_info:
    table_list.append(table_folder.name.strip('/'))
  return table_list

def remove_from_string(text,char):
  clean_text = text.replace(char,"")
  return clean_text

def make_first_capital(s,char="_"):
    new_string = ''
    i=0
    words = s.split(char)
    for word in words:
      word = word.lower()
      if i==0:
        new_string += word
      else:
        new_string += word.capitalize()
      i=+1
    return new_string
      
        
def extract_datetime_from_filename(file_name):
  file_datetime = '' 
  try:
    match = re.search(r'\d{8}_\d{6}', file_name)
    match = match.group()
    file_datetime = datetime.strptime(match, '%Y%m%d_%H%M%S')
  except Exception as err:
    print(err)   
  return file_datetime
      
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## structured streaming functions

# COMMAND ----------

# DBTITLE 0,Untitled
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

# MAGIC %md
# MAGIC 
# MAGIC ## jigs functions

# COMMAND ----------

#checks if table exits in database
def table_exists(table_name):
  table_exist=False
  if spark._jsparkSession.catalog().tableExists(table_name):
    table_exist=True
  else:
    table_exist=False
  return table_exist



#checks if data exists in s3 for the relevant file path+file name
def data_exist(table_name,source_path,file_name):
  data_exist = False
  if table_exists(table_name):
    data_exist = True
  else:
    try:
      df = spark.read. \
           option("pathGlobFilter",file_name). \
           option("recursiveFileLookup","true"). \
           option("multiLine","true"). \
           json(source_path).count()
      data_exist = True
    except Exception as err:
      print(err)        
  return data_exist




# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## sfdc API

# COMMAND ----------

#aproach API and get acces token
def sfdc_acces_token(client_id,client_secret,sfdc_user,sfdc_pass):
    # Consumer Key
  client_id = sfdc_client_id
  # Consumer Secret
  client_secret = sfdc_client_secret
  # Callback URL
  redirect_uri = 'http://localhost/'
  # sfdc_pass and user
  sfdc_user = sfdc_username
  sfdc_pass = sfdc_password

  auth_url = 'https://aligntech.my.salesforce.com/services/oauth2/token'

  # POST request for access token
  response = requests.post(auth_url, data = {
                      'client_id':client_id,
                      'client_secret':client_secret,
                      'grant_type':'password',
                      'username':sfdc_user,
                      'password':sfdc_pass
                      })
  json_res = response.json()
  return json_res
  
  

#extracts the data from the API
def sfdc_call(access_token,action,parameters={},method='get',data={}):
  headers = {
    'content_type':'application/json',
    'accept_Encoding' :'gzip',
    'Authorization':'Bearer ' + access_token
  }
  if method == 'get':
      r=requests.request(method,instance_url+action,headers=headers,params=parameters,timeout=30)
  elif method in('post','patch'):
      r=requests.request(method,instance_url+action,headers=headers,json = data , params=parameters,timeout=30)
  else:
      raise ValueError('Method should be either get or post')
  return r.json()

#shows the temp tables currently "live"
def tmp_tables_list():
  x = spark.sql("SHOW TABLES").filter("isTemporary=True").select("tableName").collect()
  print(x)
  
def create_tmp_table(action,query_SOQL,table_name):
  action = '/services/data/v45.0/query/'
  dataextract = sfdc_call(action,{'q':query_SOQL})
  sf_df = pd.DataFrame(dataextract['records']).drop(columns='attributes')
  sf_df = sf_df.dropna(axis='columns', how='all') #hsegal(21/06/2021) - removes all null columns to prevent the process from falling
  api_running_status = dataextract['done']
  while api_running_status is False:
    action = dataextract['nextRecordsUrl']
    dataextract = sfdc_call(action,{'q':query_SOQL})
    df = pd.DataFrame(dataextract['records']).drop(columns='attributes')
    df = df.dropna(axis='columns', how='all')  #hsegal(21/06/2021) - removes all null columns to prevent the process from falling
    api_running_status = dataextract['done']
    sf_df = sf_df.append(df)
  f = spark.createDataFrame(sf_df)
#   f.createOrReplaceTempView(table_name)
  
def create_df_sfdc(action,query_SOQL,access_token):
  action = '/services/data/v45.0/query/'
  dataextract = sfdc_call(access_token,action,{'q':query_SOQL})
  sf_df = pd.DataFrame(dataextract['records']).drop(columns='attributes')
  sf_df = sf_df.dropna(axis='columns', how='all') 
  api_running_status = dataextract['done']
  while api_running_status is False:
    action = dataextract['nextRecordsUrl']
    dataextract = sfdc_call(access_token,action,{'q':query_SOQL})
    df = pd.DataFrame(dataextract['records']).drop(columns='attributes')
    df = df.dropna(axis='columns', how='all')  #hsegal(21/06/2021) - removes all null columns to prevent the process from falling
    api_running_status = dataextract['done']
    sf_df = sf_df.append(df)
  f = spark.createDataFrame(sf_df)
  return f


def string_to_timestamp_sfdc(df):
  df_final = df.withColumn("CreatedDate",col("CreatedDate").cast(TimestampType())).withColumn("LastModifiedDate",col("LastModifiedDate").cast(TimestampType()))
#   df_temp = df.withColumn("CreatedDate_test", F.to_timestamp(F.col('CreatedDate'),"yyyy-MM-dd'T'HH:mm:ss")).withColumn("LastModifiedDate_test",    F.to_timestamp(F.col('LastModifiedDate'),"yyyy-MM-dd'T'HH:mm:ss")).drop('CreatedDate').drop('LastModifiedDate')
#   df_final = df_temp.withColumnRenamed('LastModifiedDate_test','LastModifiedDate').withColumnRenamed('CreatedDate_test','CreatedDate')
  return df_final


def convert_to_sfdc_timestamp(timestamp):
  if timestamp is not None:
    sfdc_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S"+'.000+0000')
  else:
    sfdc_timestamp = ('1970-01-01T00:00:00.000+0000')
  #       to_date = now.strftime("%d/%m/%Y %H:%M:%S")
  return sfdc_timestamp
  

# COMMAND ----------

def build_sql_with_pk(query_text,primary_key,start_time,end_time):
  part_soql = query_text[query_text.find('from'):]
  part_soql = part_soql.replace('{0}',start_time).replace('{1}',end_time)
  soql_query = 'select {} {}'.format(primary_key,part_soql)
  return soql_query

def delete_from_dbtable_by_pk(table_name,df,pk,min_time,date_column_name):
  df.createOrReplaceTempView('temp_{}'.format(pk))
  delete_statemnt = ("""delete from {table_name} where {pk} in 
          (select tar.{pk} from {table_name} tar 
           left join temp_{pk} src on tar.{pk}=src.{pk}
            where src.{pk} is null and tar.{date_column_name} >= '{min_time}')""".format(table_name=table_name,pk=pk,min_time=min_time,date_column_name=date_column_name))
  print(delete_statemnt)
  spark.sql(delete_statemnt)
  
  
  
def update_sc_deleted_by_pk(table_name,df,pk,min_time,date_column_name,load_time,added_column_name='is_deleted'):
  #add is_deleted to schema for 1st time
  try:
    spark.sql("select {} from {} limit 1".format(added_column_name,table_name))
  except Exception as err:
    print('add is_deleted to {}'.format(table_name))
    spark.sql('ALTER TABLE {} ADD COLUMNS ({} boolean)'.format(table_name,added_column_name))
  ##create temp view
  df.createOrReplaceTempView('temp_{}'.format(pk))
  ##   update rows in scd table
  update_statment = ("""update  {table_name} 
            set iscurrent = 0 , is_deleted = 1 , end_date = '{load_time}'
            where {pk} in 
          (select tar.{pk} from {table_name} tar 
           left join temp_{pk} src on tar.{pk}=src.{pk}
            where src.{pk} is null and tar.{date_column_name} >= '{min_time}')
            and is_deleted is null""".format(table_name=table_name,pk=pk,min_time=min_time,date_column_name=date_column_name,load_time=load_time))
  print(update_statment)
  spark.sql(update_statment)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DB config Data

# COMMAND ----------

# get a list of thables to work on 
def get_table_list (source,pii_data,end_time):
  print (" create the list of tables ")
  get_tables_sql='''select t.source_name as source_name, 
  is_pii,
  primary_key,
  t.owner_name as owner_name,
  lower(t.source_table_name) as source_table_name,
  t.get_method as get_method,
  date_column1_name,
  date_column2_name,
  query_text ,
  first_query_text,
  extraction_interval_minutes,
  m.last_updated_time as last_updated_time,
  m.last_increment_date as last_increment_date
  from lab_config.dbtables t
  left join lab_config.dbtables_manage m on t.source_name=m.source_name and t.source_table_name=m.source_table_name and t.owner_name=m.owner_name 
  where t.source_name = \'{}\' and is_pii = \'{}\'   and isactive=1  
  and ( isnull(m.last_updated_time) or extraction_interval_minutes < (to_unix_timestamp(cast(\'{}\' as timestamp)) - to_unix_timestamp(COALESCE(m.last_updated_time,current_timestamp())))/60)''' \
  .format(source,pii_data,end_time)
  print (get_tables_sql)
  list_of_tables = spark.sql(get_tables_sql)
  return list_of_tables


def  get_isdeleted_table_list(source,pii_data):
  print (" create the list of tables for isdeleted process")
  get_tables_sql='''
  select t.source_name as source_name, 
  primary_key,
  date_column1_name,
  lower(t.source_table_name) as source_table_name,
  date_column1_name,
  query_text
  from lab_config.dbtables t
  left join lab_config.dbtables_manage m on t.source_name=m.source_name and t.source_table_name=m.source_table_name and t.owner_name=m.owner_name 
  where t.source_name = '{}' and is_pii = '{}'   and isactive=1  and is_deleted_ind = 1 '''.format(source,pii_data)
  print (get_tables_sql)
  list_of_tables = spark.sql(get_tables_sql)
  return list_of_tables



# COMMAND ----------

# find max date in data frame
def find_max_date (df , timestamp_c):
  max_time = df.agg(max(timestamp_c)).collect()[0][0]
  ts_string = max_time.isoformat()
  return ts_string

def find_max_date_to_date (df , timestamp_c , end_date):
  max_time = df.agg(max(timestamp_c)).collect()[0][0]
  if max_time > end_date:
    return end_date
  else:
    return max_time

def update_dbtables_manage_nodata (source_name, owner_name , source_table_name   ,end_time ):
  sql='update lab_config.dbtables_manage  set  last_updated_time=\'{}\' where source_name=\'{}\' and  owner_name=\'{}\' and source_table_name=\'{}\' '.format(end_time,source_name, owner_name , source_table_name )
  print ('**********************  dbtables_manage  **************************')  

  print (sql)  
  spark.sql(sql)

def insert_dbtables_manage(source_name, owner_name , source_table_name   ,end_time ,  new_increment_date):
  sql='insert into lab_config.dbtables_manage VALUES (\'{}\',\'{}\',\'{}\',current_timestamp() , \'{}\' ,\'{}\')'.format (source_name, owner_name , source_table_name ,end_time, new_increment_date )
  print ('********************** insert into dbtables_manage  **************************')  

  print (sql)  
  spark.sql(sql)

def update_dbtables_manage(source_name, owner_name , source_table_name   ,end_time ,  new_increment_date):
  sql='update lab_config.dbtables_manage  set last_increment_date=\'{}\' , last_updated_time=\'{}\' where source_name=\'{}\' and  owner_name=\'{}\' and source_table_name=\'{}\' '.format(new_increment_date,end_time,source_name, owner_name , source_table_name )
  print ('********************** update table dbtables_manage  **************************')  

  print (sql)  
  spark.sql(sql)


def date_to_string_convert(date):
  str=date.strftime("%Y-%m-%d %H:%M:%S.%f")
  return 'convert(datetime,\''+ str[:-3] + '\',121)'

def date_to_string(date):
  return date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
  

# COMMAND ----------

def table_handler(table_list, end_time ,jdbcUrl, connectionProperties ):
  for table_param in tables:
    print ('last' , table_param['last_updated_time'])
    if table_param['last_updated_time']:
      df=get_df_jdbc(jdbcUrl,table_param['query_text'].format(table_param['last_updated_time'],date_to_string (end_time)) )   
      
    else:
      df=get_df_jdbc(jdbcUrl,table_param['first_query_text'])
      
    df.display()
#     return df 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DB functions

# COMMAND ----------

from datetime import date
from datetime import datetime
from pyspark.sql import functions as F


def new_schema_from_table_to_df (df,table_name):
  column_list_for_select = column_list_from_df(df) #read columns from df with ()
  column_list_for_select = column_list_for_select.replace(')','').replace('(','') #remove ()
  schema = spark.sql("select {} from {} limit 1".format(column_list_for_select,table_name)).schema #take the specific schema
#   schema = spark.read.table(table_name).schema
  rdd = df.rdd
  df_1 = spark.createDataFrame(rdd, schema)
  return df_1

# build sql from config dbtable 
def sql_query_builder(data_source,first_run_ind,first_sql_query,sql_query,last_increment_date,load_time):
  if first_run_ind:
    print("extract table from API for the 1st time")
    full_sql_query = first_sql_query  
    
  else:
      print("extract increment table from API")
#       from_date = convert_to_sfdc_timestamp(datetime(1970,1,1))
#       now = datetime.now()
#       to_date = convert_to_sfdc_timestamp(now)
      full_sql_query = sql_query.replace('{0}',last_increment_date).replace('{1}',load_time)
  print(full_sql_query)
  return full_sql_query
  



def derive_schema(df,source_table_name,db_table_name):
  df.createOrReplaceTempView('temp_{}'.format(source_table_name))
  relevant_list = column_list_no_changes(df).lower().replace('(',"").replace(')',"").split(',')
  list_of_columns = spark.sql('DESCRIBE TABLE {}'.format(db_table_name)).collect()
  cast_string = 'SELECT\n'
  for idx,column in enumerate(list_of_columns):
    col_name = column.col_name
    col_type = column.data_type
    if col_type=='':
      pass
    elif col_name.lower() in relevant_list:
      if idx==0:
        cast_string+=(' cast ({col_name} as {col_type}) as {col_name}\n'.format(col_name=col_name,col_type=col_type))
      else:
        cast_string+=(',cast ({col_name} as {col_type}) as {col_name}\n'.format(col_name=col_name,col_type=col_type))
    else:
      if idx==0:
        cast_string+=(' cast (Null as {col_type}) as {col_name}\n'.format(col_name=col_name,col_type=col_type))
      else:
        cast_string+=(',cast (Null as {col_type}) as {col_name}\n'.format(col_name=col_name,col_type=col_type))

  cast_string+= ('from temp_{}'.format(source_table_name))
  df_fixed = spark.sql(cast_string)
  return df_fixed

def db_table_first_run(source_table_name):
  first_run_ind = spark.sql("select * from lab_config.dbtables_manage where source_name = '{}' and source_table_name = '{}'".format(source,source_table_name)).count()
  if first_run_ind == 0:
    return True
  else:
    return False
  
  
def add_load_time(df,load_time):
  print("adding column load_time with current timestamp to dataframe")
#   df = df.withColumn("load_time", lit(load_time).cast(TimestampType))
  df= df.withColumn("load_time",F.lit(load_time).cast(TimestampType()))
  df = SetColumnsNullable( df, ["load_time"],nullable=False)
  
  return df

def add_sc_columns_first_time ( df , date_columns ):
  print("adding columns iscurrent , start_date ,  end_date  ")
  df=df.withColumn("iscurrent",F.lit('True').cast(BooleanType())).withColumn("start_date",col(date_columns) ).withColumn("end_date",F.lit(None).cast(TimestampType()))
  return df


def column_list_from_df(df,str_to_rmv=None):
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
      column_str+= ', ' + column_list[i] + ')'
    else:
      column_str+= ', ' + column_list[i] 
  return column_str

def check_same_column_from_df_full_merge(df,str_to_rmv=None):
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
   
    if i == len(column_list)-1:
      column_str+= ' s.'+column_list[i] + ' = staged_updates.'+ column_list[i] 
    else:
      column_str+= ' s.'+column_list[i] + ' = staged_updates.'+ column_list[i] +' and '

  return column_str

def check_column_from_df_full_merge(df,str_to_rmv=None):
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
   
    if i == len(column_list)-1:
      column_str+= ' s.'+column_list[i] + ' <> staged_updates.'+ column_list[i] 
    else:
      column_str+= ' s.'+column_list[i] + ' <> staged_updates.'+ column_list[i] +' OR '

  return column_str

# def add_sc_columns ( df , date_columns ):
#   print("adding columns iscurrent , start_date ,  end_date   ")
#   df=df.withColumn("iscurrent",F.lit('True').cast(BooleanType())).withColumn("start_date",F.lit(None).cast(TimestampType()).withColumn("end_date",F.lit(None).cast(TimestampType()))
#   return df

def overwrite_df_to_table (df ,table_name, target_db ):
  print('overwrite data to table {} on DB {} '.format (table_name,target_db))
  df.write.mode("overwrite").format("delta").saveAsTable(target_db+'.'+table_name)

def append_df_to_table (df ,table_name, target_db ):
  print('appending data to table {} on DB {} '.format (table_name,target_db))
  df.write.option("mergeSchema","true").mode("append").format("delta").saveAsTable(target_db+'.'+table_name)
  
def append_df_to_table_with_partition (df ,table_name, target_db , partition ):
  print('appending data to table {} on DB {} '.format (table_name,target_db))
  df.write.mode("append").format("delta").saveAsTable(target_db+'.'+table_name).partitionBy(partition)

# active table -- do not handle deleted data   
def merge_df_to_table (target_db , table_name, temp_table  ,pk):
  print('merge data to table {} on DB {} '.format (table_name,target_db))
  sql = '''MERGE INTO {0}.{1} as s 
      USING  {2} as n
      on s.{3} = n.{3}
      WHEN MATCHED THEN 
        UPDATE SET *
      WHEN NOT MATCHED 
        THEN INSERT *'''.format(target_db,table_name,temp_table , pk)
  print (sql)
  spark.sql(sql)
  print(' def merge_df_to_table was run')

# SC table --  and full - do not hendel deleted data 
# These rows will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
# These rows will INSERT new addresses of existing customers 
# Setting the mergeKey to NULL forces these rows to NOT MATCH and be INSERTed.
def merge_df_to_sc_table ( target_db ,table_name,pk ,columns_list ):
  print('merge data to table SC {} on DB {} '.format (table_name,target_db))
  sql = '''MERGE INTO {0}.{1} as s 
      USING  (
  SELECT  temp_table_sc.{2} as mergeKey, temp_table_sc.*
  FROM temp_table_sc
  UNION ALL
  SELECT NULL as mergeKey, temp_table_sc.*
  FROM temp_table_sc JOIN {0}.{1}
  ON temp_table_sc.{2} =  {1}.{2} 
  WHERE {1}.iscurrent = true  
) staged_updates    
      on s.{2} = staged_updates.mergeKey
      WHEN MATCHED and s.iscurrent = true THEN  
          UPDATE SET iscurrent = false, end_date = staged_updates.start_date 
      WHEN NOT MATCHED 
        THEN INSERT {3} 
       VALUES {3}'''.format(target_db,table_name, pk , columns_list )
  print (sql)
  spark.sql(sql)
  print(' def merge_df_to_sc_table was run')

def merge_df_to_sc_table_full ( target_db ,table_name,pk , columns_list , columns_check_list_full ):
  print('merge data to table SC {} on DB {} '.format (table_name,target_db))
  sql = '''MERGE INTO {0}.{1} as s 
      USING ( SELECT  temp_table_sc.{2} as mergeKey, temp_table_sc.*
  FROM temp_table_sc
  UNION ALL
  SELECT NULL as mergeKey, temp_table_sc.*
  FROM temp_table_sc JOIN {0}.{1}
  ON temp_table_sc.{2} =  {1}.{2} 
  WHERE {1}.iscurrent = true  
) staged_updates    
      on 
      s.{2} = staged_updates.mergeKey 
      --and {3}
      WHEN MATCHED and s.iscurrent = true and
        ({3})
      THEN 
       UPDATE SET iscurrent = false, end_date = staged_updates.start_date
      WHEN NOT MATCHED and  ({3}) 
      THEN INSERT {4} 
       VALUES {4}'''.format(target_db,table_name,pk, columns_check_list_full , columns_list )
  print ('***** Full Full Merge ******')
  print (sql)
  spark.sql(sql)
  print(' def merge_df_to_sc_table_full was run')
  
  
  
  
  
  
def merge_df_to_sc_table_full_all ( target_db ,table_name,pk , columns_list , columns_check_list_full_same, columns_check_list_full ):
  print('merge data to table SC {} on DB {} '.format (table_name,target_db))
  sql = '''MERGE INTO {0}.{1} as s 
      USING ( SELECT  *
        FROM temp_table_sc
        ) staged_updates    
      on      {5} 
    --  WHEN not MATCHED and s.iscurrent = true 
    --  THEN 
    --   UPDATE SET iscurrent = false, end_date = staged_updates.start_date
      WHEN NOT MATCHED   --and ({3}) 
      THEN INSERT {4} 
       VALUES {4}'''.format(target_db,table_name,pk, columns_check_list_full , columns_list , columns_check_list_full_same)
  print ('***** Full ALL Full Merge ******')
  print (sql)
  spark.sql(sql)
  
  sql_update_merge='''
  MERGE INTO {0}.{1} as s 
      USING (select min(load_time) as min_load, max(load_time)   as max_load, {2}  from {0}.{1}  where {2} in (
     select  {2} from {0}.{1} where iscurrent = true
     group by {2}
     having count (*) >1)
     group by {2}
        ) staged_updates    
      on   s.load_time =  staged_updates.min_load and  s.{2} = staged_updates.{2}
      WHEN  MATCHED  
     THEN 
       UPDATE SET iscurrent = false, end_date = staged_updates.max_load
      '''.format(target_db,table_name,pk)
  print ('***** Full ALL Full UPDATE OLD Merge ******')
  print (sql_update_merge)
  spark.sql(sql_update_merge)
  
#   sql_update_old ='''
#       select  concat('update {0}.{1} SET iscurrent = false, end_date = ','"', max_load ,'"', ' where {2} = ', {2}, '  and load_time = ','"', min_load,'"',  ';' ) 
#       from (
#     select min(load_time) as min_load, max(load_time)   as max_load, {2}  from {0}.{1}  where {2} in (
#     select  {2} from {0}.{1} where iscurrent = true
#     group by {2}
#     having count (*) >1)
#     group by {2})
#     '''.format(target_db,table_name,pk)
#   print ( '*******UPDATE OLD RECORD **************** ')
#   print (sql_update_old)
#   sql_to_run=spark.sql(sql_update_old)
#   print (type(sql_to_run))
#   if sql_to_run:
#     print (sql_to_run)
  
#     spark.sql(sql_to_run)
  print(' def merge_df_to_sc_table_full_all was run')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## JDBC 

# COMMAND ----------

def jdbc_get_data(source):
  db_connetion = spark.sql('select secrets_scope ,vendor,jdbc_hostname ,jdbc_database ,jdbc_port, isactive from lab_config.db_connection where isactive = 1 and  source_name = \'{}\''.format(source)).rdd.map(lambda x: x).collect()
  for connection in db_connetion:
      if connection.vendor == 'sqlserver':
        jdbcUsername = dbutils.secrets.get(scope = connection.secrets_scope, key = "username")
        jdbcPassword = dbutils.secrets.get(scope = connection.secrets_scope, key = "password")
        jdbcUrl = f"jdbc:sqlserver://{connection.jdbc_hostname}:{connection.jdbc_port};database={connection.jdbc_database};user={jdbcUsername};password={jdbcPassword}"
        connectionProperties = {
          "user" : jdbcUsername,
          "password" : jdbcPassword,
          "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
   
  print ('jdbcUrl: ' , jdbcUrl)
  print ('connectionProperties: ' , connectionProperties )
  return jdbcUrl, connectionProperties

# get the data for a table
def get_df_jdbc (jdbcUrl,query):
  return spark.read.format("jdbc").option("url", jdbcUrl).option("query", query).load()

# COMMAND ----------


