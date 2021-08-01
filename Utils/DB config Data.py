# Databricks notebook source
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
  
def insert_dbtables_tablelog(source_name, source_table_name,load_time ,run_ind,row_count,failure_reason ):
  sql="insert into lab_config.dbtables_tablelog  VALUES (\'{}\',\'{}\',\'{}\',{},{},\'{}\')".format(source_name, source_table_name,load_time ,run_ind,row_count,failure_reason)
  print ('********************** insert into dbtables_tablelog  **************************')  
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
