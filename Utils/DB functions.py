# Databricks notebook source
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
