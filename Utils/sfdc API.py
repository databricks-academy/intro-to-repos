# Databricks notebook source
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
  
# def create_df_sfdc(action,query_SOQL,access_token):
#   action = '/services/data/v45.0/query/'
#   dataextract = sfdc_call(access_token,action,{'q':query_SOQL})
#   sf_df = pd.DataFrame(dataextract['records']).drop(columns='attributes')
#   sf_df = sf_df.dropna(axis='columns', how='all') 
#   api_running_status = dataextract['done']
#   while api_running_status is False:
#     action = dataextract['nextRecordsUrl']
#     dataextract = sfdc_call(access_token,action,{'q':query_SOQL})
#     df = pd.DataFrame(dataextract['records']).drop(columns='attributes')
#     df = df.dropna(axis='columns', how='all')  #hsegal(21/06/2021) - removes all null columns to prevent the process from falling
#     api_running_status = dataextract['done']
#     sf_df = sf_df.append(df)
#   f = spark.createDataFrame(sf_df)
#   return f
def pandas_object_to_string(df):
  columns = df.columns
  for column in columns:
    column_type = df[column].dtype
    if column_type == 'object':
      df["{}".format(column)]= df["{}".format(column)].astype(str)
  return df

def create_df_sfdc(action,query_SOQL,access_token):
  action = '/services/data/v45.0/query/'
  api_error = ''
  f = spark.createDataFrame([], StructType([]))
  try:
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
    pandas_object_to_string(sf_df)
    f = spark.createDataFrame(sf_df)
  except Exception as err:
    api_error = 'error:' + dataextract['errorCode'] + ', reason :' + dataextract['message']   
  return f,api_error

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


