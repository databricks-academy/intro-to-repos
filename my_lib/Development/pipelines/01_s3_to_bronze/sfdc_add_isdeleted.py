# Databricks notebook source
# MAGIC %md
# MAGIC (wiki link : ) <br />
# MAGIC ### notebook flow:  
# MAGIC 
# MAGIC daily process that marks rows as isdeleted when not found in API

# COMMAND ----------

# MAGIC %run ./include/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## local params

# COMMAND ----------

source = "sfdc"
target_env = "lab"
data_layer = "silver"
process_name = "{}_is_deleted".format(source)
pii_data='False'
days_interval = 365
minutes_interval = -180




action = '/services/data/v45.0/query/'

load_time = datetime.now()
min_time = (load_time - timedelta(days=days_interval)).strftime("%Y-%m-%dT%H:%M:%S"+'.000+0000')
load_time_str = load_time.strftime("%Y-%m-%dT%H:%M:%S"+'.000+0000')
max_time = (load_time - timedelta(minutes=minutes_interval)).strftime("%Y-%m-%dT%H:%M:%S"+'.000+0000') ##currnet time for data extraction from API


print (load_time_str)
print(min_time)
print(max_time)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## sfdc paramters required to approach api

# COMMAND ----------

sfdc_username = dbutils.secrets.get(scope = source, key = "username")
sfdc_password = dbutils.secrets.get(scope = source, key = "password")
sfdc_client_id = dbutils.secrets.get(scope = source, key = "client_id")
sfdc_client_secret = dbutils.secrets.get(scope = source, key = "client_secret")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## API approach 

# COMMAND ----------

json_res = sfdc_acces_token(sfdc_client_id,sfdc_client_secret,sfdc_username,sfdc_password)
access_token = json_res['access_token']
instance_url = json_res['instance_url']

# COMMAND ----------

table_list = spark.sql("""select t.source_name as source_name, 
  primary_key,
  date_column1_name,
  lower(t.source_table_name) as source_table_name,
  date_column1_name,
  query_text
  from lab_config.dbtables t
  left join lab_config.dbtables_manage m on t.source_name=m.source_name and t.source_table_name=m.source_table_name and t.owner_name=m.owner_name 
  where t.source_name = 'sfdc' and is_pii = '0'   and isactive=1  and is_deleted_ind = 1 and t.source_table_name = 'opportunity' """).collect()

print(table_list)

# COMMAND ----------

table_list = get_isdeleted_table_list(source,pii_data).collect()

for table_params in table_list:
  df_api = spark.createDataFrame([], StructType([])) #create an empty df 
  #define parameters for table from config
  source_name = table_params.source_name
  source_table_name = table_params.source_table_name
  date_column1_name = table_params.date_column1_name
  primary_key = table_params.primary_key
  query_text=table_params.query_text
  date_column1_name = table_params.date_column1_name
  
  db_table_name = '{}_{}.{}_{}'.format(target_env,data_layer,source_name,source_table_name)

  ## create sql statemnt and extract data from api by PK
  sql_statment = build_sql_with_pk(query_text,primary_key,min_time,max_time)
  print(sql_statment)
  df_api = create_df_sfdc(action,sql_statment,access_token)
  ## delete from silver
  delete_from_dbtable_by_pk(db_table_name,df_api,primary_key,min_time,date_column1_name)
  ## update in scd
  update_sc_deleted_by_pk(db_table_name+'_scd',df_api,primary_key,min_time,date_column1_name,load_time)  
  


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## End process

# COMMAND ----------


