# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC to do: <br /> 
# MAGIC 1.handle empty API <br />
# MAGIC 2.handle full SCD <br />
# MAGIC 3.Remote RPC client disassociated. Likely due to containers exceeding thresholds, or network issues. Check driver logs for WARN messages. <br />
# MAGIC 4.create a buffer for dbtables from user side 
# MAGIC 5.add max time for last_updated_time ,last_increment_date in dbtables_manage

# COMMAND ----------

# MAGIC %md
# MAGIC (wiki link : https://wiki.aligntech.com/display/ITD/2.source+to+bronze under Data pulled directly with Databricks) <br />
# MAGIC ### notebook flow:  
# MAGIC 
# MAGIC 1.define local params <br />
# MAGIC 2.approach SFDC API and build all neccesary variables <br />
# MAGIC &nbsp; * - all sfdc credentials are  stored in a secrets scope <br />
# MAGIC 
# MAGIC preliminary stage for new tables :<br />
# MAGIC users define schema for new tables in lab_bronze and table configuration in lab_config.dbtables : https://aligntech-jarvis-dv1-e2.cloud.databricks.com/?o=7351116569006252#notebook/3028150989614966/command/3028150989614967 <br />
# MAGIC 
# MAGIC 3.process flow <br/>
# MAGIC &nbsp;&nbsp;3.1 defines the tables the process will go over according to lab_config.dbtables <br/>
# MAGIC &nbsp;&nbsp;3.2 fetch data from SFDC API according to the provided sql query <br/>
# MAGIC &nbsp;&nbsp;3.3 append data to bronze table <br/>
# MAGIC &nbsp;&nbsp;3.4 update data and insert new data into silver table  <br/>
# MAGIC &nbsp;&nbsp;3.5 update data in silver_scd table that holds all historical data  <br/>
# MAGIC 4.update dbtables_manage before next run

# COMMAND ----------

# MAGIC %run ./include/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## local params

# COMMAND ----------

source = "sfdc"
target_env = "lab"
first_data_layer = "bronze"
second_data_layer = "silver"
process_name = "{}_to_bronze".format(source)
pii_data='False'
minute_interval = 2


load_time = datetime.now() - timedelta(minutes=minute_interval)
load_time_str= get_current_timestamp(minute_interval)

action = '/services/data/v45.0/query/'

print (load_time)
print(load_time_str)




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

# MAGIC %sql
# MAGIC 
# MAGIC SET spark.sql.legacy.timeParserPolicy = Legacy;

# COMMAND ----------

# table_list = spark.sql('''select t.source_name as source_name, 
#   is_pii,
#   primary_key,
#   t.owner_name as owner_name,
#   lower(t.source_table_name) as source_table_name,
#   t.get_method as get_method,
#   date_column1_name,
#   date_column2_name,
#   query_text ,
#   first_query_text,
#   extraction_interval_minutes,
#   m.last_updated_time as last_updated_time,
#   m.last_increment_date as last_increment_date
#   from lab_config.dbtables t
#   left join lab_config.dbtables_manage m on t.source_name=m.source_name and t.source_table_name=m.source_table_name and t.owner_name=m.owner_name 
#   where t.source_name = 'sfdc' and is_pii = '0'   and isactive=1  and t.source_table_name ='opportunity' ''').collect()

# print(table_list)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## process flow 

# COMMAND ----------

#extract data from config table

table_list = get_table_list(source,0,load_time).collect()


# load_time = get_current_timestamp(load_time) #defines the time that all data will be extracted by(top limit)
for table_params in table_list:
  df_api = spark.createDataFrame([], StructType([])) #create an empty df 
  #define parameters for table from config
  source_name = table_params.source_name
  source_table_name = table_params.source_table_name
  date_column1_name = table_params.date_column1_name
  date_column2_name = table_params.date_column2_name
  first_query_text = table_params.first_query_text
  query_text = table_params.query_text
  get_method = table_params.get_method
  primary_key = table_params.primary_key
  extraction_interval = table_params.extraction_interval_minutes
  owner_name = table_params.owner_name
  last_increment_date = table_params.last_increment_date
  
  db_table_name = '{}_{}.{}_{}'.format(target_env,first_data_layer,source_name,source_table_name) #table name in DB
  print('running update for {}'.format(db_table_name))
  table_exist_ind = table_exists(db_table_name) #table indication in DB
    
  if table_exist_ind:  #check if table exists in DB
    first_run_ind = db_table_first_run(source_table_name)
    sql_statment = sql_query_builder(source,first_run_ind,first_query_text,query_text,convert_to_sfdc_timestamp(last_increment_date),convert_to_sfdc_timestamp(load_time))
    
    try:
      ## API Approach and DF enrichment ##
      df_api = create_df_sfdc(action,sql_statment,access_token) ##API approach
      df_api_enriched = string_to_timestamp_sfdc(df_api) # chnage createdate and lastmodified date to right format
      df_api_enriched = add_load_time(df_api_enriched,load_time) #add loadtime
      df_api_with_schema_from_db_table = derive_schema(df_api_enriched,source_table_name,db_table_name) #use schema from table on DF      
      num_rows = df_api.count()
      print('number of rows for current API call:{}'.format(num_rows))

      if df_api.count() == 0: ## no data to fetch from API
        print('no data in current API run')
      elif first_run_ind:
        print('extract data for the 1st time')
        append_df_to_table (df_api_with_schema_from_db_table ,source_name+'_'+source_table_name, target_env+'_'+first_data_layer) #append data to table in Bronze layer
        append_df_to_table (df_api_with_schema_from_db_table ,source_name+'_'+source_table_name, target_env+'_'+second_data_layer) #append data to current table in silver layer 
        df_sc = add_sc_columns_first_time(df_api_with_schema_from_db_table,date_column1_name)#add relevant fields for scd table
        append_df_to_table (df_sc ,source_name+'_'+source_table_name+'_scd', target_env+'_'+second_data_layer) #append data to current table in silver layer 
        new_increment_date = find_max_date (df_api_with_schema_from_db_table , date_column1_name) #take max timestamp from df 
        insert_dbtables_manage(source_name, owner_name , source_table_name   ,load_time ,  new_increment_date  ) #update config manage table



      else:
        print('ongoing process for {}'.format(db_table_name))
        append_df_to_table (df_api_with_schema_from_db_table ,source_name+'_'+source_table_name, target_env+'_'+first_data_layer) #append data to table in Bronze layer
        df_api_with_schema_from_db_table.createOrReplaceTempView('temp_table') #create temp view for merge 
        merge_df_to_table (target_env+'_'+second_data_layer , source_name+'_'+source_table_name, 'temp_table'  ,primary_key) #
        df_sc = add_sc_columns_first_time(df_api_with_schema_from_db_table,date_column1_name)#add relevant fields for scd table
        df_sc.createOrReplaceTempView('temp_table_sc')   #create temp view for merge SCD
        df_sc_column_list = column_list_from_df(df_sc)
        merge_df_to_sc_table(target_env+'_'+second_data_layer ,source_name+'_'+source_table_name+'_scd ',primary_key ,df_sc_column_list )
        new_increment_date = find_max_date (df_api_with_schema_from_db_table , date_column1_name) #take max timestamp from df 
        update_dbtables_manage(source_name, owner_name , source_table_name   ,load_time ,  new_increment_date  ) #update config manage table
        





      print('end_of_process for table {}'.format(db_table_name))
      new_increment_date = find_max_date (df_api_with_schema_from_db_table , date_column1_name) #take max timestamp from df 
      update_dbtables_manage(source_name, owner_name , source_table_name   ,load_time ,  new_increment_date  ) #update config manage table
      
    except Exception as err:
      print('process for {} didnt run properly : possible reason : {}'.format(db_table_name,err)  )
  else:
    print('{} doesnt exist , please create table with required schema in DB'.format(db_table_name))
    





# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## end of process

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC 
# MAGIC ## show  table in layers

# COMMAND ----------

# for table_params in table_list:
#   print(table_params.source_table_name)
#   try:
#     table_name = table_params.source_table_name
#     print(first_data_layer)
#     spark.sql('select load_time,count(1) from lab_bronze.{}_{} group by 1 order by 1 desc'.format(source,table_name)).show(20, False)
#     print(second_data_layer)
#     spark.sql('select load_time,count(1) from lab_silver.{}_{} group by 1 order by 1 desc'.format(source,table_name)).show(20, False)
#     print(second_data_layer+'SCD')
#     spark.sql('select load_time,count(1) from lab_silver.{}_{}_scd group by 1 order by 1 desc'.format(source,table_name)).show(20, False)
#     print('dbtables_manage')
#     spark.sql("select *  from lab_config.dbtables_manage where source_name = 'sfdc' and source_table_name = '{}'".format(table_name)).show(20, False)
# #     print('dbtables')
# #     spark.sql("select *  from lab_config.dbtables where source_name = 'sfdc' and source_table_name = '{}'".format(table_name)).show(20, False)
#   except Exception as err:
#     pass
# #     print(err)
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## truncate table

# COMMAND ----------

table_name = 'apttus_proposal_proposal_c'
print(first_data_layer)
spark.sql('truncate table lab_bronze.{}_{}'.format(source,table_name))
print(second_data_layer)
spark.sql('drop table if exists lab_silver.{}_{}'.format(source,table_name))
print(second_data_layer+'SCD')
spark.sql('drop table if exists lab_silver.{}_{}_scd'.format(source,table_name))
print('dbtables_manage')
spark.sql("delete  from lab_config.dbtables_manage where source_name = 'sfdc' and source_table_name = '{}'".format(table_name))




# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- OPTIMIZE lab_bronze.sfdc_contact

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select load_time,count(1)  from lab_bronze.sfdc_account group by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lab_config.dbtables where source_name = 'sfdc'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select now()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lab_config.dbtables_tablelog

# COMMAND ----------


