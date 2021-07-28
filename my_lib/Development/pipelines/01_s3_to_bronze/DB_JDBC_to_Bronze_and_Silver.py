# Databricks notebook source
# MAGIC %md
# MAGIC (wiki link : https://wiki.aligntech.com/display/ITD/2.source+to+bronze under Data pulled directly with Databricks) <br />
# MAGIC ### notebook flow:  
# MAGIC 
# MAGIC 1.define local params <br />
# MAGIC 2.configer JDBC <br />
# MAGIC &nbsp; * - get data from table db_connection (need to add the credentials a secrets scope) <br />
# MAGIC 
# MAGIC preliminary stage for new tables :<br />
# MAGIC add data to table dbtables with insert statment or with the notebook : 
# MAGIC 
# MAGIC by insert statment:
# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC    \#source_name , is_pii , primary_key , extraction_interval_minutes , owner_name , source_table_name , get_method , date_column1_name , date_column2_name , isactive , query_text , first_query_text , updated_by , start_time , last_updated_time
# MAGIC    insert into lab_config.dbtables VALUES ('mat', 0,'ContactID',1,'dbo', 'contact','i','DateUpdated','DateCreated',1,'select * from dbo.Contact where (DateUpdated >\'{}\' and DateUpdated<=\'{}\')', 'select * from    dbo.Contact','mika',current_date(),current_date ())
# MAGIC    
# MAGIC by notebook:   
# MAGIC     add table configuration in lab_config.dbtables : https://aligntech-jarvis-dv1-e2.cloud.databricks.com/?o=7351116569006252#notebook/3028150989614966/command/3028150989614967 <br />
# MAGIC 
# MAGIC 3.process flow <br/>
# MAGIC &nbsp;&nbsp;3.1 defines the tables the process will go over according to lab_config.dbtables <br/>
# MAGIC &nbsp;&nbsp;3.2 fetch data from JDBC according to the provided sql query <br/>
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

dbutils.widgets.combobox('target_env', 'lab', ['lab', 'master'])
dbutils.widgets.combobox('source', 'mat', ['mat','mes'])
dbutils.widgets.combobox('pii_data', 'False', ['False','True'])
dbutils.widgets.combobox('target_db_bronze', 'lab_bronze', ['lab_bronze','lab_red'])
dbutils.widgets.combobox('target_db_silver', 'lab_silver', ['lab_silver','lab_red'])

# COMMAND ----------

# source = 'mat'
# target_db_bronze = "lab_bronze"
# target_db_silver = "lab_silver"
# target_env = "lab"
# pii_data='False'
# data_layer = "bronze"
# process_name = "{}_to_bronze".format(source)

source =  getArgument('source')
target_db_bronze =  getArgument('target_db_bronze')
target_db_silver =  getArgument('target_db_silver')
pii_data =  getArgument('pii_data')



end_time= datetime.now().replace(microsecond=0) - timedelta(minutes=1)

#end_time_str= end_time.strftime("%Y-%m-%d %H:%M:%S")
end_time_with_convert= date_to_string_convert(end_time)
end_time_str=  date_to_string(end_time)
print ('end_time', end_time)
print ('end_time_str' , end_time_str)
print ('end_time_with_convert' , end_time_with_convert)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## JDBC paramters

# COMMAND ----------

jdbcUrl, connectionProperties = jdbc_get_data(source)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## get tables 

# COMMAND ----------

# tables=get_table_list('mat',0,end_time)

# COMMAND ----------

tables=get_table_list(source,pii_data,end_time).collect()
print(tables)


# COMMAND ----------

# def get_df_jdbc (jdbcUrl,query):
#   return spark.read.format("jdbc").option("url", jdbcUrl).option("query", query).load()



# COMMAND ----------

for table_param in tables: 
    print ('last' , table_param['last_updated_time'])
    source_name = table_param.source_name
    source_table_name = table_param.source_table_name
    date_column1_name = table_param.date_column1_name
    date_column2_name = table_param.date_column2_name
    first_query_text = table_param.first_query_text
    query_text = table_param.query_text
    get_method = table_param.get_method
    primary_key = table_param.primary_key
    extraction_interval = table_param.extraction_interval_minutes
    owner_name = table_param.owner_name
    last_increment_date = table_param.last_increment_date
    print ('last_increment_date' , last_increment_date)
    table_name = '{}_{}'.format(source_name,source_table_name) #table name in DB
    print('running update for {}'.format(table_name))
    # the table was updated befor 
    if table_param['last_updated_time']:
      print ( "the table {} was updated befor ".format (table_param["source_table_name"]) )
      last_increment_date_str=date_to_string(last_increment_date)
      #last_increment_date_str=date_to_string_convert(last_increment_date)
      
      sql=query_text.format(last_increment_date_str,end_time_str)
      #sql=query_text.format(last_increment_date_str,end_time_with_convert)
      print ('last_increment_date_str' , last_increment_date_str)
      print ('end_time_str' , end_time_str)
      print(sql)
      df=get_df_jdbc(jdbcUrl,sql )  
      if df.rdd.isEmpty() : 
        print('there are NO new rows for table '+ table_param["source_table_name"] + ' update table dbtables_manage' )
        update_dbtables_manage_nodata(table_param["source_name"], table_param["owner_name"] , table_param["source_table_name"] ,end_time )
      else:
        #first_time='False'
       #add lode time   
        columns_check_list_full= check_column_from_df_full_merge (df , table_param["primary_key"])
        columns_check_list_full_same = check_same_column_from_df_full_merge (df )
        df = add_load_time(df , end_time)
        max_time = find_max_date_to_date (df, table_param["date_column1_name"] , end_time)
        print('max_time ', max_time)
        df.createOrReplaceTempView("temp_table")
        #append df to  bronze table
        append_df_to_table (df , '{}_{}'.format(table_param["source_name"],table_param["source_table_name"]) , target_db_bronze  )
        merge_df_to_table (target_db_silver,'{}_{}'.format(table_param["source_name"],table_param["source_table_name"]),'temp_table', table_param["primary_key"])
        df_sc=add_sc_columns_first_time(df , table_param["date_column1_name"] )
        df_sc.createOrReplaceTempView("temp_table_sc")
        column_list_sc=column_list_from_df(df_sc)
        print ( 'column_list_sc - ', column_list_sc )
        
        if get_method=='i':
         
          merge_df_to_sc_table(target_db_silver,'{}_{}_{}'.format(table_param["source_name"],table_param["source_table_name"],'sc'), table_param["primary_key"] , column_list_sc)
        else: 
#           columns_check_list_full= check_column_from_df_full_merge (df)
          print ('columns_check_list_full' , columns_check_list_full)
#           merge_df_to_sc_table_full(target_db_silver,'{}_{}_{}'.format(table_param["source_name"],table_param["source_table_name"],'sc'), table_param["primary_key"] , column_list_sc , columns_check_list_full)
          merge_df_to_sc_table_full_all(target_db_silver,'{}_{}_{}'.format(table_param["source_name"],table_param["source_table_name"],'sc'), table_param["primary_key"] , column_list_sc , columns_check_list_full_same ,  columns_check_list_full)
        update_dbtables_manage(table_param["source_name"], table_param["owner_name"] , table_param["source_table_name"] ,end_time , max_time  )

    # this is the first time we get tha table
    else:
      print ( 'this is the first time table {} is updated  '.format (table_param["source_table_name"]) )
      df=get_df_jdbc(jdbcUrl,table_param['first_query_text'])
      #first_time='True'
       #add lode time   
      df = add_load_time(df , end_time)
      # need to hendel MAX 
      max_time = find_max_date_to_date (df, table_param["date_column1_name"] , end_time)
      print('max_time ', max_time)
      # overwrite df to  bronze table
      overwrite_df_to_table (df , '{}_{}'.format(table_param["source_name"],table_param["source_table_name"]) , target_db_bronze  )
      overwrite_df_to_table (df , '{}_{}'.format(table_param["source_name"],table_param["source_table_name"]) , target_db_silver  )
      print (table_param["date_column1_name"])
      df_sc=add_sc_columns_first_time(df , table_param["date_column1_name"] )
      overwrite_df_to_table (df_sc , '{}_{}_{}'.format(table_param["source_name"],table_param["source_table_name"],'sc') , target_db_silver  )
      insert_dbtables_manage(table_param["source_name"], table_param["owner_name"] , table_param["source_table_name"] ,end_time , max_time  )
      
   

# COMMAND ----------

# %sql 
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING (select min(load_time) as min_load, max(load_time)   as max_load, EquipmentCardID  from lab_silver.mat_svc_equipmentcard_sc  where EquipmentCardID in (
#      select  EquipmentCardID from lab_silver.mat_svc_equipmentcard_sc where iscurrent = true
#      group by EquipmentCardID
#      having count (*) >1)
#      group by EquipmentCardID
#         ) staged_updates    
#       on     s.load_time =staged_updates.min_load and s.EquipmentCardID = staged_updates.EquipmentCardID
#       WHEN  MATCHED THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.max_load

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC -- select * from lab_silver.mat_svc_equipmentcard_sc  where EquipmentCardID in (
# MAGIC --      select  EquipmentCardID from lab_silver.mat_svc_equipmentcard_sc where --iscurrent = true
# MAGIC --      group by EquipmentCardID
# MAGIC --      having count (*) >1) order by EquipmentCardID
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC -- select min(load_time) as min_load, max(load_time)   as max_load, EquipmentCardID  from lab_silver.mat_svc_equipmentcard_sc  where EquipmentCardID in (
# MAGIC --      select  EquipmentCardID from lab_silver.mat_svc_equipmentcard_sc where iscurrent = true
# MAGIC --      group by EquipmentCardID
# MAGIC --      having count (*) >1)
# MAGIC --      group by EquipmentCardID

# COMMAND ----------

# %sql
# select min(load_time) as min_load, max(load_time)   as max_load, EquipmentCardID  from lab_silver.mat_svc_equipmentcard_sc  where EquipmentCardID in (
#      select  EquipmentCardID from lab_silver.mat_svc_equipmentcard_sc where iscurrent = true
#      group by EquipmentCardID
#      having count (*) >1)
#      group by EquipmentCardID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select  concat('update lab_silver.mat_svc_equipmentcard_sc SET iscurrent = false, end_date = ','"', max_load ,'"', ' where EquipmentCardID = ', EquipmentCardID, '  and load_time = ',''', min_load,''',  ';' ) 
# MAGIC --       from (
# MAGIC --     select min(load_time) as min_load, max(load_time)   as max_load, EquipmentCardID  from lab_silver.mat_svc_equipmentcard_sc  where EquipmentCardID in (
# MAGIC --     select  EquipmentCardID from lab_silver.mat_svc_equipmentcard_sc where iscurrent = true
# MAGIC --     group by EquipmentCardID
# MAGIC --     having count (*) >1)
# MAGIC --     group by EquipmentCardID)
# MAGIC     
# MAGIC     

# COMMAND ----------



# COMMAND ----------

# %sql
# update lab_silver.mat_svc_equipmentcard_sc SET iscurrent = false, end_date = "2021-07-11 14:59:02.765195" where EquipmentCardID = 100170 and load_time = "2021-07-11 14:59:02.765195";


# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
# MAGIC --       USING ( SELECT  *
# MAGIC --         FROM temp_table_sc
# MAGIC --         ) staged_updates    
# MAGIC --       on       s.EquipmentCardID = staged_updates.EquipmentCardID and  s.SoldToBusinessPartnerID = staged_updates.SoldToBusinessPartnerID and  s.HolderBusinessPartnerID = staged_updates.HolderBusinessPartnerID and  s.ItemID = staged_updates.ItemID and  s.SerialIdentifier = staged_updates.SerialIdentifier and  s.Notes = staged_updates.Notes and  s.RowStatusID = staged_updates.RowStatusID and  s.DateCreated = staged_updates.DateCreated and  s.CreatedByUserID = staged_updates.CreatedByUserID and  s.DateUpdated = staged_updates.DateUpdated and  s.UpdatedByUserID = staged_updates.UpdatedByUserID 
# MAGIC --     --  WHEN not MATCHED and s.iscurrent = true 
# MAGIC --     --  THEN 
# MAGIC --     --   UPDATE SET iscurrent = false, end_date = staged_updates.start_date
# MAGIC --       WHEN NOT MATCHED   --and ( s.SoldToBusinessPartnerID <> staged_updates.SoldToBusinessPartnerID OR  s.HolderBusinessPartnerID <> staged_updates.HolderBusinessPartnerID OR  s.ItemID <> staged_updates.ItemID OR  s.SerialIdentifier <> staged_updates.SerialIdentifier OR  s.Notes <> staged_updates.Notes OR  s.RowStatusID <> staged_updates.RowStatusID OR  s.DateCreated <> staged_updates.DateCreated OR  s.CreatedByUserID <> staged_updates.CreatedByUserID OR  s.DateUpdated <> staged_updates.DateUpdated OR  s.UpdatedByUserID <> staged_updates.UpdatedByUserID) 
# MAGIC --       THEN INSERT (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date) 
# MAGIC --        VALUES (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date)

# COMMAND ----------

# %sql
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  *
#   FROM temp_table_sc
# ) staged_updates    
#       on 
#      -- s.EquipmentCardID = staged_updates.mergeKey and 
#       s.EquipmentCardID = staged_updates.EquipmentCardID and  s.SoldToBusinessPartnerID = staged_updates.SoldToBusinessPartnerID and  s.HolderBusinessPartnerID = staged_updates.HolderBusinessPartnerID and  s.ItemID = staged_updates.ItemID and  s.SerialIdentifier = staged_updates.SerialIdentifier and  s.Notes = staged_updates.Notes and  s.RowStatusID = staged_updates.RowStatusID and  s.DateCreated = staged_updates.DateCreated and  s.CreatedByUserID = staged_updates.CreatedByUserID and  s.DateUpdated = staged_updates.DateUpdated and  s.UpdatedByUserID = staged_updates.UpdatedByUserID 
#     --  WHEN not MATCHED and s.iscurrent = true 
#     --  THEN 
#     --   UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#       WHEN NOT MATCHED and s.iscurrent = true   ( s.SoldToBusinessPartnerID <> staged_updates.SoldToBusinessPartnerID OR  s.HolderBusinessPartnerID <> staged_updates.HolderBusinessPartnerID OR  s.ItemID <> staged_updates.ItemID OR  s.SerialIdentifier <> staged_updates.SerialIdentifier OR  s.Notes <> staged_updates.Notes OR  s.RowStatusID <> staged_updates.RowStatusID OR  s.DateCreated <> staged_updates.DateCreated OR  s.CreatedByUserID <> staged_updates.CreatedByUserID OR  s.DateUpdated <> staged_updates.DateUpdated OR  s.UpdatedByUserID <> staged_updates.UpdatedByUserID) 
#       THEN INSERT (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date) 
#        VALUES (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date)

# COMMAND ----------

# %sql
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on 
#       s.EquipmentCardID = staged_updates.mergeKey 
#       --and  s.SoldToBusinessPartnerID <> staged_updates.SoldToBusinessPartnerID OR  s.HolderBusinessPartnerID <> staged_updates.HolderBusinessPartnerID OR  s.ItemID <> staged_updates.ItemID OR  s.SerialIdentifier <> staged_updates.SerialIdentifier OR  s.Notes <> staged_updates.Notes OR  s.RowStatusID <> staged_updates.RowStatusID OR  s.DateCreated <> staged_updates.DateCreated OR  s.CreatedByUserID <> staged_updates.CreatedByUserID OR  s.DateUpdated <> staged_updates.DateUpdated OR  s.UpdatedByUserID <> staged_updates.UpdatedByUserID
#       WHEN MATCHED and s.iscurrent = true and
#         ( s.SoldToBusinessPartnerID <> staged_updates.SoldToBusinessPartnerID OR  s.HolderBusinessPartnerID <> staged_updates.HolderBusinessPartnerID OR  s.ItemID <> staged_updates.ItemID OR  s.SerialIdentifier <> staged_updates.SerialIdentifier OR  s.Notes <> staged_updates.Notes OR  s.RowStatusID <> staged_updates.RowStatusID OR  s.DateCreated <> staged_updates.DateCreated OR  s.CreatedByUserID <> staged_updates.CreatedByUserID OR  s.DateUpdated <> staged_updates.DateUpdated OR  s.UpdatedByUserID <> staged_updates.UpdatedByUserID)
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#       WHEN NOT MATCHED --and  ( s.SoldToBusinessPartnerID <> staged_updates.SoldToBusinessPartnerID OR  s.HolderBusinessPartnerID <> staged_updates.HolderBusinessPartnerID OR  s.ItemID <> staged_updates.ItemID OR  s.SerialIdentifier <> staged_updates.SerialIdentifier OR  s.Notes <> staged_updates.Notes OR  s.RowStatusID <> staged_updates.RowStatusID OR  s.DateCreated <> staged_updates.DateCreated OR  s.CreatedByUserID <> staged_updates.CreatedByUserID OR  s.DateUpdated <> staged_updates.DateUpdated OR  s.UpdatedByUserID <> staged_updates.UpdatedByUserID) 
#       THEN INSERT (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date) 
#        VALUES (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date)
#  def merge_df_to_sc_table_full was run

# COMMAND ----------

# %sql
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on 
#       s.EquipmentCardID = staged_updates.mergeKey 
#       --and  s.SoldToBusinessPartnerID <> staged_updates.SoldToBusinessPartnerID OR  s.HolderBusinessPartnerID <> staged_updates.HolderBusinessPartnerID OR  s.ItemID <> staged_updates.ItemID OR  s.SerialIdentifier <> staged_updates.SerialIdentifier OR  s.Notes <> staged_updates.Notes OR  s.RowStatusID <> staged_updates.RowStatusID OR  s.DateCreated <> staged_updates.DateCreated OR  s.CreatedByUserID <> staged_updates.CreatedByUserID OR  s.DateUpdated <> staged_updates.DateUpdated OR  s.UpdatedByUserID <> staged_updates.UpdatedByUserID
#       WHEN MATCHED and s.iscurrent = true and
#         ( s.SoldToBusinessPartnerID <> staged_updates.SoldToBusinessPartnerID OR  s.HolderBusinessPartnerID <> staged_updates.HolderBusinessPartnerID OR  s.ItemID <> staged_updates.ItemID OR  s.SerialIdentifier <> staged_updates.SerialIdentifier OR  s.Notes <> staged_updates.Notes OR  s.RowStatusID <> staged_updates.RowStatusID OR  s.DateCreated <> staged_updates.DateCreated OR  s.CreatedByUserID <> staged_updates.CreatedByUserID OR  s.DateUpdated <> staged_updates.DateUpdated OR  s.UpdatedByUserID <> staged_updates.UpdatedByUserID)
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#       WHEN NOT MATCHED and  ( s.SoldToBusinessPartnerID <> staged_updates.SoldToBusinessPartnerID OR  s.HolderBusinessPartnerID <> staged_updates.HolderBusinessPartnerID OR  s.ItemID <> staged_updates.ItemID OR  s.SerialIdentifier <> staged_updates.SerialIdentifier OR  s.Notes <> staged_updates.Notes OR  s.RowStatusID <> staged_updates.RowStatusID OR  s.DateCreated <> staged_updates.DateCreated OR  s.CreatedByUserID <> staged_updates.CreatedByUserID OR  s.DateUpdated <> staged_updates.DateUpdated OR  s.UpdatedByUserID <> staged_updates.UpdatedByUserID) 
#       THEN INSERT (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date) 
#        VALUES (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date)

# COMMAND ----------

# %sql 
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on 
#       s.EquipmentCardID = staged_updates.mergeKey 
#       --and  NOT (s.SoldToBusinessPartnerID <=> staged_updates.SoldToBusinessPartnerID) OR  NOT (s.HolderBusinessPartnerID <=> staged_updates.HolderBusinessPartnerID) OR  NOT (s.ItemID <=> staged_updates.ItemID) OR  NOT (s.SerialIdentifier <=> staged_updates.SerialIdentifier) OR  NOT (s.Notes <=> staged_updates.Notes) OR  NOT (s.RowStatusID <=> staged_updates.RowStatusID) OR  NOT (s.DateCreated <=> staged_updates.DateCreated) OR  NOT (s.CreatedByUserID <=> staged_updates.CreatedByUserID) OR  NOT (s.DateUpdated <=> staged_updates.DateUpdated) OR  NOT (s.UpdatedByUserID <=> staged_updates.UpdatedByUserID)
#       WHEN MATCHED and s.iscurrent = true and
#         ( NOT (s.SoldToBusinessPartnerID <=> staged_updates.SoldToBusinessPartnerID) OR  NOT (s.HolderBusinessPartnerID <=> staged_updates.HolderBusinessPartnerID) OR  NOT (s.ItemID <=> staged_updates.ItemID) OR  NOT (s.SerialIdentifier <=> staged_updates.SerialIdentifier) OR  NOT (s.Notes <=> staged_updates.Notes) OR  NOT (s.RowStatusID <=> staged_updates.RowStatusID) OR  NOT (s.DateCreated <=> staged_updates.DateCreated) OR  NOT (s.CreatedByUserID <=> staged_updates.CreatedByUserID) OR  NOT (s.DateUpdated <=> staged_updates.DateUpdated) OR  NOT (s.UpdatedByUserID <=> staged_updates.UpdatedByUserID))
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#       WHEN NOT MATCHED and  ( NOT (s.SoldToBusinessPartnerID <=> staged_updates.SoldToBusinessPartnerID) OR  NOT (s.HolderBusinessPartnerID <=> staged_updates.HolderBusinessPartnerID) OR  NOT (s.ItemID <=> staged_updates.ItemID) OR  NOT (s.SerialIdentifier <=> staged_updates.SerialIdentifier) OR  NOT (s.Notes <=> staged_updates.Notes) OR  NOT (s.RowStatusID <=> staged_updates.RowStatusID) OR  NOT (s.DateCreated <=> staged_updates.DateCreated) OR  NOT (s.CreatedByUserID <=> staged_updates.CreatedByUserID) OR  NOT (s.DateUpdated <=> staged_updates.DateUpdated) OR  NOT (s.UpdatedByUserID <=> staged_updates.UpdatedByUserID)) 
#       THEN INSERT (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date) 
#        VALUES (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date)

# COMMAND ----------

# %sql
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on 
#       s.EquipmentCardID = staged_updates.mergeKey 
#       --and  NOT (s.SoldToBusinessPartnerID <=> staged_updates.SoldToBusinessPartnerID) OR  NOT (s.HolderBusinessPartnerID <=> staged_updates.HolderBusinessPartnerID) OR  NOT (s.ItemID <=> staged_updates.ItemID) OR  NOT (s.SerialIdentifier <=> staged_updates.SerialIdentifier) OR  NOT (s.Notes <=> staged_updates.Notes) OR  NOT (s.RowStatusID <=> staged_updates.RowStatusID) OR  NOT (s.DateCreated <=> staged_updates.DateCreated) OR  NOT (s.CreatedByUserID <=> staged_updates.CreatedByUserID) OR  NOT (s.DateUpdated <=> staged_updates.DateUpdated) OR  NOT (s.UpdatedByUserID <=> staged_updates.UpdatedByUserID)
#       WHEN MATCHED and s.iscurrent = true and
#         ( NOT (s.SoldToBusinessPartnerID <=> staged_updates.SoldToBusinessPartnerID) OR  NOT (s.HolderBusinessPartnerID <=> staged_updates.HolderBusinessPartnerID) OR  NOT (s.ItemID <=> staged_updates.ItemID) OR  NOT (s.SerialIdentifier <=> staged_updates.SerialIdentifier) OR  NOT (s.Notes <=> staged_updates.Notes) OR  NOT (s.RowStatusID <=> staged_updates.RowStatusID) OR  NOT (s.DateCreated <=> staged_updates.DateCreated) OR  NOT (s.CreatedByUserID <=> staged_updates.CreatedByUserID) OR  NOT (s.DateUpdated <=> staged_updates.DateUpdated) OR  NOT (s.UpdatedByUserID <=> staged_updates.UpdatedByUserID))
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#       WHEN NOT MATCHED and  ( NOT (s.SoldToBusinessPartnerID <=> staged_updates.SoldToBusinessPartnerID) OR  NOT (s.HolderBusinessPartnerID <=> staged_updates.HolderBusinessPartnerID) OR  NOT (s.ItemID <=> staged_updates.ItemID) OR  NOT (s.SerialIdentifier <=> staged_updates.SerialIdentifier) OR  NOT (s.Notes <=> staged_updates.Notes) OR  NOT (s.RowStatusID <=> staged_updates.RowStatusID) OR  NOT (s.DateCreated <=> staged_updates.DateCreated) OR  NOT (s.CreatedByUserID <=> staged_updates.CreatedByUserID) OR  NOT (s.DateUpdated <=> staged_updates.DateUpdated) OR  NOT (s.UpdatedByUserID <=> staged_updates.UpdatedByUserID)) 
#       THEN INSERT (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date) 
#        VALUES (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date)

# COMMAND ----------

# %sql
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on 
#       s.EquipmentCardID = staged_updates.mergeKey 
#       WHEN MATCHED and s.iscurrent = true 
#      and
#         ( s.SoldToBusinessPartnerID != staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID != staged_updates.HolderBusinessPartnerID or  s.ItemID != staged_updates.ItemID or  s.SerialIdentifier != staged_updates.SerialIdentifier or  s.Notes != staged_updates.Notes or  s.RowStatusID != staged_updates.RowStatusID or  s.DateCreated != staged_updates.DateCreated or  s.CreatedByUserID != staged_updates.CreatedByUserID or  s.DateUpdated != staged_updates.DateUpdated or  s.UpdatedByUserID != staged_updates.UpdatedByUserID)
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#       WHEN NOT MATCHED  and s.iscurrent = true and
#       ( s.SoldToBusinessPartnerID != staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID != staged_updates.HolderBusinessPartnerID or  s.ItemID != staged_updates.ItemID or  s.SerialIdentifier != staged_updates.SerialIdentifier or  s.Notes != staged_updates.Notes or  s.RowStatusID != staged_updates.RowStatusID or    s.CreatedByUserID != staged_updates.CreatedByUserID or  s.DateUpdated != staged_updates.DateUpdated or  s.UpdatedByUserID != staged_updates.UpdatedByUserID) 
#       THEN INSERT (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date) 
#        VALUES (EquipmentCardID, SoldToBusinessPartnerID, HolderBusinessPartnerID, ItemID, SerialIdentifier, Notes, RowStatusID, DateCreated, CreatedByUserID, DateUpdated, UpdatedByUserID, load_time, iscurrent, start_date, end_date)
       

# COMMAND ----------

# print (check_column_from_df_full_merge(df))

# COMMAND ----------

# %sql
#  (SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc)
#   UNION ALL
#   (SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  ) order by 

# COMMAND ----------

# %sql 
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on 
#       s.EquipmentCardID = staged_updates.mergeKey 
#       --and  s.EquipmentCardID != staged_updates.EquipmentCardID or  s.SoldToBusinessPartnerID != staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID != staged_updates.HolderBusinessPartnerID or  s.ItemID != staged_updates.ItemID or  s.SerialIdentifier != staged_updates.SerialIdentifier or  s.Notes != staged_updates.Notes or  s.RowStatusID != staged_updates.RowStatusID or  s.DateCreated != staged_updates.DateCreated or  s.CreatedByUserID != staged_updates.CreatedByUserID or  s.DateUpdated != staged_updates.DateUpdated or  s.UpdatedByUserID != staged_updates.UpdatedByUserID
#       WHEN MATCHED and s.iscurrent = true and
#         ( s.EquipmentCardID != staged_updates.EquipmentCardID or  s.SoldToBusinessPartnerID != staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID != staged_updates.HolderBusinessPartnerID or  s.ItemID != staged_updates.ItemID or  s.SerialIdentifier != staged_updates.SerialIdentifier or  s.Notes != staged_updates.Notes or  s.RowStatusID != staged_updates.RowStatusID or  s.DateCreated != staged_updates.DateCreated or  s.CreatedByUserID != staged_updates.CreatedByUserID or  s.DateUpdated != staged_updates.DateUpdated or  s.UpdatedByUserID != staged_updates.UpdatedByUserID)
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#       WHEN NOT MATCHED and  ( s.EquipmentCardID != staged_updates.EquipmentCardID or  s.SoldToBusinessPartnerID != staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID != staged_updates.HolderBusinessPartnerID or  s.ItemID != staged_updates.ItemID or  s.SerialIdentifier != staged_updates.SerialIdentifier or  s.Notes != staged_updates.Notes or  s.RowStatusID != staged_updates.RowStatusID or  s.DateCreated != staged_updates.DateCreated or  s.CreatedByUserID != staged_updates.CreatedByUserID or  s.DateUpdated != staged_updates.DateUpdated or  s.UpdatedByUserID != staged_updates.UpdatedByUserID) 
#       THEN INSERT (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date) 
#        VALUES (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date)

# COMMAND ----------


# %sql 
# select * from lab_silver.mat_svc_equipmentcard_sc  limit 12;

# COMMAND ----------

# %sql

# select * from temp_table_sc limit 3

# COMMAND ----------


# %sql
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on 
#       s.EquipmentCardID = staged_updates.mergeKey 
#       --and  s.EquipmentCardID != staged_updates.EquipmentCardID or  s.SoldToBusinessPartnerID != staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID != staged_updates.HolderBusinessPartnerID or  s.ItemID != staged_updates.ItemID or  s.SerialIdentifier != staged_updates.SerialIdentifier or  s.Notes != staged_updates.Notes or  s.RowStatusID != staged_updates.RowStatusID or  s.DateCreated != staged_updates.DateCreated or  s.CreatedByUserID != staged_updates.CreatedByUserID or  s.DateUpdated != staged_updates.DateUpdated or  s.UpdatedByUserID != staged_updates.UpdatedByUserID
#       WHEN MATCHED and --s.iscurrent = true
#         ( s.EquipmentCardID != staged_updates.EquipmentCardID or  s.SoldToBusinessPartnerID != staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID != staged_updates.HolderBusinessPartnerID or  s.ItemID != staged_updates.ItemID or  s.SerialIdentifier != staged_updates.SerialIdentifier or  s.Notes != staged_updates.Notes or  s.RowStatusID != staged_updates.RowStatusID or  s.DateCreated != staged_updates.DateCreated or  s.CreatedByUserID != staged_updates.CreatedByUserID or  s.DateUpdated != staged_updates.DateUpdated or  s.UpdatedByUserID != staged_updates.UpdatedByUserID)
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#       WHEN NOT MATCHED and  ( s.EquipmentCardID != staged_updates.EquipmentCardID or  s.SoldToBusinessPartnerID != staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID != staged_updates.HolderBusinessPartnerID or  s.ItemID != staged_updates.ItemID or  s.SerialIdentifier != staged_updates.SerialIdentifier or  s.Notes != staged_updates.Notes or  s.RowStatusID != staged_updates.RowStatusID or  s.DateCreated != staged_updates.DateCreated or  s.CreatedByUserID != staged_updates.CreatedByUserID or  s.DateUpdated != staged_updates.DateUpdated or  s.UpdatedByUserID != staged_updates.UpdatedByUserID) THEN INSERT (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date) 
#        VALUES (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date)

# COMMAND ----------

# %sql
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on 
#       s.EquipmentCardID = staged_updates.mergeKey 
#       --and  s.EquipmentCardID!= staged_updates.EquipmentCardID or  s.SoldToBusinessPartnerID!= staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID!= staged_updates.HolderBusinessPartnerID or  s.ItemID!= staged_updates.ItemID or  s.SerialIdentifier!= staged_updates.SerialIdentifier or  s.Notes!= staged_updates.Notes or  s.RowStatusID!= staged_updates.RowStatusID or  s.DateCreated!= staged_updates.DateCreated or  s.CreatedByUserID!= staged_updates.CreatedByUserID or  s.DateUpdated!= staged_updates.DateUpdated or  s.UpdatedByUserID!= staged_updates.UpdatedByUserID
#       WHEN MATCHED and --s.iscurrent = true
#         ( s.EquipmentCardID!= staged_updates.EquipmentCardID or  s.SoldToBusinessPartnerID!= staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID!= staged_updates.HolderBusinessPartnerID or  s.ItemID!= staged_updates.ItemID or  s.SerialIdentifier!= staged_updates.SerialIdentifier or  s.Notes!= staged_updates.Notes or  s.RowStatusID!= staged_updates.RowStatusID or  s.DateCreated!= staged_updates.DateCreated or  s.CreatedByUserID!= staged_updates.CreatedByUserID or  s.DateUpdated!= staged_updates.DateUpdated or  s.UpdatedByUserID!= staged_updates.UpdatedByUserID)
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#       WHEN NOT MATCHED and  ( s.EquipmentCardID!= staged_updates.EquipmentCardID or  s.SoldToBusinessPartnerID!= staged_updates.SoldToBusinessPartnerID or  s.HolderBusinessPartnerID!= staged_updates.HolderBusinessPartnerID or  s.ItemID!= staged_updates.ItemID or  s.SerialIdentifier!= staged_updates.SerialIdentifier or  s.Notes!= staged_updates.Notes or  s.RowStatusID!= staged_updates.RowStatusID or  s.DateCreated!= staged_updates.DateCreated or  s.CreatedByUserID!= staged_updates.CreatedByUserID or  s.DateUpdated!= staged_updates.DateUpdated or  s.UpdatedByUserID!= staged_updates.UpdatedByUserID) THEN INSERT (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date) 
#        VALUES (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date)

# COMMAND ----------

# columns_check_list_full= check_column_from_df_full_merge (df_sc)

# COMMAND ----------

# %sql 
# SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc where mat_svc_equipmentcard_sc.iscurrent = true
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true order by EquipmentCardID

# COMMAND ----------


# %sql

# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
    
# ) staged_updates    
#       on s.EquipmentCardID = staged_updates.mergeKey and  s.EquipmentCardID= staged_updates.EquipmentCardID and  s.SoldToBusinessPartnerID= staged_updates.SoldToBusinessPartnerID and  s.HolderBusinessPartnerID= staged_updates.HolderBusinessPartnerID and  s.ItemID= staged_updates.ItemID and  s.SerialIdentifier= staged_updates.SerialIdentifier and  s.Notes= staged_updates.Notes and  s.RowStatusID= staged_updates.RowStatusID and  s.DateCreated= staged_updates.DateCreated and  s.CreatedByUserID= staged_updates.CreatedByUserID and  s.DateUpdated= staged_updates.DateUpdated and  s.UpdatedByUserID= staged_updates.UpdatedByUserID and  s.load_time= staged_updates.load_time and  s.iscurrent= staged_updates.iscurrent and  s.start_date= staged_updates.start_date and  s.end_date= staged_updates.end_date
#       WHEN MATCHED and s.iscurrent = true 
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#        WHEN NOT MATCHED  THEN INSERT (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date) 
#        VALUES (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date)
       

# COMMAND ----------

# %sql
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on  s.EquipmentCardID= staged_updates.EquipmentCardID and  s.SoldToBusinessPartnerID= staged_updates.SoldToBusinessPartnerID and  s.HolderBusinessPartnerID= staged_updates.HolderBusinessPartnerID and  s.ItemID= staged_updates.ItemID and  s.SerialIdentifier= staged_updates.SerialIdentifier and  s.Notes= staged_updates.Notes and  s.RowStatusID= staged_updates.RowStatusID and  s.DateCreated= staged_updates.DateCreated and  s.CreatedByUserID= staged_updates.CreatedByUserID and  s.DateUpdated= staged_updates.DateUpdated and  s.UpdatedByUserID= staged_updates.UpdatedByUserID and  s.load_time= staged_updates.load_time and  s.iscurrent= staged_updates.iscurrent and  s.start_date= staged_updates.start_date and  s.end_date= staged_updates.end_date
#       WHEN MATCHED and s.iscurrent = true 
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
#        WHEN NOT MATCHED  THEN INSERT (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date) 
#        VALUES (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date)


# COMMAND ----------

# %sql
# MERGE INTO lab_silver.mat_svc_equipmentcard_sc as s 
#       USING ( SELECT  temp_table_sc.EquipmentCardID as mergeKey, temp_table_sc.*
#   FROM temp_table_sc
#   UNION ALL
#   SELECT NULL as mergeKey, temp_table_sc.*
#   FROM temp_table_sc JOIN lab_silver.mat_svc_equipmentcard_sc
#   ON temp_table_sc.EquipmentCardID =  mat_svc_equipmentcard_sc.EquipmentCardID 
#   WHERE mat_svc_equipmentcard_sc.iscurrent = true  
# ) staged_updates    
#       on 
#       --s.EquipmentCardID = staged_updates.mergeKey and
#        s.EquipmentCardID= staged_updates.EquipmentCardID and  s.SoldToBusinessPartnerID= staged_updates.SoldToBusinessPartnerID and  s.HolderBusinessPartnerID= staged_updates.HolderBusinessPartnerID and  s.ItemID= staged_updates.ItemID and  s.SerialIdentifier= staged_updates.SerialIdentifier and  s.Notes= staged_updates.Notes and  s.RowStatusID= staged_updates.RowStatusID and  s.DateCreated= staged_updates.DateCreated and  s.CreatedByUserID= staged_updates.CreatedByUserID and  s.DateUpdated= staged_updates.DateUpdated and  s.UpdatedByUserID= staged_updates.UpdatedByUserID and  s.load_time= staged_updates.load_time and  s.iscurrent= staged_updates.iscurrent and  s.start_date= staged_updates.start_date and  s.end_date= staged_updates.end_date
#       WHEN Not MATCHED and s.iscurrent = true 
#       THEN 
#        UPDATE SET iscurrent = false, end_date = staged_updates.start_date
# -------^^^
#       WHEN NOT MATCHED  THEN INSERT (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date) 
#        VALUES (EquipmentCardID,SoldToBusinessPartnerID,HolderBusinessPartnerID,ItemID,SerialIdentifier,Notes,RowStatusID,DateCreated,CreatedByUserID,DateUpdated,UpdatedByUserID,load_time,iscurrent,start_date,end_date)

# COMMAND ----------

# %sql 
# select count (*), EquipmentCardID from lab_silver.mat_svc_equipmentcard_sc 
# group by EquipmentCardID
# order by 1 desc
# limit 10 ;



# COMMAND ----------

# %sql 
# --update SET iscurrent = false, end_date = max  where EquipmentCardID =  and load_time = min
# select  concat('update lab_silver.mat_svc_equipmentcard_sc SET iscurrent = false, end_date = ','\'', max_load ,'\'', '  where EquipmentCardID = ', EquipmentCardID, '  and load_time = ','\'', min_load,'\'',  ';' ) from (
# select min(load_time) as min_load, max(load_time)   as max_load, EquipmentCardID  from lab_silver.mat_svc_equipmentcard_sc  where EquipmentCardID in (
# select  EquipmentCardID from lab_silver.mat_svc_equipmentcard_sc where iscurrent = true
# group by EquipmentCardID
# having count (*) >1)
# group by EquipmentCardID)


# COMMAND ----------

# %sql
# update lab_silver.mat_svc_equipmentcard_sc SET iscurrent = false, end_date = '2021-07-11 15:04:23.228391' where EquipmentCardID = 128354 and load_time = '2021-07-11 14:59:02.765195';


# COMMAND ----------

# %sql 
# select * from lab_silver.mat_svc_equipmentcard_sc  where EquipmentCardID= 128354

# COMMAND ----------

# %sql 
# select * from lab_silver.mat_svc_equipmentcard_sc  where EquipmentCardID in ( 5 , 4,3) 
# order by EquipmentCardID
# limit 10 ;



# COMMAND ----------

# %sql

# select load_time , count(1)
# from lab_silver.mat_case_scaninfo
# group by 1
# order by 1 desc

# COMMAND ----------


