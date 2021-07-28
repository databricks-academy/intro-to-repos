# Databricks notebook source
# dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC <h1>Add new table to DB process</h1>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC the purpose of this notebook is to easily add new tables to our DB process with minimum (as possible) mistakes <br />
# MAGIC by the following steps: <br />
# MAGIC 1.define the table schema (relevant for SFDC only)<br />
# MAGIC 2.define the required params that will be a part of the prccess <br />
# MAGIC 3.overview the added table before moving the data to the lab_config <br />
# MAGIC 4.approve and move the data

# COMMAND ----------

dbutils.widgets.combobox("env", 'lab', ['lab', 'master'])
dbutils.widgets.combobox("table_source", 'sfdc', ['sfdc','mat','mes'])
dbutils.widgets.combobox("delete_from_config",'False', ['False','True'])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## define the table schema (relevant for SFDC only)
# MAGIC 
# MAGIC table_fields example = 'id int,createdate timestamp'

# COMMAND ----------

env = getArgument("env")
table_source = getArgument("table_source")


#### defined by user ####

table_fields = 'Id string,Child_Account__c string,child_parentid__c string,Parent_Account__c string,CreatedDate timestamp,LastModifiedDate timestamp,child_Account_Status__c string,Child_Relationship__c string,IsDeleted string,Status__c string,Parent_Type__c string,Parent_Relationship__c string' 
table_name = 'Account_Relationship__c'




# COMMAND ----------

# MAGIC %sql
# MAGIC drop table   lab_bronze.sfdc_Account_Relationship__c

# COMMAND ----------

if table_source == 'sfdc':
  spark.sql('create table if not exists {}_bronze.{}_{} ({},load_time timestamp) using delta'.format(env,table_source,table_name,table_fields))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## user defined lab_config (relevant for all process)

# COMMAND ----------

source_name = getArgument("table_source")
is_pii  = 0
primary_key  = 'id'
extraction_interval_minutes = 10
owner_name  = 'sdfc_api'
source_table_name  = table_name.lower()
get_method  = 'i'
date_column1_name  = 'LastModifiedDate'
date_column2_name  = 'CreatedDate'
isactive  = 1
query_text  = " select Id,Child_Account__c,child_parentid__c,Parent_Account__c,CreatedDate,LastModifiedDate,child_Account_Status__c,Child_Relationship__c,IsDeleted,Status__c,Parent_Type__c,Parent_Relationship__c from Account_Relationship__c where (LastModifiedDate >{0} and LastModifiedDate <={1})"
first_query_text  = "select Id,Child_Account__c,child_parentid__c,Parent_Account__c,CreatedDate,LastModifiedDate,child_Account_Status__c,Child_Relationship__c,IsDeleted,Status__c,Parent_Type__c,Parent_Relationship__c from Account_Relationship__c "
updated_by  = 'ramiga'
start_time  = 'current_timestamp()'
last_updated_time = 'current_timestamp()'



# COMMAND ----------

sql = "insert into {}_config.dbtables VALUES ('{}',{},'{}',{},'{}','{}','{}','{}','{}',{},'{}','{}','{}',{},{})".format(env,source_name,is_pii,primary_key,extraction_interval_minutes,owner_name,source_table_name,get_method,date_column1_name,date_column2_name,isactive,query_text,first_query_text,updated_by,start_time,last_updated_time)

print(sql)

spark.sql(sql)

spark.sql("select * from {}_config.dbtables where source_name = '{}' and source_table_name = '{}'".format(env,source_name,source_table_name)).createOrReplaceTempView('new_column_add')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC insert into lab_config.dbtables 
# MAGIC VALUES ('sfdc',0,'id',10,'sdfc_api','test','i','LastModifiedDate','CreatedDate',0,'select ,id,AccountId,ContactId,CreatedDate,LastModifiedDate,IsActive,IsDeleted,IsDirect,StartDate,EndDate,Roles from accountcontactrelation where (LastModifiedDate >{0} and LastModifiedDate <={1})  ',"select ,id,AccountId,ContactId,CreatedDate,LastModifiedDate,IsActive,IsDeleted,IsDirect,StartDate,EndDate,Roles from accountcontactrelation where RecordTypeId in ('0120H000001J6QaQAK', '0120H000000u011QA')",'ramiga',current_timestamp(),current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lab_config.dbtables where source_name = 'sfdc' and source_table_name = 'test'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## new line added  - reflected in DB

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from new_column_add
# MAGIC 
# MAGIC -- select * from lab_config.dbtables where source_name = 'sfdc' and source_table_name = 'test'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## delete from config table (in needed)

# COMMAND ----------

delete_from_config = getArgument("delete_from_config")

if  delete_from_config == 'True':
  spark.sql("delete from {}_config.dbtables where source_name = '{}' and source_table_name = '{}'".format(env,source_name,source_table_name))
else:
  pass

spark.sql("select * from {}_config.dbtables where source_name = '{}' and source_table_name = '{}'".format(env,source_name,source_table_name)).createOrReplaceTempView('new_column_add')


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- select * from lab_config.dbtables_manage where source_name = 'sfdc'
# MAGIC 
# MAGIC 
# MAGIC -- UPDATE lab_config.dbtables 
# MAGIC -- SET  query_text = "select id,CreatedDate ,LastModifiedDate,name ,SobjectType,IsActive from RecordType where (LastModifiedDate >{0} and LastModifiedDate<={0}) or (CreatedDate >={1} and CreatedDate<={1})"
# MAGIC -- where source_name = 'sfdc'
# MAGIC -- and source_table_name = 'recordtype'

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from lab_config.dbtables where source_name = 'sfdc' and source_table_name = 'account_relationship__c'

# COMMAND ----------

# MAGIC  %sql
# MAGIC   select * from lab_config.dbtables where source_name = 'sfdc'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lab_gold.profiledata

# COMMAND ----------


