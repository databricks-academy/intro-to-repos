# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### notebook flow:  
# MAGIC 1.approach SFDC API <br />
# MAGIC 2.create temp tables from the data extracted <br />
# MAGIC 3.query the extracted data with simple sql functions

# COMMAND ----------

sfdc_username = dbutils.secrets.get(scope = "sfdc", key = "username")
sfdc_password = dbutils.secrets.get(scope = "sfdc", key = "password")
sfdc_client_id = dbutils.secrets.get(scope = "sfdc", key = "client_id")
sfdc_client_secret = dbutils.secrets.get(scope = "sfdc", key = "client_secret")


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # API approach - run cell

# COMMAND ----------

import requests
import json
import pandas as pd




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




#  Retrieve access token and instance URL from respone
json_res = response.json()
access_token = json_res['access_token']
auth = {'Authorization':'Bearer ' + access_token}
instance_url = json_res['instance_url']

#query approach from sfdc API
action = '/services/data/v45.0/query/' # no need to change

#  GET requests
url = instance_url + '/services/data/v45.0/query/01g0H0000BPopBCQQZ-2000'
res = requests.get(url, headers=auth)
r = res.json()


# nextRecordsUrl


# url = instance_url + '/services/data/v45.0/query/01g0H0000BPopBCQQZ-2000'
# res = requests.get(url, headers=auth)
# r = res.json()
# print(r)

url = instance_url + '/services/data/v45.0/sobjects/opportunityfieldhistory/describe'
url = instance_url + '/services/data/v45.0/sobjects/Lead/listviews' 
url = instance_url + '/services/data/v52.0/sobjects' 
res = requests.get(url, headers=auth)
r = res.json()
print(r)


# /services/data/v48.0/tooltip/query/?q=

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # python functions that supports the process - run cell

# COMMAND ----------

#extracts the data from the API
def sfdc_call(action,parameters={},method='get',data={}):
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
  f.createOrReplaceTempView(table_name)
  return f

  
def create_df_sfdc(action,query_SOQL):
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
  return f




# COMMAND ----------

query_SOQL = '''select Apttus_Proposal__Account__c,Apttus_Proposal__Approval_Stage__c,Apttus_Proposal__Opportunity__c,Apttus_Proposal__Payment_Term__c,Apttus_Proposal__Presented_Date__c,Apttus_Proposal__Primary__c,Apttus_Proposal__Primary_Contact__c,Apttus_Proposal__Proposal_Approval_Date__c,Apttus_Proposal__Proposal_Name__c,Apttus_QPConfig__BillToAccountId__c,Apttus_QPConfig__ShipToAccountId__c,CreatedById,CreatedDate,Funding_Source__c,Id,LastActivityDate,LastModifiedDate,Name,RecordTypeId,Service_Fees_PAV__c,SystemModstamp from Apttus_Proposal_Proposal_c  limit 10'''


action = '/services/data/v45.0/query/'


action = '/services/data/v45.0/tooltip/query/?q='
dataextract = sfdc_call(action,{'q':query_SOQL})

print(dataextract)

for x in dataextract:
  print(type(x))




# print(dataextract)

# dataextract.append(next)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # user defined parameters for extarcting the data from the API

# COMMAND ----------


# change sql statment and table name as desired to create temp table

# query_SOQL = 'select id ,OwnerId ,RecordTypeId, CreatedDate , LastModifiedDate, Account_Number__c , Name, ShippingCountryCode ,Type , Account_Status__c, Account_Status__c , Line_of_Business__c , Account_Sub_Type__c, ShippingPostalCode , ShippingLatitude, ShippingLongitude from Account limit 10'

#query_SOQL = '''select id ,OwnerId ,RecordTypeId,CreatedDate,LastModifiedDate,Account_Number__c,Name,ShippingCountryCode,Type
#, Account_Status__c,Line_of_Business__c ,Account_Sub_Type__c,ShippingPostalCode,ShippingLatitude,ShippingLongitude,MAT_ID__C 
#,ShippingCity,ShippingState,ShippingStateCode from account where (CreatedDate >=2021-06-27T00:00:00.000+0000 and CreatedDate<2021-06-28T00:00:00.000+0000)  

# query_SOQL = '''select id ,OwnerId ,RecordTypeId,CreatedDate,LastModifiedDate,Account_Number__c,Name,ShippingCountryCode,Type
# #, Account_Status__c,Line_of_Business__c ,Account_Sub_Type__c,ShippingPostalCode,ShippingLatitude,ShippingLongitude,MAT_ID__C 
# #,ShippingCity,ShippingState,ShippingStateCode from account where (CreatedDate >=2021-06-27T00:00:00.000+0000 and CreatedDate<2021-06-28T00:00:00.000+0000)  '''
# table_name = 'ramiga_account'

query_SOQL = '''select Id,AccountId,ContactId,CreatedDate,LastModifiedDate,IsActive,IsDeleted,IsDirect,StartDate,EndDate,Roles
 from AccountContactRelation  where (CreatedDate >= 2018-01-01T00:00:00.000+0000  ) '''


table_name = 'AccountContactRelation_testing'


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### extract data from sfdc and create temp table

# COMMAND ----------

create_tmp_table(action,query_SOQL,table_name)
tmp_tables_list()



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### query data

# COMMAND ----------

spark.sql('select * from AccountContactRelation_testing').printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #create df from soql

# COMMAND ----------

# DBTITLE 1,create df from soql
df = create_df_sfdc(action,query_SOQL)



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # create table in lab_datateam from dataframe

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("lab_datateam.{}".format(table_name))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # documnet queries and table name that were executed
# MAGIC 
# MAGIC table name / sql query

# COMMAND ----------

# DBTITLE 1,Account - rotem_sfdc_account
select 
id ,OwnerId ,RecordTypeId 
,CreatedDate ,LastModifiedDate 
,Account_Number__c ,Name ,ShippingCountryCode ,Type ,Account_Status__c , Line_of_Business__c , Account_Sub_Type__c ,ShippingPostalCode, ShippingLatitude, ShippingLongitude 
, MAT_ID__C , ShippingCity , ShippingState , ShippingStateCode 
from Account 

# COMMAND ----------

# DBTITLE 1,Contact - rotem_sfdc_contact
select 
id, accountid
,CreatedDate, LastModifiedDate
, Contact_ID__c, Clinician_ID__c
, Name, Professional_Category__c, Certification_Date__c
,Contact_Status__c, Doctor_Segment__c , RecordTypeId
from Contact

# COMMAND ----------

# DBTITLE 1,RecordType - rotem_sfdc_recordtype
select
Rt.id
,Rt.[CreatedDate] ,Rt.LastModifiedDate
, Rt.name ,Rt.SobjectType,Rt.IsActive
 from RecordType Rt

# COMMAND ----------

# DBTITLE 1,AccountContactRelation - rotem_sfdc_accountcontactrelation
select
id, AccountId, ContactId
, CreatedDate  , LastModifiedDate
, IsActive, IsDeleted, IsDirect, StartDate, EndDate, Roles
 from AccountContactRelation

# COMMAND ----------

# DBTITLE 1,Zip_Code_Territory__c  - rotem_sfdc_zip_code_territory__c
select
id, OwnerId, GM_ID__c, TM_ID__c, CreatedDate , LastModifiedDate, Zip_Code__c , Friendly_Name__c, isdeleted, Itero_Territory_Code__c, Territory_Code__c, SA_Territory__c
from Zip_Code_Territory__c 

# COMMAND ----------

# DBTITLE 1,Account_Relationship__c - rotem_account_relationship__c
select id, Child_Account__c, child_parentid__c
,Parent_Account__c, CreatedDate, LastModifiedDate
,child_Account_Status__c, Child_Relationship__c, IsDeleted, Status__c
,Parent_Type__c, Parent_Relationship__c
from Account_Relationship__c 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## hanan testing

# COMMAND ----------

print(r)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select * from test

# COMMAND ----------


query_SOQL = '''Select COUNT() from OpportunityFieldHistory where OpportunityId in (select id from opportunity where RecordTypeId in ('0120H000001J6QaQAK','0120H000000u011QAA','0120H000001QTSDQA4','0120H000001QT9eQAG','0120H000000yUddQAE','012i00000019r6NAAQ')) and CreatedDate >2021-01-01T08:23:48.000+0000'''


query_SOQL = '''Select Id,CreatedById,CreatedDate,Field,NewValue,OldValue,OpportunityId from OpportunityFieldHistory where OpportunityId in (select id from opportunity where RecordTypeId in ('0120H000001J6QaQAK','0120H000000u011QAA','0120H000001QTSDQA4','0120H000001QT9eQAG','0120H000000yUddQAE','012i00000019r6NAAQ')) limit 1000'''


query_SOQL = '''Select Id,CreatedById,CreatedDate,Field,NewValue,OldValue,OpportunityId from OpportunityFieldHistory where OpportunityId in (select id from opportunity where RecordTypeId in ('0120H000001J6QaQAK','0120H000000u011QAA','0120H000001QTSDQA4','0120H000001QT9eQAG','0120H000000yUddQAE','012i00000019r6NAAQ'))  limit 1000'''

# query_SOQL = 'select COUNT() from apttus_proposal_c'

dataextract = sfdc_call(action,{'q':query_SOQL})

df = pd.DataFrame(dataextract['records']).drop(columns='attributes')

print (dataextract)

# f = spark.createDataFrame(df)

# df.info()



# f.printSchema()
# print (dataextract)


# sf_df = pd.DataFrame(dataextract['records']).drop(columns='attributes')

# table_name = 'test'


# create_tmp_table(action,query_SOQL,table_name)



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from test

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lab_config.dbtables where isactive =  0

# COMMAND ----------


