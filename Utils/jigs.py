# Databricks notebook source
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


