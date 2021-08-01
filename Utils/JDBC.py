# Databricks notebook source
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


