# Databricks notebook source
jdbcUsername = dbutils.secrets.get(scope = "matdb", key = "username")
jdbcPassword = dbutils.secrets.get(scope = "matdb", key = "password")
jdbcHostname = "itero-matdb-replica-prdctn.ccmooump32oy.us-west-2.rds.amazonaws.com"
jdbcDatabase = "MyCadent"
jdbcPort = 1433
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

df = spark.read.jdbc(url=jdbcUrl, table="Case_ExtendedInfo", properties=connectionProperties)
display(df)

# COMMAND ----------

pushdown_query = "(select name from sys.tables where is_ms_shipped=0) tbls"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties).orderBy("name")
display(df)

# COMMAND ----------

pushdown_query = "(select top 10 * from Case_ExtendedInfo) cases"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

sql_query = "(select top 1000000 *  from Case_ExtendedInfo) alias"
df_2 = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=connectionProperties)
display(df_2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_2.printSchema()

# COMMAND ----------


