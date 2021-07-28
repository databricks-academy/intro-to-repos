# Databricks notebook source
File_path = 's3://jarvis-elasticdump-us-dv1/elasticdump/' # move to config

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read bronze delta into dataframe

# COMMAND ----------

dfBronze= spark.sql(select * from table_name where date = "get last date") # using SQL
dfBronze= spark.read.table("tabke name").filter(" date = 'last'") # using Spark (can replace filter with where)

dfBronze.printSchema()


# COMMAND ----------

#dfBronze.show()
dfBronze.count()

# COMMAND ----------

from pyspark.sql.functions import from_json


json_schema = ''' 


'''

dfBronzeNest=dfBronze.withcoulmn("coulmn_name", from_json("message",json_schema)) # parse from string since message is string

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - flat table

# COMMAND ----------

dfBronzeFlat=flatten_df(dfBronze) # call function from utilities
dfBronzeFlat.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add version to each log

# COMMAND ----------

# call function from utilities
get


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create Silver delta files and fact table, should consider to build dimention tables as well

# COMMAND ----------

(flat_df.write
 .mode("overwrite")
 .format("delta") 
 .partitionBy("????") 
 .save(File_path))

# COMMAND ----------

spark.sql("CREATE TABLE Silver.logs USING DELTA LOCATION 'file_location'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update bronze table back - the rows that are in Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## QA Analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from sandboxM.flogs

# COMMAND ----------

# MAGIC %sql
# MAGIC ---select count (distinct agent.hostname) from sandboxM.oneLog 
# MAGIC select count (distinct log_file) from sandboxM.flogs

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (distinct agent_hostname) from sandboxM.flogs;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (distinct agent_hostname, log_file) from sandboxM.flogs;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (*) 
# MAGIC from sandboxM.flogs 
# MAGIC where processed_message like "%ersion:%"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sandboxM.flogs limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select log_file, count (distinct agent_hostname) co from sandboxM.flogs group by log_file order by co desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select application,  count (distinct log_file) co from sandboxM.flogs group by application order by co desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select application, count(*) as co from sandboxM.flogs group by application order by co desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select  processed_LineNumber, log_file, processed_message from sandboxM.flogs where   --- application = "Scanner", RX from the file name
# MAGIC   processed_message like "%ersion:%" order by processed_LineNumber

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from sandboxM.flogs 
# MAGIC where application = "CaseAdmin"
# MAGIC and processed_message like "%Case Admin Version:%"
# MAGIC order by processed_message
