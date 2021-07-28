# Databricks notebook source
# MAGIC %md
# MAGIC ## from bronze.jigs  --> silver  table:<br />
# MAGIC 
# MAGIC takes all jisg table from bronze level that arrive with json as string  --> semi parsed table :<br />
# MAGIC 1.mapping of all existing tables in bronze layer <br />
# MAGIC 2.read all relevant increment data using structured streaming <br />
# MAGIC 3.split data into 2 tables: <br />
# MAGIC &nbsp; &nbsp; &nbsp; &nbsp;a.write valid data to silver table <br />
# MAGIC &nbsp; &nbsp; &nbsp; &nbsp;b.valid corrupt data to quarnatine table

# COMMAND ----------

# MAGIC %run ./include/utilities

# COMMAND ----------

# DBTITLE 1,local parameters
data_source = 'jigs'
env_source = 'lab'
datalayer_source = 'bronze'
datalayer_target = 'silver'
process_name = "{}_to_silver".format(data_source)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## read data from bronze tables  and write to silver table
# MAGIC by using structured streaming in permmisive mode

# COMMAND ----------

table_list_source  = get_table_list(data_source,env_source,datalayer_source)
print(table_list_source)
table_list_target = get_table_list(data_source,env_source,datalayer_target)
print(table_list_target)



# COMMAND ----------

# for table in table_list_target:
#   spark.sql('truncate table {}'.format(table))

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, IntegerType, DateType, StringType, DecimalType , BooleanType , StructField , LongType ,TimestampType
from pyspark.sql.functions import lower, col

## use structured streaming with foreachbatch
def processMicrobatch(df,epoch_id):
  #schema derived from target table
  schema_table = table.replace(datalayer_source,datalayer_target)
  schema = spark.read.table(schema_table).schema
  #read source table and adjust the scehma
  parsed_df = df.withColumn("nested_json", from_json("value", schema)) .\
                    withColumn("analysis", col("nested_json.analysis")) .\
                    withColumn("header", col("nested_json.header")) .\
                    withColumn("result", col("nested_json.result")) .\
                    drop("nested_json").drop("value")
#   write the data to the predefined table
  valid = parsed_df.where("analysis is not null  and  header is not null and  result is not null")
  valid.write.mode("append").format('delta').saveAsTable(schema_table)
  quarntine = parsed_df.where("analysis is null or header is null or result is null")
  quarntine.write.mode("append").format('delta').saveAsTable(schema_table+"_quarantine")

  
def processMicrobatch_1(df,epoch_id):
  #schema derived from target table
  schema_table = table.replace(datalayer_source,datalayer_target)
  schema = spark.read.table(schema_table).schema
  #build the rdd with mapping ptrocess of  json value
  rdd = df.rdd.map(lambda x: x.asDict()["value"])
  test_df = spark.read.option('multiLine','true').option('columnNameOfCorruptRecord','_corrupted').schema(schema).json(rdd)
  #write data to 2 seperate tables
  quarntine = test_df.where("_corrupted is not null")
  valid = test_df.where("_corrupted is  null")
  valid.write.mode("append").format('delta').saveAsTable(schema_table)
  quarntine.write.mode("append").format('delta').saveAsTable(schema_table+"_quarantine")
#   quarntine.write.format("delta").save(path) #complete all neccesarry for run
#   valid.write.option("checkpointLocation",'s3://jarvis-databricks-db-bronze-use1-lab/lab_bronze.db/checkpoint/hanan/').Table('lab_bronze.hanan')
#   valid.write.mode("overwrite").saveAsTable('lab_silver.hanan')
  

  
  
# withColumn("_corrupted" , expr("IF analysis is null or header is null or result is null then 1 else 0")) .\
     




# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from lab_silver.jigs_qc_incoming_mes

# COMMAND ----------

# table_list_source = ['lab_bronze.jigs_qc_incoming_mes']
table_list_source = ['lab_bronze.jigs_Tip_Tester_mes']

for table in table_list_source:
  table_name = table[table.find('.')+1:len(table)]
  print(table_name)
  checkpoint_path = 's3://jarvis-databricks-db-{datalayer_target}-use1-lab/lab_{datalayer_target}.db/checkpoint/{table_name}'. \
           format(datalayer_target=datalayer_target,table_name=table_name)
  print(checkpoint_path)
  try:
    df =   spark.readStream.\
          format('cloudFiles').\
          table(table)
    
#     df.display()
    
    df.writeStream.foreachBatch(processMicrobatch_1).\
      trigger(once=True).\
      option('checkpointLocation',checkpoint_path).\
      start().\
      awaitTermination()
    
  except Exception as err:
      print(err)
  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # update process

# COMMAND ----------

try:
  spark.sql("select process_name from  {}_config.processUpdate where process_name = '{}' ".format(env_source,process_name)).collect()[0][0]
  spark.sql("UPDATE {}_config.processUpdate SET last_run_date = current_timestamp() WHERE process_name = '{}' ".format(env_source,process_name))  
except Exception as err:
  print("process {} doesnt exist in {}_config.processUpdate".format(process_name,env_source))
  logging.warning("insert new process: {} into {}_config.processUpdate".format(process_name,env_source))
  spark.sql("INSERT INTO  {}_config.processUpdate VALUES ('{}',\'')".format(env_source,process_name))
  spark.sql("UPDATE {}_config.processUpdate SET last_run_date = current_timestamp() WHERE process_name = '{}' ".format(dest_env,process_name))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #end of process

# COMMAND ----------


