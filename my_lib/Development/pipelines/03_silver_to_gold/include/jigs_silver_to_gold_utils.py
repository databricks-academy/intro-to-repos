# Databricks notebook source
import pyspark.sql.functions as F  # By Hanan Segal

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.legacy.timeParserPolicy = LEGACY 

# COMMAND ----------

#by Hanan Segal
def get_table_list(data_source,env_source,database_source):
  table_list=[]
  tbl_df = spark.sql("""SHOW TABLES IN {}_{}""".format(env_source,database_source))
  tbl_df.registerTempTable("tables_in_db")
  table_list = spark.sql("select concat(database,'.',tableName) as fulltablename from tables_in_db where tableName like '{}%' and tableName not like '%quarantine' and isTemporary = false".format(data_source))
  full_table_list = table_list.select('fulltablename').collect()
  table_list = [row.fulltablename for row in full_table_list]
  return table_list

# COMMAND ----------

####ADD COMMENT
def load_df_from_silver(jig_table_name):
  orig_df = (spark.readStream.\
  format('cloudFiles').\
  table("lab_silver."+ jig_table_name)
            )
  return orig_df

# COMMAND ----------

####Add COMMENT
def write_df_to_gold(explode_df,jig_table_name):
  explode_df.writeStream.\
  format("delta").\
  outputMode("append").\
  trigger(once=True).\
  option("checkpointLocation", "s3://jarvis-databricks-db-gold-use1-lab/lab_gold.db/checkpoint/"+jig_table_name).\
  table("lab_gold."+jig_table_name).\
  awaitTermination()
  return 1




# COMMAND ----------

## flat two layers - solution for now that handles another nesting layer (2 instead of 1 ) which should be fine for this situation, will add a more dynamic solution later on
## orig from here (# By Hanan Segal): 
## https://aligntech-jarvis-dv1-e2.cloud.databricks.com/?o=7351116569006252#notebook/1972776597226092/command/1972776597226095

def flatten_df2(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']


    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    flat_cols_1 = [c[0] for c in flat_df.dtypes if c[1][:6] != 'struct']
    nested_cols_1 = [c[0] for c in flat_df.dtypes if c[1][:6] == 'struct']
    
    flat_df_1 = flat_df.select(flat_cols_1 +
                           [F.col(nc+'.'+c).alias(nc+'_'+c)
                            for nc in nested_cols_1 
                            for c in flat_df.select(nc+'.*').columns])
    return flat_df_1

# COMMAND ----------

# expload 'analysis' col:  ###by Omri Afgin
# output- MULTI row per file (num of row per file = num of 'analysis.id' checks)
def explode_analysis(df, cols_to_groupby, analysis_cols):
  output_df = (df.select(cols_to_groupby + analysis_cols)
                        .withColumn('analysis', F.explode('analysis'))
                        .withColumn('analysis_id', F.col('analysis.id'))
                        .withColumn('analysis_max', F.col('analysis.max'))
                        .withColumn('analysis_measured', F.col('analysis.measured'))
                        .withColumn('analysis_min', F.col('analysis.min'))
                        .withColumn('analysis_repetitions', F.col('analysis.repetitions'))
                        .withColumn('analysis_status', F.col('analysis.status'))
                        .drop('analysis')
                       )
  return output_df

# COMMAND ----------

# jig_mes_to_silver
# All 'mes.json' file have the same structure.
# This function is a baseline function to flat and re-arrenge the files as table with generic schema

def jig_silver_to_gold(jig_name):
 
  orig_df = load_df_from_silver(jig_name)
  
  #flat_df = flat the nested json columns within "orig_df"
  flat_df=flatten_df2(orig_df)

    # change columns order
  cols = list(flat_df.columns)[::-1]
  flat_df = flat_df.select(cols)
  
  # add local + utc timestamp
  orig_time_col = 'header_timestamp'
  flat_df= (flat_df.withColumn("timestamp", F.to_timestamp(F.col(orig_time_col), "yyyy-MM-dd'T'HH-mm-ss"))
            .withColumn("timestamp_UTC", F.to_timestamp(F.col(orig_time_col), "yyyy-MM-dd'T'HH-mm-ssX"))
      )
  
  
  # set analysis_col expantion: each "id" as column. other messures as cell values.
  analysis_cols = [F.col('analysis.id').alias('analysis_id'), 'analysis']
  key_cols = list(flat_df.columns)
  key_cols.remove('analysis')
  key_cols.remove('_corrupted')
  
  # expload 'analysis' col
  explode_df = explode_analysis(flat_df, key_cols, analysis_cols)
  
  
  
  return explode_df

# COMMAND ----------


