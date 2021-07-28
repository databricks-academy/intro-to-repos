# Databricks notebook source
# MAGIC %md
# MAGIC ####silver.jigs  --> gold.jigs:<br />
# MAGIC 
# MAGIC Processing all jigs table from siver layer (struct format) and explode them into gold layer

# COMMAND ----------

# MAGIC %run ./include/jigs_silver_to_gold_utils

# COMMAND ----------

#get a list of all jigs relevant (not corrupted) silver tables
jigs_silver_tables_list=get_table_list('jigs','lab','silver')


# COMMAND ----------

for i in jigs_silver_tables_list:
  print (i)

# COMMAND ----------

#loop over the silver tables list and send each at a time to 'jig_silver_to_gold' function that returns a flatten df , 
#the next step, the flatten df is sent to 'write_df_to_gold' function that writes the df into a gold table using streaming functionality. 
for i in jigs_silver_tables_list:
  print(i)    
  print(i[11:])
  jig_df=jig_silver_to_gold(i)
  write_df_to_gold(jig_df,i[11:])


# COMMAND ----------


