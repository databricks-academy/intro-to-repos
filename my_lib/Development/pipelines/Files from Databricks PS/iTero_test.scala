// Databricks notebook source
// MAGIC %md ###Example of foreachBatch
// MAGIC 
// MAGIC Here I am providing an example to the use of foreachBatch in python.

// COMMAND ----------

display(spark.readStream.format("rate").load())

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC 
// MAGIC #read the data with structured streaming
// MAGIC df = spark.readStream.format("rate").load()
// MAGIC #Any streaming transformation can be added here
// MAGIC 
// MAGIC def processMicrobatch(df, id):
// MAGIC   #microbatch processing logic
// MAGIC   transformed_df = df.filter(col("value") >=10)
// MAGIC   
// MAGIC   #write microbatch to final location
// MAGIC   transformed_df.write.format("noop").mode("overwrite").save()
// MAGIC 
// MAGIC df.writeStream.foreachBatch(processMicrobatch).start()

// COMMAND ----------

// MAGIC %md ###Loading Data from Bronze Layer
// MAGIC 
// MAGIC Here I am simulating a load of unparsed json data from a bronze layer. Ideally, you would have obtained a dataframe with a single json string from your spark.read.format("delta"), in my case I am creating a dataframe from two json samples.

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col
// MAGIC 
// MAGIC json_sample_good = '{"status": "publish", "description": null, "creator": "jules_damji", "link": "https://databricks.com/blog/2017/08/09/apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks.html", "authors": null, "id": 11820, "categories": ["Company Blog", "Product"], "dates": {"publishedOn": "2017-08-09", "tz": "UTC", "createdOn": "2017-08-09"}, "title": "Apache Spark\u2019s Structured Streaming with Amazon Kinesis on Databricks", "slug": "apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks"}'
// MAGIC 
// MAGIC #corrupted record: ID is not a Long
// MAGIC json_sample_bad = '{"status": "publish", "description": null, "creator": "jules_damji", "link": "https://databricks.com/blog/2017/08/09/apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks.html", "authors": null, "id": "this_is_not_an_ID", "categories": ["Company Blog", "Product"], "dates": {"publishedOn": "2017-08-09", "tz": "UTC", "createdOn": "2017-08-09"}, "title": "Apache Spark\u2019s Structured Streaming with Amazon Kinesis on Databricks", "slug": "apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks"}'
// MAGIC 
// MAGIC json_df = spark.createDataFrame(data=[(json_sample_good,1), (json_sample_bad,2)], schema=["value", "nothing"]).drop(col("nothing"))

// COMMAND ----------

// MAGIC %md ###Parsing the json
// MAGIC 
// MAGIC In the next example, I am creating an RDD of strings from the dataframe. There are a few things to pay attention to:
// MAGIC * the schema contains the \_corrupted column for the corrupted records
// MAGIC * in the schema, I am enforcing a data type Long for ids. This will make the second row invalid
// MAGIC * notice how only id column is set to null in the second row, but \_corrupted contains now the whole json row. Ideally, this row should be not persisted in the silver layer, instead should be either quarantined or follow a secondary process to assess the issue with the ID.
// MAGIC 
// MAGIC On the last point, please be aware that this behaviour is only relevant to DBR7.0+. Older DBRs would have set every column to null (except for \_corrupted), regardless if the issue was with only a single field data type. Please make sure all your jobs are running on DBR7.3+ and migrate any existing job that uses an older version as older DBRs are either at their end of life, or will be approching it very soon.

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC # json_schema = "authors String, categories String, content String, creator String, dates String, description String, id Long, link String, slug String, status String, title String, _corrupted String"
// MAGIC 
// MAGIC json_schema = "authors String, categories String, creator String, dates String, description String, id Long, link String, slug String, status String, title String, _corrupted String"
// MAGIC 
// MAGIC parsed_df = spark.read.json(json_df.rdd, schema=json_schema, mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupted")
// MAGIC 
// MAGIC parsed_df.display()

// COMMAND ----------

// MAGIC %md ###Final Remarks
// MAGIC 
// MAGIC This approach to parse json is suitable only for json files that do not contain new lines as part of a single field. 
// MAGIC 
// MAGIC Consider the following example: you have to parse a json file that contains a text field. The text field happens to occasionally contain multiple rows, which will cause a single record to span over multiple rows. Now the example I showed you here doesn't work anymore because when you read it and ingest it into the bronze layer, the single multiline record will be split into multiple records, making it impossible for the parser to parse those lines in the second stage. 
// MAGIC 
// MAGIC There are two solutions to this problem: 
// MAGIC * Make sure you don't have newlines as part of a single field. If there are, eliminate those before persisting the data into the bronze layer. (recommended)
// MAGIC * Enforce a basic schema with all string types at the bronze layer stage. This means you no longer have to parse the json in the second stage, but you will have to manage the casting to the silver data types manually, which is not the best.

// COMMAND ----------

// MAGIC %md ###generate schema
// MAGIC In the following example I am using the delta history of a random table to generate a schema. I am selecting the field that I know it has a dynamic schema (which is exactly what you are trying to do) and I write it to a delta table. This creates a delta table with a single field which contains a map and the elements of the map can change at every row.
// MAGIC  
// MAGIC  

// COMMAND ----------

spark.sql("describe history delta.`dbfs:/Users/diego.fanesi/deltatest2`")
  .select($"operationParameters")
  .write
  .format("delta")
  .option("path", "dbfs:/Users/diego.fanesi/deltatest3")
  .save()
 
spark.read.format("delta").load("dbfs:/Users/diego.fanesi/deltatest3").schema
 
// columns can also contain lists/arrays or sets (that don't allow duplicates).

// COMMAND ----------


