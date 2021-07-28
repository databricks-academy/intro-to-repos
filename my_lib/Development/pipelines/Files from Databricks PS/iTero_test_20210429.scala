// Databricks notebook source
// MAGIC %md ###Example of foreachBatch
// MAGIC 
// MAGIC Here I am providing an example to the use of foreachBatch in python.

// COMMAND ----------

display(spark.readStream.format("rate").load())

// COMMAND ----------

// MAGIC %python  
// MAGIC 
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
// MAGIC json_sample_bad2 = '{"status": "publish", "description": null, "creator": "jules_damji", "link": "https://databricks.com/blog/2017/08/09/apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks.html", "authors": null, "id": 11820, "categories": ["Company Blog", "Product"], "dates": {"publishedOn": "2017-08-09", "tz": "UTC", "createdOn": "2017-08-09"}, "title": "Apache Spark\u2019s Structured Streaming with Amazon Kinesis on Databricks", "slug": "apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks", "new": "test"}'
// MAGIC 
// MAGIC json_sample_bad3 = '{"status": "publish", "description": null, "creator": "jules_damji", "link": "https://databricks.com/blog/2017/08/09/apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks.html", "authors": null, "id": "4895734", "categories": ["Company Blog", "Product"], "dates": {"publishedOn": "2017-08-09", "tz": "UTC", "createdOn": "2017-08-09"}, "title": "Apache Spark\u2019s Structured Streaming with Amazon Kinesis on Databricks", "slug": "apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks"}'
// MAGIC 
// MAGIC json_sample_bad4 = '{"status": 45234523, "description": null, "creator": "jules_damji", "link": "https://databricks.com/blog/2017/08/09/apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks.html", "authors": null, "id": "4895734", "categories": ["Company Blog", "Product"], "dates": {"publishedOn": "2017-08-09", "tz": "UTC", "createdOn": "2017-08-09"}, "title": "Apache Spark\u2019s Structured Streaming with Amazon Kinesis on Databricks", "slug": "apache-sparks-structured-streaming-with-amazon-kinesis-on-databricks"}'
// MAGIC 
// MAGIC json_df = spark.createDataFrame(data=[(json_sample_good,1), (json_sample_bad,2), (json_sample_bad2, 3), (json_sample_bad3, 3), (json_sample_bad4, 3)], schema=["value", "nothing"]).drop(col("nothing"))
// MAGIC 
// MAGIC display(json_df)

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
// MAGIC import pyspark.sql.functions as F
// MAGIC #json_df.select(from_json(col("value"), schema_of_json(col("value"))))
// MAGIC 
// MAGIC df = json_df.withColumn("schema", F.schema_of_json(F.col("value")))
// MAGIC 
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC from pyspark.sql.dataframe import DataFrame
// MAGIC 
// MAGIC json_schema = "authors String, categories String, content String, creator String, dates String, description String, id Long, link String, slug String, status String, title String, _corrupted String"
// MAGIC 
// MAGIC rdd = json_df.rdd.map(lambda x: x.asDict()["value"])
// MAGIC 
// MAGIC parsed_df = spark.read.json(rdd, schema=json_schema, mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupted")
// MAGIC 
// MAGIC display(parsed_df)

// COMMAND ----------

// MAGIC %md ###More efficient scala json parser
// MAGIC When features are not available in pyspark, you can call scala methods from python. This is exactly what I am doing in this case. However, not all methods can be called by python. For instance, when a method requires a type (scala syntax .methodname[type](....)) those cannot be called. 
// MAGIC 
// MAGIC To workaround this issue, I had to create a scala object that calls the .as[String] method and wrap it into a scala method which I can call from python.

// COMMAND ----------

// MAGIC %scala
// MAGIC package org.itero
// MAGIC 
// MAGIC import org.apache.spark.sql._
// MAGIC //This is something I will be calling from python later.
// MAGIC object PythonUtils {
// MAGIC   def toParseableDF(df:DataFrame) = {
// MAGIC     import df.sparkSession.implicits._
// MAGIC     
// MAGIC     df.as[String]
// MAGIC   }
// MAGIC }

// COMMAND ----------

// MAGIC %python
// MAGIC #Unfortunately, there is no way to call a typed scala method from python. The only way is to define a scala class and method that calls the typed method, and use it from python. 
// MAGIC j_string_dataset = spark._jvm.org.itero.PythonUtils.toParseableDF(json_df._jdf)
// MAGIC 
// MAGIC parsed_df_wj = DataFrame(spark._jsparkSession.read().format("json").option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupted").schema(json_schema).json(j_string_dataset), spark)
// MAGIC 
// MAGIC 
// MAGIC display(parsed_df_wj)

// COMMAND ----------

quarantine = parsed_df.filter(!col("_corrupted").isNull)
silver = parsed_df.filter(col("_corrupted").isNull)

qurantine.write(....)
silver.write(...)

// COMMAND ----------

df = spark.read.format("delta")......load()

df1 = df.withColumn("test1", ....).withColumn("test2", ....).withColumn("test3", ....).withColumn("test4", ....).cache()

df1.filter(col("test1") == 4).write.format("delta").....save()
df1.filter(col("test1") > 4).write.format("delta").....save()

df1.unpersist()

// COMMAND ----------

df = spark.read.format("delta")......load()

df1 = df.withColumn("test1", ....).withColumn("test2", ....).withColumn("test3", ....).withColumn("test4", ....).join(...)

df1.filter(col("test1") == 4).write.format("delta").....save()
df1.filter(col("test1") > 4).write.format("delta").....save()

df1.unpersist()

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
