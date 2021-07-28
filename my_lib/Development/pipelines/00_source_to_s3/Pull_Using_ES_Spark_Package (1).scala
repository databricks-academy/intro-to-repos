// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Pulling logs from ElasticSearch into Databricks
// MAGIC 
// MAGIC ## Prerequisites: Download elasticsearch-spark JAR and create library in Databricks
// MAGIC 1. Download the appropriate version of the [ES-Hadoop package](https://www.elastic.co/downloads/hadoop).
// MAGIC 1. Extract `elasticsearch-spark-xx_x.xx-x.x.x.jar` from the zip file. For example, `elasticsearch-spark-20_2.11-6.2.3.jar`.
// MAGIC 1. Create the library in your Databricks Workspace and attach to one or more clusters.
// MAGIC 
// MAGIC ## Documentation
// MAGIC * [ES-Hadoop documentation](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/float.html)
// MAGIC * [Release Notes](https://www.elastic.co/guide/en/elasticsearch/hadoop/6.2/eshadoop-6.2.3.html)
// MAGIC * [Elasticsearch configuration](https://www.elastic.co/guide/en/elasticsearch/hadoop/master/configuration.html)
// MAGIC * [Elasticsearch Spark documentation](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html)
// MAGIC 
// MAGIC ## Secrets 
// MAGIC The username and password for accessing elasticsearch is stored in a secrets scope.
// MAGIC To create the secrets scope, use the databricks cli as follows:
// MAGIC 
// MAGIC     databricks secrets create-scope --scope elasticsearch
// MAGIC     databricks secrets put --scope elasticsearch --key username
// MAGIC     databricks secrets put --scope elasticsearch --key password
// MAGIC     
// MAGIC   

// COMMAND ----------

val elasticsearch_url = "0be5a95263da4a5b8d2afedf4c45a5cd.us-east-1.aws.found.io"
val port = "9243"
val username = dbutils.secrets.get(scope = "elasticsearch", key = "username")
val password = dbutils.secrets.get(scope = "elasticsearch", key = "password")
val index_name = "element1-2021.01.29"

// COMMAND ----------

import org.elasticsearch.spark.sql._

val query = "{\"exists\": { \"field\" : \"log.file.path\" }}"
val reader = spark.read
  //.format("org.elasticsearch.spark.sql")
  .format("es")
  .option("es.net.ssl","true")
  .option("es.nodes", elasticsearch_url)
  .option("es.net.http.auth.user", username)
  .option("es.net.http.auth.pass", password)
  .option("es.port",port)
  .option("es.query",query)
  .option("es.resource",index_name)
  .option("es.nodes.wan.only","true")
  .option("es.scroll.size",10000)
  .option("pushdown","true")
 
val df1 = reader.load(index_name)//.select("@timestamp","agent","application","processed")
val df2 = df1.select(
  $"@timestamp".alias("serverTimestamp"),
  $"agent.hostname".alias("hostname"),
  $"application",
  $"processed.DateTime".alias("clientTimestamp"),
  $"processed.LineNumber".alias("lineNumber"),
  $"processed.Severity".alias("severity"),
  $"processed.Source".alias("source"),
  $"processed.Thread".alias("thread"),
  $"processed.Message".alias("message"),
  $"log.file.path".alias("logFile"))

/*.where(df.processed.DateTime > '2020-12-07 23:00')
#df = df.orderBy(df.processed.DateTime.desc())
#  .select(df.application,df.processed') \*/

display(df2)


// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StringType}
//val schema = new StructType()
//  .add("@timestamp",StringType,true)
//  .add("agent",StringType,true)

val reader = spark.read
  .format("org.elasticsearch.spark.sql")
  .option("es.net.ssl","true")
  .option("es.nodes", elasticsearch_url)
  .option("es.net.http.auth.user", username)
  .option("es.net.http.auth.pass", password)
  .option("es.port",port)
  .option("es.resource",s"${index_name}/_doc")
  .option("es.nodes.wan.only","true")
 
val df = reader.load(index_name).select("@timestamp","agent")
//display(df)
val s = df.schema

val df2 = reader.load(index_name).schema(s).select("@timestamp","agent")


// COMMAND ----------

df2.limit(100000).
  write.
  format("delta").
  partitionBy("application").
  saveAsTable("logtry6")

// COMMAND ----------

df2.
  limit(1000000).
  write.
  format("delta").
  saveAsTable("logtry19")

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from logtry16
