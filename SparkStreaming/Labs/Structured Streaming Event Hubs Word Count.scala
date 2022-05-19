// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC **必要条件**
// MAGIC 
// MAGIC * 使用 Maven 坐标 `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21` 在 Databricks 工作区中创建库，排除`log4j:log4j`，并将库安装到集群。

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }

// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
val connectionString = ConnectionStringBuilder("Endpoint=sb://wux-event-hubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xHOOPZpJFaeoY0NwoqQfbyIOthHxaNvWns/Q5gx5R0s=")
  .setEventHubName("eventhub1")
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

// split lines by whitespaces and explode the array as rows of 'word'
val df = eventhubs.select(explode(split($"body".cast("string"), "\\s+")).as("word"))
  .groupBy($"word")
  .count

// COMMAND ----------

// follow the word counts as it updates
display(df.select($"word", $"count"))
