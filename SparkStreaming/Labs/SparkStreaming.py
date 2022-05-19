# Databricks notebook source
from pyspark.streaming import StreamingContext

connectionString = "Endpoint=sb://wux-event-hubs.servicebus.windows.net/;EntityPath=eventhub1;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xHOOPZpJFaeoY0NwoqQfbyIOthHxaNvWns/Q5gx5R0s="

ehConf = {
  'eventhubs.connectionString' : connectionString,
  'eventhubs.consumerGroup' : "$Default"
}

df = spark.readStream.format("eventhubs").options(**ehConf).load()

stream = df.writeStream.format("console").option("checkpointLocation","/checkpoint").start("/")

stream.awaitTermination()

# show = df.writeStream.start(path=)
# show = df.show().start()

# show.awaitTermination()
# show
