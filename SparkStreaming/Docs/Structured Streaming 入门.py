# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # 概述
# MAGIC 
# MAGIC 结构化流（Structured Streaming）式处理是在Spark SQL引擎上构建的可扩展且容错的流处理引擎。可以像对静态数据表达批处理计算一样表达流式计算。Spark SQL引擎将负责以增量方式连续运行它，并在流数据继续到达时更新最终结果。系统通过检查点和预写日志确保端到端的一次容错保证。
# MAGIC 
# MAGIC > 简而言之，结构化流式处理提供快速、可扩展、容错、端到端的一次性流处理，而无需用户对流进行推理。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 入门示例
# MAGIC 
# MAGIC 假设我们希望维护从侦听 TCP 套接字的数据服务器接收的文本数据的运行字数统计。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Spark Streaming(DStreams)实现方式
# MAGIC 
# MAGIC 在学习Spark Streaming(DStreams)的时候，我们实现过监听TCP套接字来统计单词个数的功能。

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)

# lines = ssc.socketTextStream("localhost", 5555)
lines = ssc.socketTextStream("20.187.99.126", 5555)

words = lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a,b: a + b)

words.pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Structured Streaming实现方式
# MAGIC 
# MAGIC 下面我们来看看用Structured Streaming的方式如何实现。

# COMMAND ----------

# 获取行数据
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "20.187.99.126") \
    .option("port", 5555) \
    .load()

print(type(lines))

# 将行拆分成单词
words = lines.select(
   # 炸裂函数
   explode(
       split(lines.value, " ")
   ).alias("word")
)

print(type(words))

# 统计单词的个数
wordCounts = words.groupBy("word").count()

print(type(wordCounts))

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console")

print(type(query))

query.start()
query.awaitTermination()

# COMMAND ----------

query.stop()
