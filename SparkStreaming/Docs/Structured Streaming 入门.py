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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 编程模型
# MAGIC 
# MAGIC 结构化流中的关键思想是将实时数据流视为连续追加的表。这将导致与批处理模型非常相似的新流处理模型。将流式处理计算表示为标准批处理式查询，就像在静态表上一样，Spark将其作为增量查询在未绑定的输入表上运行。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 基本概念
# MAGIC 
# MAGIC 将输入数据流视为“输入表”。到达流的每个数据项都像是追加到输入表的新行。
# MAGIC 
# MAGIC ![](https://spark.apache.org/docs/3.2.1/img/structured-streaming-stream-as-a-table.png)
# MAGIC 
# MAGIC 对输入的查询将生成“结果表”。每个触发间隔（例如，1秒），新行都会追加到输入表中，最终会更新结果表。每当更新结果表时，我们都希望将更改的结果行写入外部接收器。
# MAGIC 
# MAGIC ![](https://spark.apache.org/docs/3.2.1/img/structured-streaming-model.png)
# MAGIC 
# MAGIC “输出”被定义为写出到外部存储的内容。输出可以在不同的模式下定义：
# MAGIC * 完整模式（Complete Mode） - 整个更新的结果表将写入外部存储。由存储连接器决定如何处理整个表的写入。
# MAGIC * 追加模式（Append Mode） - 只有自上次触发器以来在结果表中追加的新行才会写入外部存储。这仅适用于结果表中现有行不应更改的查询。
# MAGIC * 更新模式（Update Mode） - 只有自上次触发器以来在结果表中更新的行才会写入外部存储（自 Spark 2.1.1 起可用）。请注意，这与完整模式的不同之处在于，此模式仅输出自上次触发器以来已更改的行。如果查询不包含聚合，则它将等效于追加模式。
# MAGIC 
# MAGIC ![](https://spark.apache.org/docs/3.2.1/img/structured-streaming-example-model.png)
# MAGIC 
# MAGIC > **结构化流式处理不会具体化整个表。** 它从流数据源读取最新的可用数据，以增量方式处理该数据以更新结果，然后丢弃源数据。它仅保留更新结果所需的最小中间状态数据（例如，前面示例中的中间计数）。
