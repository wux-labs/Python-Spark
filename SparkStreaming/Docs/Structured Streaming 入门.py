# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # 概述
# MAGIC 
# MAGIC **结构化流（Structured Streaming）**式处理是在Spark SQL引擎上构建的可扩展且容错的流处理引擎。可以像对静态数据表达批处理计算一样表达流式计算。Spark SQL引擎将负责以增量方式连续运行它，并在流数据继续到达时更新最终结果。系统通过检查点和预写日志确保端到端的一次容错保证。
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
lines = ssc.socketTextStream("104.208.105.98", 5555)

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

import pyspark.sql.functions as F

# 获取行数据
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "104.208.105.98") \
    .option("port", 5555) \
    .load()

print(type(lines))

# 将行拆分成单词
words = lines.select(
   # 炸裂函数
   F.explode(
       F.split(lines.value, " ")
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
# query.awaitTermination()

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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 处理事件时间和延迟数据
# MAGIC 
# MAGIC **事件时间**是嵌入在数据本身中的时间，也就是产生增量数据的时间，而不是Spark接收增量数据的时间，这个时间体现在数据中可以作为一列存在，比如数据中存在UPDATED_TS字段表示数据被更新的时间。这就可以支持我们进行基于时间窗口的聚合操作（例如每分钟的事件数量），只要针对数据中的UPDATED_TS字段进行分组和聚合即可。每个时间窗口就是一个分组，而每一行都可以落入对应的分组内。因此，类似这样的基于时间窗口的分组聚合操作，既可以被定义在一份静态数据上，也可以被定义在一个实时数据流上。
# MAGIC 
# MAGIC 此外，这种模型也天然支持延迟到达的数据。Spark会负责更新结果表，因此它有绝对的控制权来针对延迟到达的数据进行聚合结果的重新计算。自Spark2.1之后Structured Streaming开始支持 watermarking(水位线)，允许用户指定延时数据的阈值，并允许引擎相应地清除阈值范围之外的旧状态。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 容错语义
# MAGIC 
# MAGIC 提供端到端的精确一次语义是Structured Streaming设计背后的关键目标之一。为了实现这一目标，Structured Streaming设计了源（sources）、接收器（sinks）和执行引擎（execution engine），以可靠地跟踪处理的确切进度，以便它可以通过重新启动和/或重新处理来处理任何类型的故障。假定每个流源都有偏移量（类似于 Kafka 偏移量或 Kinesis 序列号）来跟踪流中的读取位置。引擎使用检查点（checkpointing）和预写日志（write-ahead logs）来记录每个触发器中正在处理的数据的偏移范围。接收器设计成可以支持在多次计算处理时保持幂等性。结合使用可重放源和幂等接收器，结构化流式处理可以在任何故障下确保端到端的精确一次语义。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # 使用Datasets和DataFrames的API
# MAGIC 
# MAGIC 从Spark 2.0开始，DataFrame和Dataset可以表示静态的有界数据，也可以表示流式处理、无界数据。与静态DataFrame/Dataset类似，可以使用通用入口点SparkSession从流式处理源创建流式处理DataFrame/Dataset，并对它们应用与静态DataFrame/Dataset相同的操作。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 创建流式DataFrames和流式Datasets
# MAGIC 
# MAGIC 流式DataFrame可以通过SparkSession.readStream()返回的DataStreamReader接口进行创建。与用于创建静态DataFrame的read接口类似，可以指定源的详细信息：数据格式（data format）、架构（schema）、选项（options）等。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 输入源
# MAGIC 
# MAGIC 有一些内置源。
# MAGIC 
# MAGIC * 文件源 - 读取作为数据流写入目录中的文件。文件将按文件修改时间的顺序进行处理。支持的文件格式包括文本、CSV、JSON、ORC、Parquet。请注意，文件必须以原子方式放置在给定目录中，这在大多数文件系统中可以通过文件移动操作来实现。
# MAGIC 
# MAGIC * Kafka源 - 从Kafka读取数据。它与Kafka版本0.10.0或更高版本兼容。
# MAGIC 
# MAGIC * Socket源（用于测试）- 从Socket连接读取UTF8文本数据。请注意，这应仅用于测试，因为这不提供端到端容错保证。
# MAGIC 
# MAGIC * Rate源（用于测试）- 以每秒指定的行数生成数据，每个输出行包含一个timestamp和value。其中timestamp是包含消息调度时间的Timestamp类型，value是包含消息计数的Long类型，从0开始作为第一行。此源用于测试和基准测试。
# MAGIC 
# MAGIC 某些源不具有容错能力，因为它们不保证在发生故障后可以使用检查点偏移量重放数据，比如Socket源。
# MAGIC 
# MAGIC 以下是 Spark 中所有源的详细信息。
# MAGIC 
# MAGIC | 源           | 选项                                                         | 容错 |
# MAGIC | :----------- | :----------------------------------------------------------- | :--- |
# MAGIC | **文件源**   | `path`：输入目录的路径，并且对所有文件格式通用。<br/><br/>`maxFilesPerTrigger`：每个触发器中要考虑的最大新文件数（默认：无最大值）。<br/><br/>`latestFirst`： 是否首先处理最新的新文件，当存在大量积压文件时很有用（默认：false）。<br/><br/>`fileNameOnly`： 是否仅基于文件名而不是完整路径检查新文件（默认：false）。<br/>将此设置为`true`时，以下文件将被视为同一文件，因为它们的文件名“dataset.txt”是相同的： <br/>"file:///dataset.txt"<br/>"s3：//a/dataset.txt"<br/>"s3n：//a/b/dataset.txt"<br/>"s3a：//a/b/c/dataset.txt"<br/><br/>`maxFileAge`：在此目录中可以找到的文件的最大期限，在这之前的会被忽略。对于第一批，所有文件都将被视为有效。<br/>如果`latestFirst`设置为 `true` 并设置了`maxFilesPerTrigger`，则将忽略此参数，因为有效且应处理的旧文件可能会被忽略。<br/>最大期限是根据最新文件的时间戳指定的，而不是当前系统的时间戳。（默认值：1 周）<br/><br/> `cleanSource`：选项，用于在处理后清理已完成的文件。可用选项包括"archive"、"delete"、"off"。<br/>如果未提供该选项，则默认值为"off"。 | 是 |
# MAGIC | **Socket源** | `host`：要连接到的主机，必须指定<br/><br/>`port`：要连接到的端口，必须指定 | 否 |
# MAGIC | **Rate源**   | `rowsPerSecond`（例如 100，默认值：1）：每秒应生成多少行。<br/><br/>`rampUpTime`（例如 5s，默认值：0s）：在生成速度变为`rowsPerSecond`之前需要多长时间上升。使用比秒更精细的粒度将被截断为整数秒。<br/><br/>`numPartitions`（例如 10，默认值：Spark的默认并行度）：生成的行的分区号。<br/><br/>源将尽力达到`rowsPerSecond`，但查询可能受资源限制，可以进行`numPartitions`调整以帮助达到所需的速度。| 是 |
# MAGIC | **Kafka源** | 请参阅[Kafka集成指南](https://spark.apache.org/docs/3.2.1/structured-streaming-kafka-integration.html)。 | 是 |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 下面来看一些案例。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 文件源

# COMMAND ----------

from pyspark.sql.types import StructType, StringType

schema = StructType().\
add("OrderNumber", StringType(), nullable=False).\
add("OrderDate", StringType(), nullable=False).\
add("ItemName", StringType(), nullable=False).\
add("Quantity", StringType(), nullable=False).\
add("ProductPrice", StringType(), nullable=False).\
add("TotalProducts", StringType(), nullable=False)

df = spark.readStream.option("sep", ",").schema(schema).format("csv").load("/mnt/databrickscontainer1")
# df = spark.readStream.option("sep", ",").schema(schema).csv("/mnt/databrickscontainer1/")

print(type(df))

# 想要查看、输出数据，必须使用writeStream.start()
# AnalysisException: Queries with streaming sources must be executed with writeStream.start();
# df.show()
print(df.isStreaming)

# 不带聚合操作直接显示数据
# format: console/memory
# query = df.writeStream.format("console").start()

# 带有聚合操作的，需要设置outputMode
df.createOrReplaceTempView("orders")
query = spark.sql("select ItemName, count(*) from orders group by ItemName").writeStream.outputMode("complete").format("console").start()

# query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Socket源

# COMMAND ----------

import pyspark.sql.functions as F

# 获取行数据
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "104.208.105.98") \
    .option("port", 5555) \
    .load()

print(type(lines))

# 将行拆分成单词
words = lines.select(
   # 炸裂函数
   F.explode(
       F.split(lines.value, " ")
   ).alias("word")
)
print(type(words))

# 不带聚合操作直接显示数据
query = words.writeStream.format("console").start()

# 带有聚合操作的，需要设置outputMode
# 统计单词的个数
# wordCounts = words.groupBy("word").count()
# print(type(wordCounts))
# query = wordCounts.writeStream.outputMode("complete").format("console").start()

print(type(query))

# query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Kafka源
# MAGIC 
# MAGIC [https://spark.apache.org/docs/3.2.1/structured-streaming-kafka-integration.html](https://spark.apache.org/docs/3.2.1/structured-streaming-kafka-integration.html)
# MAGIC 
# MAGIC #### Ubuntu20.04下Kafka安装与部署
# MAGIC 
# MAGIC ```
# MAGIC sudo apt-get update
# MAGIC ```
# MAGIC 
# MAGIC ##### 安装JDK
# MAGIC 
# MAGIC ```
# MAGIC sudo apt install openjdk-8-jdk
# MAGIC ```
# MAGIC 
# MAGIC ##### 下载Kafka
# MAGIC 
# MAGIC ```
# MAGIC wget https://downloads.apache.org/kafka/3.3.1/kafka_2.12-3.3.1.tgz
# MAGIC wget https://archive.apache.org/dist/kafka/3.1.0/kafka_2.12-3.1.0.tgz
# MAGIC ```
# MAGIC 
# MAGIC ##### 安装Kafka
# MAGIC 
# MAGIC ###### 解压
# MAGIC 
# MAGIC ```
# MAGIC mkdir apps
# MAGIC tar -xzf kafka_2.12-3.1.0.tgz -C apps/
# MAGIC ```
# MAGIC 
# MAGIC ###### 配置
# MAGIC 
# MAGIC ```
# MAGIC vi apps/kafka_2.12-3.1.0/config/zookeeper.properties
# MAGIC vi apps/kafka_2.12-3.1.0/config/server.properties
# MAGIC ```
# MAGIC 
# MAGIC ###### 启动
# MAGIC 
# MAGIC ```
# MAGIC cd apps/kafka_2.12-3.1.0
# MAGIC 
# MAGIC bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
# MAGIC 
# MAGIC bin/kafka-server-start.sh -daemon config/server.properties
# MAGIC ```
# MAGIC 
# MAGIC ##### 使用Kafka
# MAGIC 
# MAGIC ###### 创建Topic
# MAGIC 
# MAGIC ```
# MAGIC bin/kafka-topics.sh --create --topic KafkaFirstTopic --partitions 1 --replication-factor 1 --bootstrap-server 10.0.0.4:9092
# MAGIC 
# MAGIC bin/kafka-topics.sh --list --bootstrap-server 10.0.0.4:9092
# MAGIC ```
# MAGIC 
# MAGIC ###### 发送消息
# MAGIC 
# MAGIC ```
# MAGIC bin/kafka-console-producer.sh --bootstrap-server 10.0.0.4:9092 --topic KafkaFirstTopic
# MAGIC ```
# MAGIC 
# MAGIC ###### 消费消息
# MAGIC 
# MAGIC ```
# MAGIC bin/kafka-console-consumer.sh --bootstrap-server 10.0.0.4:9092 --topic KafkaFirstTopic
# MAGIC ```

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC cat /etc/hosts

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC echo "104.208.105.98 wux-labs-vm.internal.cloudapp.net" >> /etc/hosts

# COMMAND ----------

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "104.208.105.98:9092") \
  .option("subscribe", "KafkaFirstTopic") \
  .load()

# 原始数据
query = df.writeStream.format("console").start()

# 对数据做转换
df.selectExpr("explode(split(CAST(value AS STRING),\" \"))").writeStream.format("console").start()

# 对数据做转换、统计
df.selectExpr("explode(split(CAST(value AS STRING),\" \"))").groupBy("col").count().writeStream.outputMode("complete").format("console").start()

# query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 流式 DataFrames/Datasets 的模式（Schema）推断和分区
# MAGIC 
# MAGIC 默认情况下，来自基于文件的源的结构化流式处理需要指定schema，而不是依赖Spark自动推断schema。此限制可确保将一致的schema用于流式处理查询，即使在失败的情况下也是如此。对于临时用例，可以通过将`spark.sql.streaming.schemaInference`设置为`true`。
# MAGIC 
# MAGIC 当存在名为/key=value/的子目录并且列表将自动递归到这些目录中时，分区发现确实会发生。如果这些列出现在用户提供的schema中，则Spark将根据正在读取的文件的路径填充这些列。组成分区方案的目录必须在查询开始时存在，并且必须保持静态。例如，当/data/year=2015/存在时，可以添加/data/year=2016/，但更改分区列（即通过创建目录/data/date=2016-04-17/）是无效的。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 对流式DataFrames/Datasets的操作
# MAGIC 
# MAGIC 可以对流式DataFrames/Datasets应用各种操作，范围从非类型化的、SQL类的操作（例如select、where、groupBy）到类型化的RDD类操作（如map、filter、flatMap）。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 基本操作 - 选择、投影、聚合
# MAGIC 
# MAGIC 流式处理支持对DataFrame/Dataset执行大多数常见操作。

# COMMAND ----------

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "104.208.105.98:9092") \
  .option("subscribe", "KafkaFirstTopic") \
  .load()

# 原始数据
query = df.writeStream.format("console").start()

# 对数据做转换
df.selectExpr("explode(split(CAST(value AS STRING),\" \"))").writeStream.format("console").start()

# 对数据做转换、统计
df.selectExpr("explode(split(CAST(value AS STRING),\" \"))").groupBy("col").count().writeStream.outputMode("complete").format("console").start()


# COMMAND ----------

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "104.208.105.98:9092") \
  .option("subscribe", "KafkaFirstTopic") \
  .load()

df.createOrReplaceTempView("topics")

spark.sql("select cast(value as string) as value from topics").createOrReplaceTempView("values")

spark.sql("select explode(split(value, ' ')) as word from values").createOrReplaceTempView("words")

spark.sql("select word,count(*) from words group by word having count(*) > 1").writeStream.outputMode("complete").format("console").start()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 事件时间的窗口操作
# MAGIC 
# MAGIC 使用结构化流式处理，滑动事件时间窗口上的聚合非常简单，并且与分组聚合非常相似。在分组聚合中，为用户指定的分组列中的每个唯一值维护聚合值（例如计数）。对于基于窗口的聚合，将为行的事件时间所属的每个窗口维护聚合值。让我们通过插图来理解这一点。
# MAGIC 
# MAGIC 假设我们的入门示例被修改，流现在包含行以及生成行的时间。我们希望在 10 分钟内计算字数，每 5 分钟更新一次，而不是运行字数统计。也就是说，在 10 分钟窗口 12：00 - 12：10、12：05 - 12：15、12：10 - 12：20 等之间收到的字数。请注意，12：00 - 12：10 表示在 12：00 之后但 12：10 之前到达的数据。现在，考虑在 12：07 收到的一个词。此字应递增对应于两个窗口 12：00 - 12：10 和 12：05 - 12：15 的计数。因此，计数将按分组键（即单词）和窗口（可以从事件时间计算）进行索引。
# MAGIC 
# MAGIC ![](https://spark.apache.org/docs/3.2.1/img/structured-streaming-window.png)
# MAGIC 
# MAGIC 由于此窗口化类似于分组，因此在代码中，可以使用 groupBy() 和 window() 操作来表示窗口化聚合。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 处理延迟数据和水印
# MAGIC 
# MAGIC 现在考虑如果其中一个事件迟到应用程序会发生什么情况。例如，应用程序可以在 12：11 接收在 12：04（即事件时间）生成的单词。应用程序应使用时间 12：04 而不是 12：11 来更新窗口 12:00 - 12:10 的旧计数。这在我们基于窗口的分组中自然发生 - 结构化流式处理可以长时间保持部分聚合的中间状态，以便后期数据可以正确更新旧窗口的聚合，如下图所示。
# MAGIC 
# MAGIC ![](https://spark.apache.org/docs/3.2.1/img/structured-streaming-late-data.png)
