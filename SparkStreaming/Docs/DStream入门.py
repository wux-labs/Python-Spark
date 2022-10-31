# Databricks notebook source
# MAGIC %md
# MAGIC # DStream入门

# COMMAND ----------

# MAGIC %md
# MAGIC ## 基本数据源
# MAGIC 
# MAGIC 直接在流式处理上下文API中提供的源。示例：文件系统和套接字连接。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 文件流
# MAGIC 
# MAGIC 为了从与HDFS API兼容的任何文件系统（即 HDFS、S3、NFS 等）上的文件读取数据，可以通过以下方式创建 DStream。
# MAGIC 
# MAGIC ```
# MAGIC StreamingContext.fileStream[KeyClass, ValueClass, InputFormatClass](dataDirectory)
# MAGIC ```
# MAGIC 
# MAGIC 文件流不需要运行接收器，因此无需分配任何内核来接收文件数据。
# MAGIC 
# MAGIC 对于简单的文本文件，最简单的方法是：StreamingContext.textFileStream(dataDirectory)。

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark._
# MAGIC import org.apache.spark.streaming._
# MAGIC import org.apache.spark.streaming.StreamingContext._
# MAGIC 
# MAGIC import org.apache.hadoop.io.Text
# MAGIC import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
# MAGIC 
# MAGIC val ssc = new StreamingContext(sc,Seconds(10))
# MAGIC 
# MAGIC val lines = ssc.fileStream[Text,Text,KeyValueTextInputFormat]("/mnt/databrickscontainer1/SparkStreaming")
# MAGIC 
# MAGIC // 由于fileStream读取到的数据是KV型数据，我们只处理其中的K值，所以需要用 _._1 来取得K值
# MAGIC val words = lines.flatMap(x => x._1.toString().split(" ")).map((_,1)).reduceByKey(_+_)
# MAGIC 
# MAGIC words.print()
# MAGIC 
# MAGIC ssc.start()
# MAGIC ssc.awaitTermination()
# MAGIC // ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC **由于fileStream方法需要泛型支持，所以fileStream方法在Python中不可用，Python仅支持textFileStream。**

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)

lines = ssc.textFileStream("/mnt/databrickscontainer1/SparkStreaming")

words = lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a,b: a + b)

words.pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 如何监视目录
# MAGIC 
# MAGIC Spark流式处理将监视dataDirectory目录并处理在该目录中创建的任何文件。
# MAGIC 
# MAGIC * 可以监视一个简单的目录，例如 "hdfs://namenode:8040/logs/"。直接位于此路径下的所有文件都将在被发现时进行处理。
# MAGIC * 可以提供 POSIX glob 模式，例如 "hdfs://namenode:8040/logs/2017/\*" 。DStream将包含目录中与模式匹配的所有文件。也就是说：它是目录的模式，而不是目录中的文件的模式。
# MAGIC * 所有文件必须采用相同的数据格式。
# MAGIC * 文件根据其修改时间（而不是其创建时间）被视为时间段的一部分。
# MAGIC * 处理后，在当前窗口中对文件所做的更改将不会导致重新读取该文件。也就是说：更新将被忽略。
# MAGIC * 目录下的文件越多，扫描更改所需的时间就越长，即使没有修改任何文件也是如此。
# MAGIC * 如果使用通配符来标识目录（例如 "hdfs://namenode:8040/logs/2016-\*"），则重命名整个目录以匹配路径会将该目录添加到受监视目录列表中。只有目录中修改时间在当前窗口内的文件才会包含在流中。
# MAGIC * 调用 FileSystem.setTimes() 来修复时间戳是一种在以后的窗口中选取文件的方法，即使其内容没有更改。

# COMMAND ----------

# MAGIC %md
# MAGIC #### 使用对象存储作为数据源
# MAGIC 
# MAGIC “Full”文件系统（如 HDFS）倾向于在创建输出流后立即设置其文件的修改时间。当文件被打开时，甚至在数据完全写入之前，它也可能包含在DStream中，之后将忽略同一窗口中对文件的更新。也就是说：可能会错过更改，并且从流中省略数据。
# MAGIC 
# MAGIC 要确保在窗口中选取更改，请将文件写入不受监视的目录，然后在输出流关闭后立即将其重命名为目标目录。如果重命名的文件在创建过程中出现在扫描的目标目录中，则将选取新数据。
# MAGIC 
# MAGIC 相比之下，对象存储（如Amazon S3和Azure存储）通常具有**缓慢的重命名操作**，因为数据实际上是复制的。此外，重命名的对象可能将rename()操作的时间作为其修改时间，因此可能不被视为原始创建时间的窗口的一部分，也就是有延迟。
# MAGIC 
# MAGIC 需要针对目标对象存储进行仔细测试，以验证存储的时间戳行为是否与Spark流式处理的预期一致。直接写入目标目录可能是通过所选对象存储流式传输数据的适当策略。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 套接字
# MAGIC 
# MAGIC 可以使用 ssc.socketTextStream(...) 从通过TCP套接字连接接收的文本数据创建DStream。
# MAGIC 
# MAGIC > nc -lk 5555

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)

# lines = ssc.socketTextStream("localhost", 5555)
lines = ssc.socketTextStream("20.187.125.128", 5555)

words = lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a,b: a + b)

words.pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD队列
# MAGIC 
# MAGIC 要使用测试数据测试Spark流式处理应用程序，还可以使用streamingContext.queueStream(queueOfRDDs)创建基于RDD队列的DStream。推送到队列中的每个RDD将被视为DStream中的一批数据，并像流一样进行处理。

# COMMAND ----------

from pyspark.streaming import StreamingContext
import time

ssc = StreamingContext(sc, 10)

# 定义一个队列
rddQueue = [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

# 往队列中写入数据
for i in range(5):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

lines = ssc.queueStream(rddQueue)

words = lines.map(lambda x: (x % 10, 1)).reduceByKey(lambda a,b: a + b)

words.pprint()

ssc.start()

ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 高级数据源
# MAGIC 
# MAGIC Spark Streaming支持从外部数据源获取数据来构建DStream。实际项目中使用较多的就是Kafka。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Kafka
# MAGIC 
# MAGIC 在Spark 2.x版本中，PySpark Streaming还支持Kafka模块，但是在Spark 3.x版本中，该功能被移除了。
# MAGIC 
# MAGIC https://spark.apache.org/docs/2.4.8/api/python/index.html
# MAGIC 
# MAGIC https://spark.apache.org/docs/3.0.0/api/python/index.html
# MAGIC 
# MAGIC https://spark.apache.org/docs/3.2.1/api/python/index.html
# MAGIC 
# MAGIC 我们无法直接使用Python来开发，只能使用Scala/Java。
# MAGIC 
# MAGIC 需要为集群安装库：`org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1`和`org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1`

# COMMAND ----------

from pyspark.streaming.kafka import KafkaUtils


# COMMAND ----------

# MAGIC %md
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
# MAGIC ping 104.208.105.98

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC cat /etc/hosts

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC echo "104.208.105.98 wux-labs-vm.internal.cloudapp.net" >> /etc/hosts

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark._
# MAGIC import org.apache.spark.streaming._
# MAGIC import org.apache.spark.streaming.StreamingContext._
# MAGIC import org.apache.spark.streaming.kafka010._
# MAGIC import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
# MAGIC import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
# MAGIC 
# MAGIC import org.apache.kafka.clients.consumer.ConsumerRecord
# MAGIC import org.apache.kafka.common.serialization.StringDeserializer
# MAGIC 
# MAGIC val ssc = new StreamingContext(sc,Seconds(10))
# MAGIC 
# MAGIC val kafkaParams = Map[String, Object](
# MAGIC   "bootstrap.servers" -> "104.208.105.98:9092",
# MAGIC   "key.deserializer" -> classOf[StringDeserializer],
# MAGIC   "value.deserializer" -> classOf[StringDeserializer],
# MAGIC   "group.id" -> "databricks_kafka_group",
# MAGIC   "auto.offset.reset" -> "latest",
# MAGIC   "enable.auto.commit" -> (false: java.lang.Boolean)
# MAGIC )
# MAGIC 
# MAGIC val topics = Array("KafkaFirstTopic")
# MAGIC val stream = KafkaUtils.createDirectStream[String, String](
# MAGIC   ssc,
# MAGIC   PreferConsistent,
# MAGIC   Subscribe[String, String](topics, kafkaParams)
# MAGIC )
# MAGIC 
# MAGIC // stream.map(record => (record.key, record.value)).reduceByKey(_+_).print()
# MAGIC stream.flatMap(x => x.value.toString().split(" ")).map((_,1)).reduceByKey(_+_).print()
# MAGIC 
# MAGIC ssc.start()
# MAGIC ssc.awaitTermination()

# COMMAND ----------

# MAGIC %scala
# MAGIC ssc.stop()

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC jps

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC kill -9 516

# COMMAND ----------

# MAGIC %md
# MAGIC ## DStream上的Transformations
# MAGIC 
# MAGIC 与RDD类似，转换允许修改输入DStream中的数据。DStreams支持普通Spark RDD上可用的许多转换。一些常见的如下。
# MAGIC 
# MAGIC | Transformation                           | Meaning                                                      |
# MAGIC | :--------------------------------------- | :----------------------------------------------------------- |
# MAGIC | **map**(*func*)                          | Return a new DStream by passing each element of the source DStream through a function *func*. |
# MAGIC | **flatMap**(*func*)                      | Similar to map, but each input item can be mapped to 0 or more output items. |
# MAGIC | **filter**(*func*)                       | Return a new DStream by selecting only the records of the source DStream on which *func* returns true. |
# MAGIC | **repartition**(*numPartitions*)         | Changes the level of parallelism in this DStream by creating more or fewer partitions. |
# MAGIC | **union**(*otherStream*)                 | Return a new DStream that contains the union of the elements in the source DStream and *otherDStream*. |
# MAGIC | **count**()                              | Return a new DStream of single-element RDDs by counting the number of elements in each RDD of the source DStream. |
# MAGIC | **reduce**(*func*)                       | Return a new DStream of single-element RDDs by aggregating the elements in each RDD of the source DStream using a function *func* (which takes two arguments and returns one). The function should be associative and commutative so that it can be computed in parallel. |
# MAGIC | **countByValue**()                       | When called on a DStream of elements of type K, return a new DStream of (K, Long) pairs where the value of each key is its frequency in each RDD of the source DStream. |
# MAGIC | **reduceByKey**(*func*, [*numTasks*])    | When called on a DStream of (K, V) pairs, return a new DStream of (K, V) pairs where the values for each key are aggregated using the given reduce function. **Note:** By default, this uses Spark's default number of parallel tasks (2 for local mode, and in cluster mode the number is determined by the config property `spark.default.parallelism`) to do the grouping. You can pass an optional `numTasks` argument to set a different number of tasks. |
# MAGIC | **join**(*otherStream*, [*numTasks*])    | When called on two DStreams of (K, V) and (K, W) pairs, return a new DStream of (K, (V, W)) pairs with all pairs of elements for each key. |
# MAGIC | **cogroup**(*otherStream*, [*numTasks*]) | When called on a DStream of (K, V) and (K, W) pairs, return a new DStream of (K, Seq[V], Seq[W]) tuples. |
# MAGIC | **transform**(*func*)                    | Return a new DStream by applying a RDD-to-RDD function to every RDD of the source DStream. This can be used to do arbitrary RDD operations on the DStream. |
# MAGIC | **updateStateByKey**(*func*)             | Return a new "state" DStream where the state for each key is updated by applying the given function on the previous state of the key and the new values for the key. This can be used to maintain arbitrary state data for each key. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### map
# MAGIC 
# MAGIC 是将DStream的数据一条一条处理，处理的逻辑是基于map算子中接收的处理函数的，返回新的DStream（TransformedDStream）。

# COMMAND ----------

from pyspark.streaming import StreamingContext
import time

ssc = StreamingContext(sc, 10)

# 定义一个队列
rddQueue = [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

# 往队列中写入数据
for i in range(5):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

lines = ssc.queueStream(rddQueue)

# DStream
print(type(lines))

words = lines.map(lambda x: (x % 10, 1))

# TransformedDStream
print(type(words))
words.pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### filter
# MAGIC 
# MAGIC 筛选满足条件的数据，返回新的DStream（TransformedDStream）。

# COMMAND ----------

from pyspark.streaming import StreamingContext
import time

ssc = StreamingContext(sc, 10)

# 定义一个队列
rddQueue = [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

# 往队列中写入数据
for i in range(5):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

lines = ssc.queueStream(rddQueue)

# DStream
print(type(lines))

words = lines.filter(lambda x: x % 5 == 0)

# TransformedDStream
print(type(words))
words.pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### reduceByKey
# MAGIC 
# MAGIC 针对K-V型DStream，自动按照K分组，然后根据提供的聚合逻辑，完成组内数据的聚合操作，返回新的DStream（TransformedDStream）。

# COMMAND ----------

from pyspark.streaming import StreamingContext
import time

ssc = StreamingContext(sc, 10)

# 定义一个队列
rddQueue = [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

# 往队列中写入数据
for i in range(5):
    rddQueue += [ssc.sparkContext.parallelize([j for j in range(1, 1001)], 10)]

lines = ssc.queueStream(rddQueue)

words = lines.map(lambda x: (x % 10, 1)).reduceByKey(lambda a,b: a + b)

words.pprint()

ssc.start()

ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### updateStateByKey
# MAGIC 
# MAGIC updateStateByKey操作允许您保持任意状态，同时不断使用新信息对其进行更新，这需要chekcpoint保存历史数据。要使用它，您必须执行两个步骤。
# MAGIC * 定义状态 - 状态可以是任意数据类型。
# MAGIC * 定义状态更新函数 - 使用函数指定如何使用以前的状态和输入流中的新值来更新状态。
# MAGIC 
# MAGIC 在每个批次中，Spark都会对所有现有key应用状态更新功能，无论它们是否在批处理中具有新数据。如果更新函数返回None，则键值对将被淘汰。

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    if len(newValues) == 2:
        return None
    return sum(newValues, runningCount)

ssc.checkpoint("/mnt/databrickscontainer1/checkpoint/")

lines = ssc.textFileStream("/mnt/databrickscontainer1/SparkStreaming")

# 按行读取文件、用空格拆分单词
words = lines.flatMap(lambda x: x.split(" "))
# 为每个单词计数为1
pairs = words.map(lambda x: (x, 1))
# 根据单词汇总单词的个数
counts = pairs.updateStateByKey(updateFunction)
# 打印最终的结果，得到WordCount
counts.pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### join
# MAGIC 
# MAGIC 对两个RDD执行JOIN操作（可实现SQL的内、外连接）。

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)

rdd = ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 11)])

# 定义一个队列
rddQueue1 = [rdd]

for i in range(2,10):
    rddQueue1 += [ssc.sparkContext.parallelize([("value%s" % (i * j),i * j) for j in range(1, 11)])]
    
rdd1 = ssc.queueStream(rddQueue1)

# 定义一个队列
rddQueue2 = [rdd]

for i in range(2,10):
    rddQueue2 += [ssc.sparkContext.parallelize([("value%s" % (i * j),i * j) for j in range(6, 16)])]
    
rdd2 = ssc.queueStream(rddQueue2)

# COMMAND ----------

# 单个流执行没有问题
rdd1.pprint()
rdd2.pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC #### inner join

# COMMAND ----------

rdd1.join(rdd2).pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### left outer join

# COMMAND ----------

rdd1.leftOuterJoin(rdd2).pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### right outer join

# COMMAND ----------

rdd1.rightOuterJoin(rdd2).pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### full outer join

# COMMAND ----------

rdd1.fullOuterJoin(rdd2).pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 存在一个小问题
# MAGIC 
# MAGIC DStream不能直接与普通的RDD进行Join

# COMMAND ----------

print(type(rdd))
print(type(rdd1))
print(type(rdd2))

# COMMAND ----------

# 'RDD' object has no attribute '_jdstream'
rdd1.join(rdd).pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### transform
# MAGIC 
# MAGIC transform操作允许在DStream上应用任意 RDD 到 RDD 的函数。它可以用于处理没有在DStream API公布的RDD操作，这提供了很大的灵活性。

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)

rdd = ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 11)])

# 定义一个队列
rddQueue1 = [rdd]

for i in range(2,10):
    rddQueue1 += [ssc.sparkContext.parallelize([("value%s" % (i * j),i * j) for j in range(1, 11)])]
    
rdd1 = ssc.queueStream(rddQueue1)

# 通过transform将DStream中的元素与RDD进行join
rdd1.transform(lambda rd1: rd1.fullOuterJoin(rdd)).pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### window
# MAGIC 
# MAGIC Spark Streaming还提供窗口计算，允许通过滑动窗口对数据进行转换。
# MAGIC 
# MAGIC ![](https://spark.apache.org/docs/3.2.1/img/streaming-dstream-window.png)
# MAGIC 
# MAGIC 每次窗口在源DStream上滑动时，落在窗口内的源RDD都会被组合并对其进行操作，以生成窗口DStream的RDD。
# MAGIC 
# MAGIC 任何窗口操作都需要指定两个参数。
# MAGIC 
# MAGIC * window length - 窗口的持续时间（图中为 3）。
# MAGIC * sliding interval - 执行窗口操作的间隔（图中为 2）。
# MAGIC 
# MAGIC >这两个参数必须是源DStream的批处理间隔（BatchDuration）的倍数
# MAGIC 
# MAGIC | Transformation                                               | Meaning                                                      |
# MAGIC | :----------------------------------------------------------- | :----------------------------------------------------------- |
# MAGIC | **window**(*windowLength*, *slideInterval*)                  | Return a new DStream which is computed based on windowed batches of the source DStream. |
# MAGIC | **countByWindow**(*windowLength*, *slideInterval*)           | Return a sliding window count of elements in the stream.     |
# MAGIC | **reduceByWindow**(*func*, *windowLength*, *slideInterval*)  | Return a new single-element stream, created by aggregating elements in the stream over a sliding interval using *func*. The function should be associative and commutative so that it can be computed correctly in parallel. |
# MAGIC | **reduceByKeyAndWindow**(*func*, *windowLength*, *slideInterval*, [*numTasks*]) | When called on a DStream of (K, V) pairs, returns a new DStream of (K, V) pairs where the values for each key are aggregated using the given reduce function *func* over batches in a sliding window. **Note:** By default, this uses Spark's default number of parallel tasks (2 for local mode, and in cluster mode the number is determined by the config property `spark.default.parallelism`) to do the grouping. You can pass an optional `numTasks` argument to set a different number of tasks. |
# MAGIC | **reduceByKeyAndWindow**(*func*, *invFunc*, *windowLength*, *slideInterval*, [*numTasks*]) | A more efficient version of the above `reduceByKeyAndWindow()` where the reduce value of each window is calculated incrementally using the reduce values of the previous window. This is done by reducing the new data that enters the sliding window, and “inverse reducing” the old data that leaves the window. An example would be that of “adding” and “subtracting” counts of keys as the window slides. However, it is applicable only to “invertible reduce functions”, that is, those reduce functions which have a corresponding “inverse reduce” function (taken as parameter *invFunc*). Like in `reduceByKeyAndWindow`, the number of reduce tasks is configurable through an optional argument. Note that [checkpointing](https://spark.apache.org/docs/3.2.1/streaming-programming-guide.html#checkpointing) must be enabled for using this operation. |
# MAGIC | **countByValueAndWindow**(*windowLength*, *slideInterval*, [*numTasks*]) | When called on a DStream of (K, V) pairs, returns a new DStream of (K, Long) pairs where the value of each key is its frequency within a sliding window. Like in `reduceByKeyAndWindow`, the number of reduce tasks is configurable through an optional argument. |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### countByWindow
# MAGIC 
# MAGIC 返回流中元素的滑动窗口中元素的个数。
# MAGIC 
# MAGIC 参数：
# MAGIC * windowLength  - 窗口的持续时间
# MAGIC * slideInterval - 执行窗口操作的间隔

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)
ssc.checkpoint("/mnt/databrickscontainer1/checkpoint/")

rdd = ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 11)])

# 定义一个队列
rddQueue1 = [rdd]

for i in range(2,10):
    rddQueue1 += [ssc.sparkContext.parallelize([("value%s" % (i * j),i * j) for j in range(1, 11)])]
    
rdd1 = ssc.queueStream(rddQueue1)

# rdd1.count().pprint()
# rdd1.countByWindow(Seconds(30), Seconds(10)).pprint()
rdd1.countByWindow(30, 10).pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### reduceByKeyAndWindow
# MAGIC 
# MAGIC 当在(K,V)型的DStream上调用时，返回一个新的(K,V)型的DStream，其中每个键的值在滑动窗口中使用给定的reduce函数func对批次进行聚合。
# MAGIC 
# MAGIC 几个重要的参数：
# MAGIC 
# MAGIC * func - 聚合函数
# MAGIC * invFunc - 反向函数（就是用于处理从上一个窗口到当前窗口的过程中从窗口中移除出去的数据的）
# MAGIC * windowLength  - 窗口的持续时间
# MAGIC * slideInterval - 执行窗口操作的间隔
# MAGIC * [numTasks]
# MAGIC 
# MAGIC > 注意：  
# MAGIC > 默认情况下，这使用Spark的默认并行任务数（本地模式为2，集群模式下由配置属性Spark.default.parallelism确定）进行分组。可以传递可选的numTasks参数来设置不同数量的任务。

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)
ssc.checkpoint("/mnt/databrickscontainer1/checkpoint/")

rdd = ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 11)])

# 定义一个队列
rddQueue1 = [rdd]

for i in range(2,10):
    rddQueue1 += [ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 11)])]
    
rdd1 = ssc.queueStream(rddQueue1)

# rdd1.reduceByKey(lambda a,b: a + b).pprint()

# rdd1.reduceByKeyAndWindow(lambda a, b: a + b, Seconds(30), Seconds(10)).pprint()
# rdd1.reduceByKeyAndWindow(lambda a, b: a + b, 30, 10).pprint()

# rdd1.reduceByKeyAndWindow(lambda a, b: a + b, lambda x, y: x - y, Seconds(30), Seconds(10)).pprint()
rdd1.reduceByKeyAndWindow(lambda a, b: a + b, lambda x, y: x - y, 30, 10).pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DStream上的输出操作
# MAGIC 
# MAGIC 输出操作允许将DStream的数据推送到外部系统，比如数据库或文件系统。由于输出操作实际上允许外部系统使用转换后的数据，因此它们会触发执行所有DStream转换（类似于RDD的Action）。
# MAGIC 
# MAGIC | Output Operation                            | Meaning                                                      |
# MAGIC | :------------------------------------------ | :----------------------------------------------------------- |
# MAGIC | **print**()                                 | Prints the first ten elements of every batch of data in a DStream on the driver node running the streaming application. This is useful for development and debugging. **Python API** This is called **pprint()** in the Python API. |
# MAGIC | **saveAsTextFiles**(*prefix*, [*suffix*])   | Save this DStream's contents as text files. The file name at each batch interval is generated based on *prefix* and *suffix*: *"prefix-TIME_IN_MS[.suffix]"*. |
# MAGIC | **saveAsObjectFiles**(*prefix*, [*suffix*]) | Save this DStream's contents as `SequenceFiles` of serialized Java objects. The file name at each batch interval is generated based on *prefix* and *suffix*: *"prefix-TIME_IN_MS[.suffix]"*. **Python API** This is not available in the Python API. |
# MAGIC | **saveAsHadoopFiles**(*prefix*, [*suffix*]) | Save this DStream's contents as Hadoop files. The file name at each batch interval is generated based on *prefix* and *suffix*: *"prefix-TIME_IN_MS[.suffix]"*. **Python API** This is not available in the Python API. |
# MAGIC | **foreachRDD**(*func*)                      | The most generic output operator that applies a function, *func*, to each RDD generated from the stream. This function should push the data in each RDD to an external system, such as saving the RDD to files, or writing it over the network to a database. Note that the function *func* is executed in the driver process running the streaming application, and will usually have RDD actions in it that will force the computation of the streaming RDDs. |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### print / pprint
# MAGIC 
# MAGIC 打印DStream的前10条数据。

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)
ssc.checkpoint("/mnt/databrickscontainer1/checkpoint/")

rdd = ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 11)])

# 定义一个队列
rddQueue1 = [rdd]

for i in range(2,10):
    rddQueue1 += [ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 101)])]
    
rdd1 = ssc.queueStream(rddQueue1)

rdd1.pprint()

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### saveAsTextFiles
# MAGIC 
# MAGIC 将DStream的数据保存到文本文件，每个批量间隔的数据保存一个独立的文件。

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)
ssc.checkpoint("/mnt/databrickscontainer1/checkpoint/")

rdd = ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 11)])

# 定义一个队列
rddQueue1 = [rdd]

for i in range(2,10):
    rddQueue1 += [ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 101)])]
    
rdd1 = ssc.queueStream(rddQueue1)

rdd1.saveAsTextFiles('/mnt/databrickscontainer1/SparkStreaming/output','dat')

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### foreachRDD
# MAGIC 
# MAGIC foreachRDD是一个功能强大的原语，允许将数据发送到外部系统。但是，了解如何正确有效地使用此基元非常重要。

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)
ssc.checkpoint("/mnt/databrickscontainer1/checkpoint/")

rdd = ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 11)])

# 定义一个队列
rddQueue1 = [rdd]

for i in range(2,10):
    rddQueue1 += [ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 101)])]
    
rdd1 = ssc.queueStream(rddQueue1)

rdd1.foreachRDD(lambda rdd: rdd.foreachPartition(lambda partition: print(list(partition))))

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 注意
# MAGIC 
# MAGIC 官方文档提到：了解如何正确有效地使用此基元非常重要。
# MAGIC 
# MAGIC > [Design Patterns for using foreachRDD](https://spark.apache.org/docs/3.2.1/streaming-programming-guide.html#design-patterns-for-using-foreachrdd)  
# MAGIC >  However, it is important to understand how to use this primitive correctly and efficiently.
# MAGIC 
# MAGIC 当需要将DStream的数据推送到外部系统时，比如关系型数据库，如果需要创建数据源连接：
# MAGIC * 不要在Driver端创建连接，因为可能出现序列化反序列化失败的问题
# MAGIC * 不要为每个RDD的数据创建一个连接，一方面会导致连接数过多，一方面创建连接的开销很大
# MAGIC * 尽量让一次连接处理一个分区的数据
# MAGIC * 可以使用共享连接池

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DStream上的SQL操作
# MAGIC 
# MAGIC 要想执行SQL操作，需要将DStream中的RDD注册成DataFrame/视图，然后才能进行SQL操作。

# COMMAND ----------

from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

ssc = StreamingContext(sc, 10)
ssc.checkpoint("/mnt/databrickscontainer1/checkpoint/")

rdd = ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 11)])

# 定义一个队列
rddQueue1 = [rdd]

for i in range(2,10):
    rddQueue1 += [ssc.sparkContext.parallelize([("value%s" % j, j) for j in range(1, 101)])]
    
rdd1 = ssc.queueStream(rddQueue1)

# SparkContext can only be used on the driver, not in code that it run on workers. For more information, see SPARK-5063.
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def processRDD(time, rdd):
    print("========= %s =========" % str(time))
    # 通过RDD创建DataFrame方式1
    # df = spark.createDataFrame(rdd, schema=["world","value"])
    # SparkContext can only be used on the driver, not in code that it run on workers. For more information, see SPARK-5063.
    
    # 通过RDD创建DataFrame方式3
    schema = StructType().add("world", StringType(), nullable=False).add("value", IntegerType(), nullable=False)
    df = rdd.toDF(schema)

    df.createOrReplaceTempView("dstream_table")
    # SparkContext can only be used on the driver, not in code that it run on workers. For more information, see SPARK-5063.
    sparki = getSparkSessionInstance(rdd.context.getConf())
    sparki.sql("select * from dstream_table").show()

rdd1.foreachRDD(processRDD)

ssc.start()
ssc.awaitTermination()

# COMMAND ----------

ssc.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DStream上的持久化
# MAGIC 
# MAGIC 在学习SparkCore的时候，我们知道RDD的数据是过程数据，在下一次要用到RDD的数据的时候，再根据血缘关系，从头重新处理一遍RDD的数据。RDD提供了`cache`、`persist`、`checkpoint`来进行数据的持久化。
# MAGIC 
# MAGIC 与RDD类似，DStream还允许开发人员将流的数据保存在内存中。
# MAGIC 
# MAGIC 对于基于窗口的操作（如reduceByWindow和reduceByKeyAndWindow）和基于状态的操作（如updateStateByKey），这是隐式的。因此，由基于窗口的操作生成的DStream会自动保留在内存中，而无需开发人员调用persist()。
# MAGIC 
# MAGIC > 与RDD不同，DStream的默认持久性级别将数据序列化在内存中。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### 检查点/Checkpoint
# MAGIC 
# MAGIC 流应用程序必须全天候运行，因此必须能够灵活应对与应用程序逻辑无关的故障（例如，系统故障、JVM 崩溃等）。为了实现这一点，Spark Streaming需要将足够多的信息检查点发送到容错存储系统，以便它可以从故障中恢复。
# MAGIC 
# MAGIC 有两种类型的数据是检查点的。
# MAGIC 
# MAGIC * 元数据检查点 - 将定义流计算的信息保存到HDFS等容错存储中。这用于从运行流式处理应用程序驱动程序的节点的故障中恢复。元数据包括：
# MAGIC   * 配置 - 用于创建流式处理应用程序的配置。
# MAGIC   * DStream操作 - 定义流式处理应用程序的DStream操作集。
# MAGIC   * 未完成的批次 - 作业已排队但尚未完成的批次。
# MAGIC * 数据检查点 - 将生成的RDD保存到可靠的存储。在某些跨多个批次合并数据的有状态转换中，这是必需的。在此类转换中，生成的RDD依赖于先前批次的RDD，这会导致依赖关系链的长度随着时间的推移而不断增加。为了避免恢复时间的这种无限增加（与依赖关系链成正比），有状态转换的中间RDD定期检查点到可靠存储（例如HDFS）以切断依赖链。
# MAGIC 
# MAGIC 总而言之，元数据检查点主要用于从驱动程序故障中恢复，而数据或RDD检查点对于基本功能（如果使用有状态转换）也是必需的。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 何时启用检查点
# MAGIC 必须为具有以下任何要求的应用程序启用检查点：
# MAGIC 
# MAGIC * 有状态转换的使用 - 如果在应用程序中使用了updateStateByKey或reduceByKeyAndWindow（具有反函数），则必须提供检查点目录以允许定期RDD检查点。
# MAGIC * 从运行应用程序的驱动程序的故障中恢复 - 元数据检查点用于恢复进度信息。
# MAGIC 
# MAGIC > 请注意，无需启用检查点即可运行没有上述有状态转换的简单流式处理应用程序。在这种情况下，从驱动程序故障中恢复也将是部分的（某些已接收但未处理的数据可能会丢失）。这通常是可以接受的，许多人以这种方式运行Spark Streaming应用程序。对非Hadoop环境的支持有望在未来得到改善。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 如何配置检查点
# MAGIC 
# MAGIC 可以通过在容错、可靠的文件系统（例如，HDFS、S3等）中设置一个目录来启用检查点操作，检查点信息将保存到该目录。这是通过使用streamingContext.checkpoint(checkpointDirectory)来完成的。这将允许您使用上述有状态转换。此外，如果要使应用程序从驱动程序故障中恢复，则应重写流式处理应用程序以具有以下行为。
# MAGIC 
# MAGIC * 当程序首次启动时，它将创建一个新的StreamingContext，设置所有流，然后调用start()。
# MAGIC * 当程序在失败后重新启动时，它将从检查点目录中的检查点数据重新创建流文本。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 累加器、广播变量和检查点
# MAGIC 
# MAGIC 累加器和广播变量无法从 Spark 流式处理中的检查点恢复。如果启用检查点并同时使用累加器或广播变量，则必须为累加器和广播变量创建延迟实例化的单一实例，以便在驱动程序失败时重新启动后可以重新实例化它们。
# MAGIC 
# MAGIC 请参阅完整的[源代码](https://github.com/apache/spark/blob/v3.2.1/examples/src/main/scala/org/apache/spark/examples/streaming/RecoverableNetworkWordCount.scala)。
