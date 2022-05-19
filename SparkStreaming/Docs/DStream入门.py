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
# MAGIC * 可以监视一个简单的目录，例如 "hdfs://namenode:8040/logs/"。直接位于此类路径下的所有文件都将在被发现时进行处理。
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

# COMMAND ----------

from pyspark.streaming import StreamingContext

ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream("localhost", 5555)

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
# MAGIC 需要为集群安装库：`org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.1`和`org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1`

# COMMAND ----------

from pyspark.streaming.kafka import KafkaUtils

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
# MAGIC ### updateStateByKey
# MAGIC 
# MAGIC updateStateByKey操作允许您保持任意状态，同时不断使用新信息对其进行更新。要使用它，您必须执行两个步骤。
# MAGIC * 定义状态 - 状态可以是任意数据类型。
# MAGIC * 定义状态更新函数 - 使用函数指定如何使用以前的状态和输入流中的新值来更新状态。
# MAGIC 
# MAGIC 在每个批次中，Spark都会对所有现有key应用状态更新功能，无论它们是否在批处理中具有新数据。如果更新函数返回None，则键值对将被淘汰。

# COMMAND ----------

from pyspark.streaming import StreamingContext

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

ssc = StreamingContext(sc, 10)
ssc.checkpoint("/mnt/databrickscontainer1/SparkStreaming/checkpoint/")

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
