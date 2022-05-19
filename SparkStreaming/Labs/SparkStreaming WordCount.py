# Databricks notebook source
# MAGIC %md
# MAGIC # SparkStreaming WordCount
# MAGIC 
# MAGIC 在这里，我们监控一个指定的路径，用SparkStreaming读取路径下的文件，并统计单词个数。

# COMMAND ----------

from pyspark.streaming import StreamingContext

# 通过SparkContext对象，构建StreamingContext对象，每5秒钟执行一次
ssc = StreamingContext(sc, 5)

# SparkContext对象，SparkCore的入口
# SparkSession对象，SparkSQL的入口
# StreamingContext对象，Spark Streaming的入口
ssc

# COMMAND ----------

# 监听指定路径下的文件，读取一个时间窗口内发生变化的（新增、修改）文件进行处理
lines = ssc.textFileStream("/mnt/databrickscontainer1/SparkStreaming")

# 按行读取文件、用空格拆分单词
words = lines.flatMap(lambda x: x.split(" "))
# 为每个单词计数为1
pairs = words.map(lambda x: (x, 1))
# 根据单词汇总单词的个数
counts = pairs.reduceByKey(lambda a,b: a + b)
# 打印最终的结果，得到WordCount
counts.pprint()

# lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a,b: a + b).pprint()

# 启动流处理
ssc.start()
# 阻塞当前线程，让Streaming程序一直执行
ssc.awaitTermination()

# COMMAND ----------

# 停止流处理程序
ssc.stop()
