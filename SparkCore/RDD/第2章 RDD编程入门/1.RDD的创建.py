# Databricks notebook source
# MAGIC %md
# MAGIC # RDD的创建
# MAGIC 可以通过以下几种方式创建RDD：  
# MAGIC * 并行化创建
# MAGIC * 读取文件创建

# COMMAND ----------

# MAGIC %md
# MAGIC 环境初始化
# MAGIC > 首先执行环境的初始化。  
# MAGIC > 将存储账户与Spark环境关联，以便于在Spark程序中可以使用存储。  
# MAGIC > `dfs_endpoint` 是文件系统的根端点。  

# COMMAND ----------

# MAGIC %run "../../../initialization"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 并行化创建
# MAGIC 并行化创建，是指将本地集合转换为分布式RDD  
# MAGIC 并行化创建RDD，使用`parallelize`

# COMMAND ----------

list = [1,2,3,4,5,6,7,8,9]
print(type(list))
print("本地集合：", list)

# COMMAND ----------

# MAGIC %md
# MAGIC 以下代码通过将本地集合并行化，转换成分布式RDD。

# COMMAND ----------

rdd = sc.parallelize(list, 4)
print(type(rdd))
print("分布式RDD：", rdd)
print("分布式RDD：", rdd.collect())
print("RDD的分区数：", rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 读取文件创建
# MAGIC 读取文件创建，可以使用以下几个API  
# MAGIC * textFile
# MAGIC * wholeTextFile

# COMMAND ----------

# MAGIC %md
# MAGIC ### textFile
# MAGIC `textFile`可以读取本地文件，也可以读取分布式文件系统上的文件，比如：`HDFS`、`S3`。

# COMMAND ----------

# 直接读取 Azure Blob 存储容器中的文件
rdd = sc.textFile("%s/input/WordCount.txt" % dfs_endpoint)
rdd.collect()

# COMMAND ----------

# 读取Databricks文件系统(DBFS)上的文件
rdd = sc.textFile("dbfs:/mnt/databrickscontainer01/input/WordCount.txt")
rdd.collect()

# COMMAND ----------

# Databricks文件系统(DBFS)
# 可以像访问本地文件一样直接使用绝对路径进行访问
rdd = sc.textFile("/mnt/databrickscontainer01/input/WordCount.txt")
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### wholeTextFile
# MAGIC 与`textFile`功能一致，不过`wholeTextFile`更适合读取很多小文件的场景。

# COMMAND ----------

rdd = sc.textFile("/mnt/databrickscontainer01/input")
rdd.collect()
