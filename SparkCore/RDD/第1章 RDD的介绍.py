# Databricks notebook source
# MAGIC %md
# MAGIC # 第1章 RDD的介绍

# COMMAND ----------

# MAGIC %md
# MAGIC 环境初始化
# MAGIC > 首先执行环境的初始化。  
# MAGIC > 将存储账户与Spark环境关联，以便于在Spark程序中可以使用存储。  
# MAGIC > `dfs_endpoint` 是文件系统的根端点。  

# COMMAND ----------

# MAGIC %run "../../initialization"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 为什么需要RDD
# MAGIC 分布式计算需要
# MAGIC * 分区控制
# MAGIC * Shuffle控制
# MAGIC * 数据存储\序列化\发送
# MAGIC * 数据计算API
# MAGIC * 等等
# MAGIC 
# MAGIC 我们需要一个统一的数据抽象来实现上述分布式计算所需的功能，这个数据抽象就是RDD。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 什么是RDD
# MAGIC RDD(Resilient Distributed Dataset)叫做弹性分布式数据集，是Spark中最基本的数据抽象，代表一个不可变、可分区、元素可并行计算的集合。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 RDD的五大特性
# MAGIC * A list of partitions.(RDD是有分区的)
# MAGIC * A function for computing each split.(计算方法都会作用到每个分区上)
# MAGIC * A list of dependencies on other RDDs.(RDD之间是有依赖关系的)
# MAGIC * Optionally, a Partitioner for key-value RDDs.(KV型的RDD可以有分区器)
# MAGIC * Optionally, a list of preferred locations to compute each split on.(RDD分区数据的读取会尽量靠近数据所在地)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 特性1 RDD是有分区的
# MAGIC 分区是RDD数据存储的最小单位。  
# MAGIC 一份RDD的数据，本质上是分割成了多个分区。

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],5)
print("RDD的分区数：",rdd.getNumPartitions())
print("RDD的分区情况：",rdd.glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 特性2 计算方法都会作用到每个分区上
# MAGIC 以下代码中的 x * 10 会作用到每个分区的每个元素上。

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3).map(lambda x: x * 10)
rdd.glom().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 特性3 RDD之间是有依赖关系的
# MAGIC 如下代码之间是有依赖关系的  
# MAGIC `textFile`->`rdd1`->`rdd2`->`rdd3`->`rdd4`

# COMMAND ----------

rdd1 = sc.textFile(dfs_endpoint + "/input/WordCount.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
rdd4.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 特性4 Key-Value型的RDD可以有分区器
# MAGIC Key-Value RDD：RDD中存储的是二元组，比如：("spark", 3.2)。  
# MAGIC 默认分区器：Hash分区规则。  
# MAGIC 可以手动设置分区规则（rdd.partitionBy的方法来设置）。  
# MAGIC 由于不是所有RDD都是Key-Value型的，所以这个特性是可选的。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 特性5 RDD的分区规划，会尽量靠近数据所在的服务器
# MAGIC 因为这样可以走`本地读取`，避免`网络读取`。  
# MAGIC > Spark会在`确保并行计算能力的前提下`，尽量确保本地读取。  
# MAGIC > 这里是尽量确保，而不是100%确保。  
# MAGIC > 所以这个特性也是可选的。
