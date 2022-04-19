# Databricks notebook source
# MAGIC %md
# MAGIC # RDD的介绍

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
# MAGIC ## 为什么需要RDD
# MAGIC 分布式计算需要
# MAGIC * 分区控制
# MAGIC * Shuffle控制
# MAGIC * 数据存储\序列化\发送
# MAGIC * 数据计算API
# MAGIC * 等等
# MAGIC 
# MAGIC 这些功能，不能简单的通过Python内置的本地集合对象(如 List)去完成。
# MAGIC 
# MAGIC 我们在分布式框架中，我们需要一个统一的数据抽象来实现上述分布式计算所需的功能，这个数据抽象就是RDD。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 什么是RDD
# MAGIC RDD(Resilient Distributed Dataset)叫做弹性分布式数据集，是Spark中最基本的数据抽象，代表一个不可变、可分区、元素可并行计算的集合。
# MAGIC 
# MAGIC * Dataset：一个数据集合，用于存放数据的。
# MAGIC * Distributed：RDD中的数据是分布式存储的，可用于分布式计算。
# MAGIC * Resilient：RDD中的数据可以存储在内存中或者磁盘中。
# MAGIC 
# MAGIC RDD
# MAGIC * 不可变：immutable
# MAGIC   * 不可变集合
# MAGIC   * 变量的声明使用val
# MAGIC * 可分区：partitioned
# MAGIC   * 集合的数据被划分为很多部分
# MAGIC   * 每部分称为分区 partition
# MAGIC * 并行计算：parallel
# MAGIC   * 集合中的数据可以被并行的计算处理
# MAGIC   * 每个分区数据被一个Task任务处理
# MAGIC 
# MAGIC 所有的运算以及操作都建立在RDD数据结构的基础之上。
# MAGIC 
# MAGIC 可以认为RDD是分布式的列表List或数组Array，抽象的数据结构，RDD是一个抽象类Abstract Class和泛型Generic Type。

# COMMAND ----------

# MAGIC %scala
# MAGIC /**
# MAGIC  * Internally, each RDD is characterized by five main properties:
# MAGIC  *
# MAGIC  *  - A list of partitions
# MAGIC  *  - A function for computing each split
# MAGIC  *  - A list of dependencies on other RDDs
# MAGIC  *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
# MAGIC  *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
# MAGIC  *    an HDFS file)
# MAGIC  */
# MAGIC abstract class RDD[T: ClassTag](
# MAGIC     @transient private var _sc: SparkContext,
# MAGIC     @transient private var deps: Seq[Dependency[_]]
# MAGIC   ) extends Serializable with Logging {
# MAGIC   // TODO ...
# MAGIC }

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD的五大特性
# MAGIC 
# MAGIC RDD 数据结构内部有五个特性（摘录RDD 源码）：前三个特征每个RDD都具备的，后两个特征可选的。
# MAGIC 
# MAGIC Internally, each RDD is characterized by five main properties:
# MAGIC 
# MAGIC * A list of partitions.(RDD是有分区的)
# MAGIC * A function for computing each split.(计算方法都会作用到每个分区上)
# MAGIC * A list of dependencies on other RDDs.(RDD之间是有依赖关系的)
# MAGIC * Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned).(KV型的RDD可以有分区器)
# MAGIC * Optionally, a list of preferred locations to compute each split on (e.g. block locations for an HDFS file).(RDD分区数据的读取会尽量靠近数据所在地)

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD是有分区的
# MAGIC 
# MAGIC - A list of partitions
# MAGIC 
# MAGIC 分区是RDD数据存储的最小单位。  
# MAGIC 一份RDD的数据，本质上是分割成了多个分区。

# COMMAND ----------

rdd = sc.parallelize([0, 1,2,3,4,5,6,7,8,9],5)
print("RDD的分区数：",rdd.getNumPartitions())
print("RDD的分区情况：",rdd.glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 计算方法都会作用到每个分区上
# MAGIC 
# MAGIC - A function for computing each split
# MAGIC 
# MAGIC 以下代码中的 x * 10 会作用到每个分区的每个元素上。

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3).map(lambda x: x * 10)
rdd.glom().collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD之间是有依赖关系的
# MAGIC 
# MAGIC - A list of dependencies on other RDDs
# MAGIC 
# MAGIC 如下代码之间是有依赖关系的  
# MAGIC `textFile`->`rdd1`->`rdd2`->`rdd3`->`rdd4`

# COMMAND ----------

rdd1 = sc.textFile(dfs_endpoint + "/Words.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x, 1))
rdd4 = rdd3.reduceByKey(lambda a, b: a + b)
rdd4.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key-Value型的RDD可以有分区器
# MAGIC 
# MAGIC - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
# MAGIC 
# MAGIC Key-Value RDD：RDD中存储的是二元组，比如：("spark", 3.2)。  
# MAGIC 默认分区器：Hash分区规则。  
# MAGIC 可以手动设置分区规则（rdd.partitionBy的方法来设置）。  
# MAGIC 由于不是所有RDD都是Key-Value型的，所以这个特性是可选的。

# COMMAND ----------

rdd = sc.parallelize([0,1,2,3,4,5,6,7,8,9],3).map(lambda x: (x, x % 4))
print(rdd.glom().collect(), rdd.partitioner)

rdd2 = rdd.partitionBy(3)
print(rdd2.glom().collect(), rdd2.partitioner)

rdd3 = rdd.partitionBy(3, lambda x: x % 3)
print(rdd3.glom().collect(), rdd3.partitioner)

rdd4 = rdd.partitionBy(3, lambda x: x % 2)
print(rdd4.glom().collect(), rdd4.partitioner)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.HashPartitioner
# MAGIC import org.apache.spark.RangePartitioner
# MAGIC 
# MAGIC val rdd = sc.parallelize(List(0,1,2,3,4,5,6,7,8,9),3).map(x => (x, x))
# MAGIC rdd.glom().collect().foreach(x => println(x.mkString(",")))
# MAGIC println(rdd.partitioner)
# MAGIC 
# MAGIC val rdd2 = rdd.partitionBy(new HashPartitioner(4))
# MAGIC rdd2.glom().collect().foreach(x => println(x.mkString(",")))
# MAGIC println(rdd2.partitioner)
# MAGIC 
# MAGIC val rdd3 = rdd.partitionBy(new RangePartitioner(4, rdd))
# MAGIC rdd3.glom().collect().foreach(x => println(x.mkString(",")))
# MAGIC println(rdd3.partitioner)

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD的分区规划，会尽量靠近数据所在的服务器
# MAGIC 
# MAGIC - Optionally, a list of preferred locations to compute each split on (e.g. block locations for an HDFS file)
# MAGIC 
# MAGIC 在初始RDD（读取数据的时候）规划的时候，分区会尽量规划到存储数据所在的服务器上。
# MAGIC 
# MAGIC 因为这样可以走`本地读取`，避免`网络读取`。
# MAGIC 
# MAGIC 本地读取：Executor所在的服务器，同样是一个DataNode，同时这个DataNode上有他要读取的数据，所以可以直接读取机器硬盘即可，无需网络传输。
# MAGIC 
# MAGIC 网络读取：读取数据，需要经过网络的传输才能读取到。
# MAGIC 
# MAGIC 本地读取的性能远大于网络读取的性能。
# MAGIC 
# MAGIC > Spark会在`确保并行计算能力的前提下`，尽量确保本地读取。  
# MAGIC > 这里是尽量确保，而不是100%确保。  
# MAGIC > 所以这个特性也是可选的。
