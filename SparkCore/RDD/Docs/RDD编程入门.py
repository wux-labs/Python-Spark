# Databricks notebook source
# MAGIC %md
# MAGIC # RDD编程入门

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
# MAGIC ## SparkContext对象
# MAGIC 
# MAGIC Spark RDD 编程的程序入口对象是SparkContext对象(不论何种编程语言)。
# MAGIC 
# MAGIC 只有构建出SparkContext，基于它才能执行后续的API调用和计算。
# MAGIC 
# MAGIC 本质上，SparkContext对编程来说，主要功能就是创建第一个RDD出来。

# COMMAND ----------

sc

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD的创建
# MAGIC 
# MAGIC 可以通过以下几种方式创建RDD：
# MAGIC * 并行化创建
# MAGIC * 读取文件创建

# COMMAND ----------

# MAGIC %md
# MAGIC ### 并行化创建
# MAGIC 
# MAGIC 并行化创建，是指将本地集合转换为分布式RDD。
# MAGIC 
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
# MAGIC ### 读取文件创建
# MAGIC 
# MAGIC 读取文件创建，可以使用以下几个API  
# MAGIC * textFile
# MAGIC * wholeTextFiles

# COMMAND ----------

# MAGIC %md
# MAGIC #### textFile
# MAGIC 
# MAGIC `textFile`可以读取本地文件，也可以读取分布式文件系统上的文件，比如：`HDFS`、`S3`。

# COMMAND ----------

# 读取分布式文件系统上的文件
rdd = sc.textFile("%s/word.txt" % dfs_endpoint)

print(type(rdd))
print(rdd)
print(rdd.collect())
print(rdd.getNumPartitions())

# COMMAND ----------

# 读取Databricks文件系统(DBFS)上的文件
rdd = sc.textFile("dbfs:/mnt/databrickscontainer1/word.txt")

print(type(rdd))
print(rdd)
print(rdd.collect())
print(rdd.getNumPartitions())

# COMMAND ----------

# 读取Databricks文件系统(DBFS)上的文件
# 可以像访问本地文件一样直接使用绝对路径进行访问
rdd = sc.textFile("/mnt/databrickscontainer1/word.txt")

print(type(rdd))
print(rdd)
print(rdd.collect())
print(rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC #### wholeTextFiles
# MAGIC 
# MAGIC 与`textFile`功能一致，不过`wholeTextFiles`更适合读取很多小文件的场景。

# COMMAND ----------

rdd = sc.wholeTextFiles("/mnt/databrickscontainer1")

print(type(rdd))
print(rdd)
print(rdd.collect())
print(rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD算子

# COMMAND ----------

# MAGIC %md
# MAGIC ### 什么是算子
# MAGIC 
# MAGIC 分布式集合对象上的API称之为**算子**。
# MAGIC 
# MAGIC 本地对象上的API叫做**函数**/**方法**。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 算子的分类
# MAGIC 
# MAGIC RDD的算子分成两类：
# MAGIC * Transformation：转换算子
# MAGIC * Action：动作算子
# MAGIC 
# MAGIC **Transformation算子：**
# MAGIC 
# MAGIC 定义：**返回值是一个RDD**的算子，称为Transformation算子。
# MAGIC 
# MAGIC 特性：这类算子是懒加载（lazy）的，如果没有Action算子，Transformation算子是不工作的。
# MAGIC 
# MAGIC **Action算子：**
# MAGIC 
# MAGIC 定义：**返回值不是RDD**的算子，称为Action算子。
# MAGIC 
# MAGIC > 对于这两类算子来说，Transformation算子相当于在构建执行计划，Action算子是一个指令让这个执行计划开始工作。  
# MAGIC 如果没有Action，Transformation算子之间的迭代关系仅仅是一个执行计划，不会执行，只有Action到来，才会按照执行计划开始工作。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 常用的Transformation算子

# COMMAND ----------

# MAGIC %md
# MAGIC #### map 算子
# MAGIC 
# MAGIC map算子，是将RDD的数据一条一条处理，处理的逻辑是基于map算子中接收的处理函数的，返回新的RDD。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.map(func)
# MAGIC ```
# MAGIC > func: f:(T) -> U  
# MAGIC f: 表示这是一个函数/方法  
# MAGIC (T) -> U 表示的是方法定义  
# MAGIC (T) 表示参数列表  
# MAGIC U 表示返回值

# COMMAND ----------

rdd = sc.parallelize([0,1,2,3,4,5,6,7,8,9])

# 定义func
def map_func(data):
    return data * 5

# rdd.map(func)
print(rdd.map(map_func).collect())

# 可以直接用匿名函数的方式，lambda表达式
print(rdd.map(lambda x: x + 5).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### flatMap 算子
# MAGIC 
# MAGIC flatMap算子，是对RDD执行map操作，然后进行解除嵌套操作，返回新的RDD。
# MAGIC 
# MAGIC > **解除嵌套：**  
# MAGIC 嵌套的数据：  
# MAGIC lst = [[1,2,3],[4,5,6],[7,8,9]]  
# MAGIC 解除嵌套的数据：  
# MAGIC lst = [1,2,3,4,5,6,7,8,9]
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.flatMap(func)
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize(["1 2 3","4 5 6","7 8 9"])
# rdd有3个元素，map操作后还是3个嵌套元素
print(rdd.map(lambda x: x.split(" ")).collect())
# rdd有3个元素，flatMap操作后是无嵌套元素
print(rdd.flatMap(lambda x: x.split(" ")).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### mapValues 算子
# MAGIC 
# MAGIC mapValues算子，**针对K-V型RDD**，对二元组的Value执行map操作。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.mapValues(func)
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 2), ("b", 2), ("a", 3)])
print(rdd.collect())
print(rdd.mapValues(lambda x: x * 2).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### reduceByKey 算子
# MAGIC 
# MAGIC reduceByKey算子，**针对K-V型RDD**，自动按照K分组，然后根据提供的聚合逻辑，完成组内数据的聚合操作。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.reduceByKey(func)
# MAGIC ```
# MAGIC 
# MAGIC > func: (V, V) -> V  
# MAGIC func 函数只负责处理聚合逻辑，不负责分组

# COMMAND ----------

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 2), ("b", 2), ("a", 3)])
print(rdd.reduceByKey(lambda a,b: a + b).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### WordCount案例回顾
# MAGIC 
# MAGIC 在我们的WordCount案例中，使用到以下知识点：
# MAGIC * 程序入口对象是SparkContext对象，sc，主要功能就是创建第一个RDD出来
# MAGIC   * wordsRDD = sc.textFile("/mnt/databrickscontainer1/word.txt")
# MAGIC * 通过读取文件创建RDD，textFile
# MAGIC   * wordsRDD = sc.textFile("/mnt/databrickscontainer1/word.txt")
# MAGIC * RDD的特性
# MAGIC   * wordsRDD -> flatMapRDD -> mapRDD -> resultRDD
# MAGIC * 三个Transformation算子
# MAGIC   * flatMap
# MAGIC   * map
# MAGIC   * reduceByKey

# COMMAND ----------

# 第一步、读取本地数据 封装到RDD集合，认为列表List
wordsRDD = sc.textFile("/mnt/databrickscontainer1/word.txt")
# 第二步、处理数据 调用RDD中函数，认为调用列表中的函数
# a. 每行数据分割为单词
flatMapRDD = wordsRDD.flatMap(lambda line: line.split(" "))
# b. 转换为二元组，表示每个单词出现一次
mapRDD = flatMapRDD.map(lambda x: (x, 1))
# c. 按照Key分组聚合
resultRDD = mapRDD.reduceByKey(lambda a, b: a + b)
# 第三步、输出数据
print(resultRDD.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### glom 算子
# MAGIC 
# MAGIC glom算子，将RDD的数据，按照数据的分区添加嵌套。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.glom()
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize([0,1,2,3,4,5,6,7,8,9],3)

print(rdd.collect())
print(rdd.glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### groupBy 算子
# MAGIC 
# MAGIC groupBy算子，将RDD的数据进行分组。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.groupBy(func)
# MAGIC 
# MAGIC # func 函数
# MAGIC # 函数要求传入一个参数，返回一个返回值
# MAGIC # groupBy根据函数的返回值，将具有相同返回值的元素放入同一个组中
# MAGIC # 分组完成后的RDD的每个元素都是一个二元组，key是返回值，value是具有相同返回值的原RDD的元素
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize([0,1,2,3,4,5,6,7,8,9])
# ResultIterable
print(rdd.groupBy(lambda x: "group {}".format(x % 3)).collect())
print(rdd.groupBy(lambda x: "group {}".format(x % 3)).map(lambda x: (x[0], list(x[1]))).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### groupByKey 算子
# MAGIC 
# MAGIC groupByKey算子，**针对K-V型RDD**，自动按照K分组。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.groupByKey()
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize([("a", 1),("b", 1), ("a", 2), ("b", 2), ("a", 3)])

print(rdd.groupByKey().collect())
print(rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### filter 算子
# MAGIC 
# MAGIC filter算子，筛选满足条件的数据。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.filter(func)
# MAGIC 
# MAGIC # func 函数
# MAGIC # 函数要求传入一个参数，返回布尔类型的返回值
# MAGIC ```
# MAGIC 
# MAGIC > 当函数返回True，则记录被保留，当函数返回False，则记录被丢弃

# COMMAND ----------

rdd = sc.parallelize([0,1,2,3,4,5,6,7,8,9])
print(rdd.filter(lambda x: x > 5).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### distinct 算子
# MAGIC 
# MAGIC distinct算子，对RDD的数据进行去重。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.distinct(参数)
# MAGIC 
# MAGIC # 参数：去重的分区数，一般不用传
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize([0,1,2,0,1,2,0,2,2,0], 5)
print(rdd.glom().collect())
print(rdd.distinct().collect())
print(rdd.distinct(5).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### union 算子
# MAGIC 
# MAGIC union算子，将两个RDD合并成1个RDD。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.union(other)
# MAGIC 
# MAGIC # other，另一个需要被合并的RDD
# MAGIC ```
# MAGIC 
# MAGIC > union 只会合并，不会去重

# COMMAND ----------

rdd = sc.parallelize([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
rdd2 = sc.parallelize([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
print(rdd.union(rdd2).collect())
print(rdd.union(rdd.map(lambda x: (x, x))).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### join 算子
# MAGIC 
# MAGIC join算子，对两个RDD执行JOIN操作（可实现SQL的内、外连接）。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.join(other)
# MAGIC rdd.leftOuterJoin(other)
# MAGIC rdd.rightOuterJoin(other)
# MAGIC ```
# MAGIC 
# MAGIC > join算子只能用于二元元组

# COMMAND ----------

rdd1 = sc.parallelize([(101,"A"), (102, "B"), (103, "C")])
rdd2 = sc.parallelize([(102, 90), (104, 95)])

# join是内连接，保留两个RDD都有的数据
print(rdd1.join(rdd2).collect())

# leftOuterJoin是左外连接
print(rdd1.leftOuterJoin(rdd2).collect())

# rightOuterJoin是右外连接
print(rdd1.rightOuterJoin(rdd2).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### intersection 算子
# MAGIC 
# MAGIC intersection算子，求两个RDD的交集，即同时存在的元素。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.intersection(other)
# MAGIC ```

# COMMAND ----------

rdd1 = sc.parallelize([(101, "A"), (102, "B"), (103, "C"), "A", 2])
rdd2 = sc.parallelize([(102, 90), (104, 95), (103, "C"), "A", 1, 2])

print(rdd1.intersection(rdd2).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### sortBy 算子
# MAGIC 
# MAGIC sortBy算子，对RDD的数据进行排序，基于给定的排序依据。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.sortBy(func, ascending=False, numPartitions=1)
# MAGIC 
# MAGIC # func 函数
# MAGIC # 函数要求传入一个参数，返回一个返回值
# MAGIC # sortBy根据函数的返回值，将RDD的元素进行排序
# MAGIC # ascending：True升序，Flase降序
# MAGIC # numPartitions：排序的分区数
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize([0,1,2,3,4,5,6,7,8,9],3)

print(rdd.sortBy(lambda x: x).collect())
print(rdd.sortBy(lambda x: x, False).collect())
print(rdd.sortBy(lambda x: x % 3, False).collect())
print(rdd.sortBy(lambda x: x % 3, False, 3).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### sortByKey 算子
# MAGIC 
# MAGIC sortByKey算子，**针对K-V型RDD**，按照K对RDD的数据进行排序。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.sortByKey(ascending=True, numPartitions=None, func)
# MAGIC 
# MAGIC # ascending：True升序，Flase降序
# MAGIC # numPartitions：排序的分区数
# MAGIC # func 函数
# MAGIC # 函数要求传入一个参数，返回一个返回值
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 3), ("c", 4), ("b", 2), ("a", 2)])

print(rdd.sortByKey().collect())
print(rdd.sortByKey(False).collect())
print(rdd.sortByKey(False, 2).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### zip 算子
# MAGIC 
# MAGIC zip算子，将两个RDD的元素一一对应地组成一个二元组的RDD，就像拉链一样。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.zip(other)
# MAGIC 
# MAGIC # other：需要做拉链的另一个RDD
# MAGIC ```
# MAGIC 
# MAGIC > Can only zip with RDD which has the same number of partitions  
# MAGIC Can only zip RDDs with same number of elements in each partition  
# MAGIC 需要确保做zip的两个RDD的元素个数相同。  
# MAGIC 即便是用Scala编写的Spark代码也要注意，确保两个RDD的元素个数相同，这一点与Scala的List的zip不同。

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4])

# Can only zip with RDD which has the same number of partitions
# print(rdd.zip(sc.parallelize(["a","b","c","d"],3)).collect())

# Can only zip RDDs with same number of elements in each partition
# print(rdd.zip(sc.parallelize(["a","b","c"])).collect())
# print(rdd.zip(sc.parallelize(["a","b","c","d","e"])).collect())

print(rdd.zip(sc.parallelize(["a","b","c","d"])).collect())

# COMMAND ----------

# MAGIC %scala
# MAGIC val list1 = List(1,2,3,4)
# MAGIC val list2 = List("a","b","c")
# MAGIC val list3 = List("a","b","c","d","e")
# MAGIC println(list1.zip(list2))
# MAGIC println(list1.zip(list3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### zipWithIndex 算子
# MAGIC 
# MAGIC zipWithIndex算子，将RDD的元素与每个元素对应的索引进行zip。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.zipWithIndex()
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize(["a","b","c","d"])

print(rdd.zipWithIndex().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### zipWithUniqueId 算子
# MAGIC 
# MAGIC zipWithUniqueId算子，将RDD的元素与不重复的ID进行zip。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.zipWithUniqueId()
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize(["a","b","c","d","e","f"])

print(rdd.zipWithUniqueId().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### partitionBy 算子
# MAGIC 
# MAGIC partitionBy算子，**针对K-V型RDD**，根据K对RDD进行自定义分区操作。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.partitionBy(numPartitions, partitionFunc)
# MAGIC 
# MAGIC # numPartitions：重新分区后的分区数
# MAGIC # partitionFunc：自定义分区规则，返回的是整数类型的分区编号，取值范围: [0, numPartitions - 1]，返回值不在这个取值范围的，默认都放0分区
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize(["a","b","c","d","e","f"]).zipWithIndex()

print(rdd.glom().collect())

def partition_func(key):
    if key <= "c": return 1
    if key <= "e": return 2
    return 0

print(rdd.partitionBy(3,partition_func).glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### repartition 算子
# MAGIC 
# MAGIC repartition算子，对RDD的分区执行重新分区。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.repartition(numPartitions)
# MAGIC 
# MAGIC # numPartitions：重新分区后的分区数
# MAGIC ```
# MAGIC 
# MAGIC > 重分区采用的是：合并小分区、增加空分区的方式进行操作的。
# MAGIC 
# MAGIC > **注意：**  对分区的数量进行操作一定要慎重。  
# MAGIC > 一般情况下，除了要求全局排序设置为1个分区外，多数时候我们都不需要重新分区。  
# MAGIC > 如果改变了分区，可能会影响**并行计算**，还可能导致shuffle。

# COMMAND ----------

rdd = sc.parallelize(["a","b","c","d","e","f"])

print(rdd.glom().collect())
print(rdd.repartition(2).glom().collect())
print(rdd.repartition(9).glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### coalesce 算子
# MAGIC 
# MAGIC coalesce算子，对分区数量进行增减。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.coalesce(numPartitions, shuffle=False)
# MAGIC 
# MAGIC # numPartitions：重新分区后的分区数
# MAGIC # shuffle：表示是否允许shuffle，增加分区数会导致shuffle
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize(["a","b","c","d","e","f"])

print(rdd.glom().collect())
print(rdd.coalesce(2).glom().collect())
print(rdd.coalesce(6).glom().collect())
print(rdd.coalesce(6, True).glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### mapPartitions 算子
# MAGIC 
# MAGIC mapPartitions算子，和map一致，对RDD执行指定的逻辑操作，一次处理一整个分区。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.mapPartitions(func, preservesPartitioning=False)
# MAGIC 
# MAGIC # func：处理函数，由于每次处理的是一整个分区的数据，处理的是迭代数据，返回值也要求是可迭代的数据
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])

print(rdd.glom().collect())
print(rdd.map(lambda x: (type(x), x)).glom().collect())
print(rdd.mapPartitions(lambda x: [type(x)]).glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 案例
# MAGIC 
# MAGIC 从订单列表中统计 Plain Papadum 每年的销售额。

# COMMAND ----------

import datetime

# 读取数据文件
fileRdd = sc.textFile("/mnt/databrickscontainer1/restaurant-1-orders.csv")

# 将订单中的标题行去掉
dataRdd = fileRdd.map(lambda x: x.split(",")).filter(lambda x: x[0] != "Order Number")

# 过滤出 Plain Papadum 的订单
papadumRdd = dataRdd.filter(lambda x: x[2] == "Plain Papadum")

# 将日期字符串转换成日期，将数量与单价相乘
dateRdd = papadumRdd.map(lambda x: [datetime.datetime.strptime(x[1],'%d/%m/%Y %H:%M').year, float(x[3]) * float(x[4])])

# 将订单金额按年进行聚合
totalRdd = dateRdd.reduceByKey(lambda a,b: a + b)

# 原始统计结果
print(totalRdd.collect())
# 统计结果按年排序
print(totalRdd.sortByKey().collect())
# 统计结果按销售额排序
print(totalRdd.sortBy(lambda x: x[1]).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 常用的Action算子

# COMMAND ----------

# MAGIC %md
# MAGIC #### collect 算子
# MAGIC 
# MAGIC collect算子，将RDD各个分区内的数据，统一收集到Driver中，形成List对象。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.collect()
# MAGIC ```
# MAGIC 
# MAGIC > 这个算子，是将RDD各个分区的数据都拉取到Driver中  
# MAGIC RDD是分布式对象，其数据可能很大，所以用这个算子之前，需要了解RDD的数据，确保数据集不会太大，否则会吧Driver的内存撑爆。

# COMMAND ----------

rdd = sc.parallelize([0,1,2,0,1,2,0,2,2,0], 2)
print(rdd)
print(rdd.collect())
print(rdd.glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### countByKey 算子
# MAGIC 
# MAGIC countByKey算子，**针对K-V型RDD**，统计K出现的次数。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.countByKey()
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 3), ("c", 4), ("b", 2), ("a", 2)])

print(rdd.countByKey())

# COMMAND ----------

# MAGIC %md
# MAGIC #### reduce 算子
# MAGIC 
# MAGIC reduce算子，对RDD数据集按照指定的逻辑进行聚合。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.reduce(func)
# MAGIC 
# MAGIC # func：(T, T) -> T
# MAGIC # 接收两个参数，返回一个返回值，要求参数与返回值的类型相同
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize(range(1, 11), 1)

print(rdd.reduce(lambda a, b: a + b))
print(rdd.reduce(lambda a, b: a * b))

# COMMAND ----------

# MAGIC %md
# MAGIC #### fold 算子
# MAGIC 
# MAGIC fold算子，使用一个初始值，对RDD数据集按照指定的逻辑进行聚合。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.fold(zeroValue, func)
# MAGIC 
# MAGIC # zeroValue：T，初始值
# MAGIC # func：(T, T) -> T
# MAGIC # 初始值、函数的参数、函数的返回值，类型相同
# MAGIC ```
# MAGIC 
# MAGIC > **注意：** fold的初始值，会同时作用在  
# MAGIC > * 分区内聚合
# MAGIC > * 分区间聚合
# MAGIC > * 即便只有1个分区，也会将初始值作用在分区上

# COMMAND ----------

# 当有1个分区时：初始值 + 分区1的聚合结果（初始值 + 分区元素的聚合结果） = 5 + (5 + 5050) = 5060
print(sc.parallelize(range(1,101),1).fold(5, lambda a,b: a + b))
# 当有2个分区时：(x1 + x2 = 5050)
# 初始值 + 分区1的聚合结果（初始值 + 分区元素的聚合结果） + 分区2的聚合结果（初始值 + 分区元素的聚合结果） = 5 + (5 + x1) + (5 + x2) = 5065
print(sc.parallelize(range(1,101),2).fold(5, lambda a,b: a + b))
# 当有3个分区时：(x1 + x2 + x3 = 5050)
# 初始值 + 分区1的聚合结果（初始值 + 分区元素的聚合结果） + 分区2的聚合结果（初始值 + 分区元素的聚合结果） + 分区3的聚合结果（初始值 + 分区元素的聚合结果） = 5 + (5 + x1) + (5 + x2) + (5 + x3) = 5070
print(sc.parallelize(range(1,101),3).fold(5, lambda a,b: a + b))
# 由于我们的CPU是4个，所以默认是4个分区，结果就是 5075
print(sc.parallelize(range(1,101)).fold(5, lambda a,b: a + b))
# 分区数每增加1个，最终聚合的结果就增加一个初始值

# COMMAND ----------

print(sc.parallelize(["a","b","c"],1).fold("f", lambda a,b: a + b))
print(sc.parallelize(["a","b","c"],2).fold("f", lambda a,b: a + b))
print(sc.parallelize(["a","b","c"],3).fold("f", lambda a,b: a + b))
print(sc.parallelize(["a","b","c"]).fold("f", lambda a,b: a + b))
print(sc.parallelize(["a","b","c"],50).fold("f", lambda a,b: a + b))

# COMMAND ----------

# MAGIC %md
# MAGIC #### first 算子
# MAGIC 
# MAGIC first算子，取出RDD中的第一个元素。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.first()
# MAGIC ```

# COMMAND ----------

print(sc.parallelize(["a","b","c"],1).first())

# COMMAND ----------

# MAGIC %md
# MAGIC #### take 算子
# MAGIC 
# MAGIC take算子，取出RDD中的前N个元素。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.take(N)
# MAGIC 
# MAGIC # N：需要取出的元素个数
# MAGIC ```

# COMMAND ----------

print(sc.parallelize(["a","b","c","e","f","g"],1).take(3))
print(sc.parallelize(["a","b","c","e","f","g"],2).glom().collect(), sc.parallelize(["a","b","c","e","f","g"],2).take(3))
print(sc.parallelize(["a","b","c","e","f","g"],3).glom().collect(), sc.parallelize(["a","b","c","e","f","g"],3).take(3))
print(sc.parallelize(["a","b","c","e","f","g"],4).glom().collect(), sc.parallelize(["a","b","c","e","f","g"],4).take(3))
print(sc.parallelize(["a","b","c","e","f","g"],5).glom().collect(), sc.parallelize(["a","b","c","e","f","g"],5).take(3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### top 算子
# MAGIC 
# MAGIC top算子，对RDD中的元素进行降序排序，然后取出前N个元素。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.top(N)
# MAGIC 
# MAGIC # N：需要取出的元素个数
# MAGIC ```

# COMMAND ----------

print(sc.parallelize(["a","b","c","e","f","g"],1).top(3))
print(sc.parallelize(["a","b","c","e","f","g"],2).glom().collect(), sc.parallelize(["a","b","c","e","f","g"],2).top(3))
print(sc.parallelize(["a","b","c","e","f","g"],3).glom().collect(), sc.parallelize(["a","b","c","e","f","g"],3).top(3))
print(sc.parallelize(["a","b","c","e","f","g"],4).glom().collect(), sc.parallelize(["a","b","c","e","f","g"],4).top(3))
print(sc.parallelize(["a","b","c","e","f","g"],5).glom().collect(), sc.parallelize(["a","b","c","e","f","g"],5).top(3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### takeSample 算子
# MAGIC 
# MAGIC takeSample算子，随机抽样RDD的数据。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.takeSample(withReplacement, num, seed=None)
# MAGIC 
# MAGIC # withReplacement：是否允许数据重复，True表示可以多次取同一个数据，False表示不可以取同一个数据
# MAGIC # num：抽样数目
# MAGIC # seed：随机数种子
# MAGIC ```
# MAGIC 
# MAGIC > 随机数种子数字可以随便传，相同随机数种子取出的结果是一致的。一般我们不给定这个参数，由Spark自己给定。

# COMMAND ----------

# 允许多次抽取同一个元素
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(True,5))
# 不允许多次抽取同一个元素，但每次抽取的结果可能是不一样的
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(False,5))
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(False,5))
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(False,5))
# 指定随机数种子后，每次抽取的结果是一致的
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(False,5, 5))
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(False,5, 5))
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(False,5, 5))
# 不同的随机数种子，抽取的结果是不一样的
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(False,5, 5))
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(False,5, 4))
print(sc.parallelize(["a","b","c","e","f","g"],1).takeSample(False,5, 2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### takeOrdered 算子
# MAGIC 
# MAGIC takeOrdered算子，对RDD进行排序，然后取出前N个元素，与top类似，只是可以自己指定排序规则。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.takeSample(num, key=None)
# MAGIC 
# MAGIC # num：抽样数目
# MAGIC # key：排序数据
# MAGIC ```

# COMMAND ----------

print(sc.parallelize(["aaa", "eee", "ff", "bbbbb", "g", "cccc"], 1).top(3))
print(sc.parallelize(["aaa", "eee", "ff", "bbbbb", "g", "cccc"], 1).takeOrdered(3, lambda x: -len(x)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### foreach 算子
# MAGIC 
# MAGIC foreach算子，对RDD每一个元素，执行指定的逻辑操作，这个算子没有返回值。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.foreach(func)
# MAGIC 
# MAGIC # func: (T) -> None
# MAGIC ```

# COMMAND ----------

print(sc.parallelize(range(1,11),1).map(lambda x: x * 2))
print(sc.parallelize(range(1,11),1).map(lambda x: x * 2).collect())
print(sc.parallelize(range(1,11),1).foreach(lambda x: x * 2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### foreachPartition 算子
# MAGIC 
# MAGIC foreachPartition算子，和foreach一致，对RDD每一个元素，执行指定的逻辑操作，一次处理一整个分区，这个算子没有返回值。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.foreachPartition(func)
# MAGIC 
# MAGIC # func: (T) -> None
# MAGIC ```

# COMMAND ----------

rdd = sc.parallelize(range(1,11),3)

rdd.foreach(lambda x: print(x, type(x)))
rdd.foreachPartition(lambda x: print(type(x), list(x)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### saveAsText 算子
# MAGIC 
# MAGIC saveAsText算子，将RDD的数据写入文本文件中。支持写到本地、分布式文件系统等，每个分区写一个子文件。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.saveAsTextFile(path,compressionCodecClass=None)
# MAGIC 
# MAGIC # path: 文件的写出路径
# MAGIC # compressionCodecClass：压缩类
# MAGIC ```

# COMMAND ----------

rdd1 = sc.parallelize(["a","b","c","e","f","g"],1)
print(rdd1.glom().collect())
rdd1.saveAsTextFile("/mnt/databrickscontainer1/partition1")

rdd2 = sc.parallelize(["a","b","c","e","f","g"],2)
print(rdd2.glom().collect())
rdd2.saveAsTextFile("/mnt/databrickscontainer1/partition2")

rdd3 = sc.parallelize(["a","b","c","e","f","g"],3)
print(rdd3.glom().collect())
rdd3.saveAsTextFile("/mnt/databrickscontainer1/partition3")

rdd4 = sc.parallelize(["a","b","c","e","f","g"],4)
print(rdd4.glom().collect())
rdd4.saveAsTextFile("/mnt/databrickscontainer1/partition4")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 注意
# MAGIC 
# MAGIC 在前面的Action算子中，有几个算子：
# MAGIC * foreach
# MAGIC * foreachPartition
# MAGIC * saveAsText
# MAGIC 
# MAGIC 这几个算子无返回值，是分区直接执行的，跳过Driver，由Executor直接执行。
# MAGIC 
# MAGIC 其他算子有返回值，都会将执行结果发送到Driver。
