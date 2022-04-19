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
# MAGIC * wholeTextFile

# COMMAND ----------

# MAGIC %md
# MAGIC #### textFile
# MAGIC 
# MAGIC `textFile`可以读取本地文件，也可以读取分布式文件系统上的文件，比如：`HDFS`、`S3`。

# COMMAND ----------

# 读取分布式文件系统上的文件
rdd = sc.textFile("%s/Words.txt" % dfs_endpoint)
print(rdd.collect())

# COMMAND ----------

# 读取Databricks文件系统(DBFS)上的文件
rdd = sc.textFile("dbfs:/mnt/databrickscontainer1/Words.txt")
print(rdd.collect())

# COMMAND ----------

# 读取Databricks文件系统(DBFS)上的文件
# 可以像访问本地文件一样直接使用绝对路径进行访问
rdd = sc.textFile("/mnt/databrickscontainer1/Words.txt")
print(rdd.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### wholeTextFile
# MAGIC 
# MAGIC 与`textFile`功能一致，不过`wholeTextFile`更适合读取很多小文件的场景。

# COMMAND ----------

rdd = sc.textFile("/mnt/databrickscontainer1")
print(rdd.collect())

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

# COMMAND ----------

rdd = sc.parallelize([0, 1,2,3,4,5,6,7,8,9])

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
# MAGIC #### reduceByKey 算子
# MAGIC 
# MAGIC reduceByKey算子，针对K-V型RDD，自动按照K分组，然后根据提供的聚合逻辑，完成组内数据的聚合操作。
# MAGIC 
# MAGIC 语法：
# MAGIC ```
# MAGIC rdd.reduceByKey(func)
# MAGIC ```
# MAGIC 
# MAGIC > func 函数只复制处理聚合逻辑，不负责分组

# COMMAND ----------

rdd = sc.parallelize([("a", 1),("b", 1), ("a", 2), ("b", 2), ("a", 3)])
print(rdd.reduceByKey(lambda a,b: a + b).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #### WordCount案例回顾
# MAGIC 
# MAGIC 在我们的WordCount案例中，使用到以下知识点：
# MAGIC * 程序入口对象是SparkContext对象，sc，主要功能就是创建第一个RDD出来
# MAGIC   * wordsRDD = sc.textFile("/mnt/databrickscontainer1/Words.txt")
# MAGIC * 通过读取文件创建RDD，textFile
# MAGIC   * wordsRDD = sc.textFile("/mnt/databrickscontainer1/Words.txt")
# MAGIC * RDD的特性
# MAGIC   * wordsRDD -> flatMapRDD -> mapRDD -> resultRDD
# MAGIC * 三个Transformation算子
# MAGIC   * flatMap
# MAGIC   * map
# MAGIC   * reduceByKey

# COMMAND ----------

# 第一步、读取本地数据 封装到RDD集合，认为列表List
wordsRDD = sc.textFile("/mnt/databrickscontainer1/Words.txt")
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
# MAGIC groupByKey算子，针对K-V型RDD，自动按照K分组。
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

rdd = sc.parallelize([0,1,2,0,1,2,0,2,2,0], 2)
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

rdd = sc.parallelize([0,1,2,3,4,5,6,7,8,9])
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

rdd1 = sc.parallelize([(101,"A"), (102, "B"), (103, "C")])
rdd2 = sc.parallelize([(102, 90), (104, 95), (103, "C")])

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
# MAGIC sortByKey算子，针对K-V型RDD，按照K对RDD的数据进行排序。
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
