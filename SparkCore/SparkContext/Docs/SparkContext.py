# Databricks notebook source
# MAGIC %md
# MAGIC # SparkContext
# MAGIC 
# MAGIC SparkContext：是Spark Application程序的入口。
# MAGIC 
# MAGIC 任何一个应用首先需要构建SparkContext对象，如下两步构建：
# MAGIC * 创建SparkConf对象
# MAGIC   * 设置Spark Application基本信息，比如应用的名称AppName和应用运行Master
# MAGIC * 基于SparkConf对象，创建SparkContext对象
# MAGIC 
# MAGIC 在spark-shell、pyspark、databricks等这种交互式的环境中，已经默认帮我们创建好了SparkContext，我们可以直接用sc来得到SparkContext。
# MAGIC 
# MAGIC 对于我们开发的需要提交到集群运行的代码，则需要我们自己创建SparkContext。
# MAGIC 
# MAGIC 具体可参考：[https://spark.apache.org/docs/3.2.1/rdd-programming-guide.html](https://spark.apache.org/docs/3.2.1/rdd-programming-guide.html)

# COMMAND ----------

# 创建SparkConf对象，设置应用的配置信息，比如应用名称和应用运行模式
conf = SparkConf().setAppName("app").setMaster("yarn")
# 构建SparkContext上下文实例对象，读取数据和调度Job执行
sc = SparkContext(conf=conf)

# COMMAND ----------

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    masters = ["local[*]", "yarn"]
    # TODO: 当应用运行在集群上的时候，main函数就是Driver，创建SparkContext对象
    # 创建SparkConf对象，设置应用的配置信息，比如应用名称和应用运行模式
    conf = SparkConf().setAppName("app").setMaster("yarn")
    # TODO: 构建SparkContext上下文实例对象，读取数据和调度Job执行
    sc = SparkContext(conf=conf)
