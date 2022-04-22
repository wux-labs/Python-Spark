# Databricks notebook source
# MAGIC %md
# MAGIC # DataFrame入门

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame的组成
# MAGIC 
# MAGIC DataFrame是一个二维表结构，那么表结构就应该有：
# MAGIC * 行
# MAGIC * 列
# MAGIC * 表结构描述
# MAGIC 
# MAGIC 基于这样的前提，DataFrame的组成如下。
# MAGIC 
# MAGIC 在结构层面：
# MAGIC * StructType对象描述整个DataFrame的表结构
# MAGIC * StructField对象描述一个列的信息
# MAGIC 
# MAGIC 在数据层面：
# MAGIC * Row对象记录一行数据
# MAGIC * Column对象记录一列数据并包含列信息

# COMMAND ----------

df = spark.read.csv("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

# DataFrame 的结构
df.printSchema()

# DataFrame的行 Row
row = df.head()
print(row)

# DataFrame的列 Column
col = df[0]
print(col)
print(col.name(), col.desc(), col.isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 一个StructField记录了：**列名、列类型、列是否允许为空**。
# MAGIC 
# MAGIC 多个StructField组成一个StructType对象。
# MAGIC 
# MAGIC 一个StructType对象可以描述一个DataFrame有几个列、每个列的名字、每个列的类型、每个列是否允许为空。
# MAGIC 
# MAGIC 一个Row对象描述一行数据，比如：Row(Order Number='16118', Order Date='03/08/2019 20:25', Item Name='Plain Papadum', Quantity='2', Product Price='0.8', Total products='6')
# MAGIC 
# MAGIC 一个Column对象描述一列数据，Column对象包含一列数据和列的信息

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame的代码构建

# COMMAND ----------

# MAGIC %md
# MAGIC ### 基于RDD方式1
# MAGIC 
# MAGIC DataFrame对象可以从RDD转换而来，都是分布式数据集其实就是转换一下内部存储的结构，转换为二维表结构。

# COMMAND ----------

rdd = sc.textFile("/mnt/databrickscontainer1/restaurant-1-orders.csv").map(lambda x: x.split(","))

print(type(rdd))

df = spark.createDataFrame(rdd, schema=["OrderNumber1","OrderDate1","ItemName1","Quantity1","ProductPrice1","TotalProducts1"])

print(type(df))

df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 基于RDD方式2
# MAGIC 
# MAGIC 通过StructType对象来定义DataFrame的“表结构”转换RDD。

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

rdd = sc.textFile("/mnt/databrickscontainer1/restaurant-1-orders.csv").map(lambda x: x.split(",")).filter(lambda x: x[0] != "Order Number")

print(type(rdd))

schema = StructType().\
add("OrderNumber2", StringType(), nullable=False).\
add("OrderDate2", StringType(), nullable=False).\
add("ItemName2", StringType(), nullable=False).\
add("Quantity2", StringType(), nullable=False).\
add("ProductPrice2", StringType(), nullable=False).\
add("TotalProducts2", StringType(), nullable=False)

df = spark.createDataFrame(rdd, schema)

print(type(df))

df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 基于RDD方式3
# MAGIC 
# MAGIC 使用RDD的toDF方法转换RDD。

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

rdd = sc.textFile("/mnt/databrickscontainer1/restaurant-1-orders.csv").map(lambda x: x.split(",")).filter(lambda x: x[0] != "Order Number")

print(type(rdd))

schema = StructType().\
add("OrderNumber3", StringType(), nullable=False).\
add("OrderDate3", StringType(), nullable=False).\
add("ItemName3", StringType(), nullable=False).\
add("Quantity3", StringType(), nullable=False).\
add("ProductPrice3", StringType(), nullable=False).\
add("TotalProducts3", StringType(), nullable=False)

df = rdd.toDF(schema)

print(type(df))

df.printSchema()
df.show()
