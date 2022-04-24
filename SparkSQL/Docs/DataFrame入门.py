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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 基于Pandas的DataFrame
# MAGIC 
# MAGIC 可以将Pandas的DataFrame对象，转变为分布式的SparkSQL的DataFrame对象。

# COMMAND ----------

import pandas as pd

# 通过读取数据文件，得到Pandas的DataFrame
pdf = pd.read_csv("../../Datasets/restaurant-1-orders.zip", compression="zip")

print(type(pdf))
print(pdf.sample(5))

# 将Pandas的DataFrame转换成Spark的DataFrame
sdf = spark.createDataFrame(pdf)

print(type(sdf))
sdf.printSchema()
sdf.show()

display(pdf)
display(sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 读取外部数据
# MAGIC 
# MAGIC 通过SparkSQL的统一API进行数据读取构建DataFrame。
# MAGIC 
# MAGIC 支持的外部数据：
# MAGIC * text
# MAGIC * csv
# MAGIC * json
# MAGIC * parquet
# MAGIC * orc
# MAGIC * avro
# MAGIC * jdbc
# MAGIC * ...
# MAGIC 
# MAGIC 统一API示例代码：
# MAGIC ```
# MAGIC spark.read.format("text|csv|json|parquet|orc|avro|jdbc|......")
# MAGIC .option("K", "V") # option可选
# MAGIC .schema(StructType | String) # String的语法如.schema("name String", "age Int")
# MAGIC .load("被读取文件的路径, 支持本地文件系统和HDFS")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC #### 文本类型数据文件
# MAGIC 
# MAGIC 文本类型的数据可以直接用简单的文本编辑器打开进行查看或编辑，比如：text文件、csv文件、json文件等。

# COMMAND ----------

# MAGIC %md
# MAGIC ##### text
# MAGIC 
# MAGIC 读取text数据源，使用format("text")读取文本数据，读取到的DataFrame只会有一个列，列名默认称之为：value。
# MAGIC 
# MAGIC 如果需要将一列进行拆分，则需要使用代码：`.map(lambda x: x.split(","))`来进行拆分转换。

# COMMAND ----------

df = spark.read.format("text").load("/mnt/databrickscontainer1/restaurant-1-orders.csv")

print(type(df))
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# 将 read.format("text").load(path) 合并为 read.text(path)
spark.read.text("/mnt/databrickscontainer1/restaurant-1-orders.csv").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### csv
# MAGIC 
# MAGIC 读取csv数据源，使用format("csv")读取csv数据。

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv")

print(type(df))
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# 可以为数据读取指定选项
spark.read.format("csv").option("header", True).load("/mnt/databrickscontainer1/restaurant-1-orders.csv").show()

# COMMAND ----------

# 将 read.format("csv").load(path) 合并为 read.csv(path)
spark.read.csv("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### json
# MAGIC 
# MAGIC 读取json数据源，使用format("json")读取json数据。

# COMMAND ----------

# 由于没有现成的文件，所以我们只能造一个
df = spark.read.csv("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

df.where("Quantity=20").write.mode("overwrite").json("/mnt/databrickscontainer1/restaurant-1-orders.json")

# COMMAND ----------

df = spark.read.format("json").load("/mnt/databrickscontainer1/restaurant-1-orders.json")

print(type(df))
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# 将 read.format("json").load(path) 合并为 read.json(path)
spark.read.json("/mnt/databrickscontainer1/restaurant-1-orders.json").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 自带schema的数据文件
# MAGIC 
# MAGIC 在大数据环境中，还有其他各种各样的数据格式，比如：Parquet、Avro、ORC。
# MAGIC 
# MAGIC 相同之处：
# MAGIC * 基于Hadoop文件系统优化出的存储结构
# MAGIC * 提供高效的压缩
# MAGIC * 二进制存储格式
# MAGIC * 文件可分割，具有很强的伸缩性和并行处理能力
# MAGIC * 使用schema进行自我描述
# MAGIC * 属于线上格式，可以在Hadoop节点之间传递数据
# MAGIC 
# MAGIC 不同之处：
# MAGIC * 行式存储or列式存储：Parquet和ORC都以列的形式存储数据，而Avro以基于行的格式存储数据。
# MAGIC * 压缩率：基于列的存储区Parquet和ORC提供的压缩率高于基于行的Avro格式。 
# MAGIC * 可兼容的平台：
# MAGIC   * ORC常用于Hive、Presto
# MAGIC   * Parquet常用于Spark、Impala、Drill、Arrow
# MAGIC   * Avro常用于Kafka、Druid。
# MAGIC 
# MAGIC 这种类型的文件，不能使用简单文本编辑器进行打开查看或编辑。

# COMMAND ----------

# 由于没有现成的文件，所以我们只能构造一个
df = spark.read.csv("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

df.where("Quantity=5").write.mode("overwrite").parquet("/mnt/databrickscontainer1/restaurant-1-orders-parquet")

df.where("Quantity=10").write.mode("overwrite").orc("/mnt/databrickscontainer1/restaurant-1-orders-orc")

df.select("Quantity").where("Quantity=15").write.mode("overwrite").format("avro").save("/mnt/databrickscontainer1/restaurant-1-orders-avro")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### parquet

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/databrickscontainer1/restaurant-1-orders-parquet")

print(type(df))
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# 将 read.format("parquet").load(path) 合并为 read.parquet(path)
spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders-parquet").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### orc

# COMMAND ----------

df = spark.read.format("orc").load("/mnt/databrickscontainer1/restaurant-1-orders-orc")

print(type(df))
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# 将 read.format("orc").load(path) 合并为 read.orc(path)
spark.read.orc("/mnt/databrickscontainer1/restaurant-1-orders-orc").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### avro

# COMMAND ----------

df = spark.read.format("avro").load("/mnt/databrickscontainer1/restaurant-1-orders-avro")

print(type(df))
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 传统的结构化数据源
# MAGIC 
# MAGIC 首先我们创建一批数据。
# MAGIC 
# MAGIC ```sql
# MAGIC create database spark;
# MAGIC use spark;
# MAGIC 
# MAGIC create table spark_mysql_test (
# MAGIC     Order_Number varchar(10),
# MAGIC     Order_Date varchar(20),
# MAGIC     Item_Name varchar(30),
# MAGIC     Quantity int,
# MAGIC     Product_Price decimal(20,2),
# MAGIC     Total_products int
# MAGIC );
# MAGIC 
# MAGIC insert into spark_mysql_test
# MAGIC values
# MAGIC ('16089','02/08/2019 18:41','Plain Papadum 5','10','0.8','21'),
# MAGIC ('15879','20/07/2019 16:55','Plain Papadum 5','10','0.8','7'),
# MAGIC ('15133','01/06/2019 13:04','Plain Papadum 5','10','0.8','7'),
# MAGIC ('14752','11/05/2019 17:48','Plain Papadum 5','10','0.8','8'),
# MAGIC ('13212','02/02/2019 17:47','Plain Papadum 5','10','0.8','8')
# MAGIC ;
# MAGIC 
# MAGIC select * from spark_mysql_test;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ##### jdbc
# MAGIC 
# MAGIC 我们可以通过jdbc链接读取数据库中的数据。
# MAGIC 
# MAGIC 读取jdbc数据，需要指定一些参数：
# MAGIC * url：数据库的链接字符串
# MAGIC * user：连接数据库的用户
# MAGIC * password：连接数据库的密码
# MAGIC * query：去读数据的查询语句

# COMMAND ----------

spark.read.format("jdbc").option("url","jdbc:mysql://wux-mysql.mysql.database.azure.com:3306/spark?useSSL=true&requireSSL=false").option("user","wux_labs@wux-mysql").option("password","Pa55w.rd").option("query","select * from spark_mysql_test").load().show()
