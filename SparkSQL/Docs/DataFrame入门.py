# Databricks notebook source
# MAGIC %md
# MAGIC # DataFrame入门

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

rdd = sc.textFile("/mnt/databrickscontainer1/restaurant-1-orders.csv").map(lambda x: x.split(",")).filter(lambda x: x[0] != "Order Number").map(lambda x: (x[0],x[1],x[2],int(x[3]),float(x[4]),float(x[5])))

print(type(rdd))

schema = StructType().\
add("OrderNumber2", StringType(), nullable=False).\
add("OrderDate2", StringType(), nullable=False).\
add("ItemName2", StringType(), nullable=False).\
add("Quantity2", IntegerType(), nullable=False).\
add("ProductPrice2", DoubleType(), nullable=False).\
add("TotalProducts2", DoubleType(), nullable=False)

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

rdd = sc.textFile("/mnt/databrickscontainer1/restaurant-1-orders.csv").map(lambda x: x.split(",")).filter(lambda x: x[0] != "Order Number").map(lambda x: (x[0],x[1],x[2],int(x[3]),float(x[4]),float(x[5])))

print(type(rdd))

schema = StructType().\
add("OrderNumber3", StringType(), nullable=False).\
add("OrderDate3", StringType(), nullable=False).\
add("ItemName3", StringType(), nullable=False).\
add("Quantity3", IntegerType(), nullable=False).\
add("ProductPrice3", DoubleType(), nullable=False).\
add("TotalProducts3", DoubleType(), nullable=False)

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

df.write.mode("overwrite").format("json").save("/mnt/databrickscontainer1/restaurant-1-orders-json")

# COMMAND ----------

df = spark.read.format("json").load("/mnt/databrickscontainer1/restaurant-1-orders-json")

print(type(df))
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# 将 read.format("json").load(path) 合并为 read.json(path)
spark.read.json("/mnt/databrickscontainer1/restaurant-1-orders-json").show(truncate=False)

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

df.selectExpr("`Order Number` as OrderNumber","`Order Date` as OrderDate","`Item Name` as ItemName","cast(Quantity as int) as Quantity","cast(`Product Price` as double) as ProductPrice","cast(`Total products` as int) as TotalProducts").write.mode("overwrite").parquet("/mnt/databrickscontainer1/restaurant-1-orders-parquet")

df.selectExpr("`Order Number` as OrderNumber","`Order Date` as OrderDate","`Item Name` as ItemName","cast(Quantity as int) as Quantity","cast(`Product Price` as double) as ProductPrice","cast(`Total products` as int) as TotalProducts").write.mode("overwrite").orc("/mnt/databrickscontainer1/restaurant-1-orders-orc")

df.selectExpr("`Order Number` as OrderNumber","`Order Date` as OrderDate","`Item Name` as ItemName","cast(Quantity as int) as Quantity","cast(`Product Price` as double) as ProductPrice","cast(`Total products` as int) as TotalProducts").write.mode("overwrite").format("avro").save("/mnt/databrickscontainer1/restaurant-1-orders-avro")

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
# MAGIC 
# MAGIC * 不支持将 read.format("avro").load(path) 合并为 read.avro(path)
# MAGIC * 需要使用spark-submit进行提交运行
# MAGIC > spark-submit --packages org.apache.spark:spark-avro_2.12:3.2.1 --name readAvro --master yarn readAvro.py

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
# MAGIC drop table spark_read_test;
# MAGIC 
# MAGIC create table spark_read_test (
# MAGIC     Order_Number varchar(10),
# MAGIC     Order_Date varchar(20),
# MAGIC     Item_Name varchar(30),
# MAGIC     Quantity int,
# MAGIC     Product_Price decimal(20,2),
# MAGIC     Total_products int
# MAGIC );
# MAGIC 
# MAGIC insert into spark_read_test
# MAGIC values
# MAGIC ('16089','02/08/2019 18:41','Plain Papadum 5','5','0.8','21'),
# MAGIC ('15879','20/07/2019 16:55','Plain Papadum 5','12','1.3','7'),
# MAGIC ('15133','01/06/2019 13:04','Plain Papadum 5','20','0.6','17'),
# MAGIC ('14752','11/05/2019 17:48','Plain Papadum 5','3','2.5','8'),
# MAGIC ('13212','02/02/2019 17:47','Plain Papadum 5','13','3.7','38')
# MAGIC ;
# MAGIC 
# MAGIC select * from spark_read_test;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ##### jdbc
# MAGIC 
# MAGIC 我们可以通过jdbc链接读取数据库中的数据。
# MAGIC 
# MAGIC 读取jdbc数据，需要指定一些参数：
# MAGIC * url：数据库的链接字符串，如果数据库表中的数据有中文，建议使用 useUnicode=true 来确保传输中不出现乱码
# MAGIC * user：连接数据库的用户
# MAGIC * password：连接数据库的密码
# MAGIC * query：去读数据的查询语句

# COMMAND ----------

df = spark.read.format("jdbc").option("url","jdbc:mysql://wux-mysql.mysql.database.azure.com:3306/spark?useSSL=true&requireSSL=false").option("user","wux_labs@wux-mysql").option("password","Pa55w.rd").option("query","select * from spark_read_test").load()

print(type(df))
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame的入门操作
# MAGIC 
# MAGIC DataFrame支持两种风格进行编程，分别是：
# MAGIC * DSL风格
# MAGIC * SQL风格

# COMMAND ----------

# MAGIC %md
# MAGIC #### printSchema
# MAGIC 
# MAGIC 功能：打印输出DataFrame的schema信息。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.printSchema()

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### show
# MAGIC 
# MAGIC 功能：展示DataFrame中的数据，默认展示20条。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.show(参数1, 参数2)
# MAGIC * 参数1：默认是20, 控制展示多少条
# MAGIC * 参数2：是否截断列的数据，默认只输出20个字符的长度，过长不显示，要显示的话需要指定 truncate = False。

# COMMAND ----------

df = spark.read.format("text").load("/mnt/databrickscontainer1/restaurant-1-orders.csv")

df.show()
df.show(truncate=False)
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DSL语法风格
# MAGIC 
# MAGIC DSL称之为：**领域特定语言**，其实就是指DataFrame的特有API。
# MAGIC 
# MAGIC DSL风格意思就是以调用API的方式来处理数据，比如：df.select().where().limit()

# COMMAND ----------

# MAGIC %md
# MAGIC #### select
# MAGIC 
# MAGIC 功能：选择DataFrame中的指定列（通过传入参数进行指定）。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.select(\*cols)
# MAGIC 
# MAGIC 可传递：
# MAGIC * 可变参数的cols对象，cols对象可以是Column对象来指定列或者字符串列名来指定列。
# MAGIC * List[Column]对象或者List[str]对象，用来选择多个列。

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

print(type(df["Order Number"]))

# 使用可变参数的Column对象
df.select(df["Order Number"], df["Order Date"]).show()
# 使用List[Column]对象
df.select([df["Order Number"], df["Order Date"], df["Item Name"]]).show()

# 使用可变参数的str对象
df.select("Order Number", "Order Date").show()
# 使用List[str]对象
df.select(["Order Number", "Order Date", "Item Name", "Quantity"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### selectExpr
# MAGIC 
# MAGIC 功能：投影一组 SQL 表达式并返回新的 DataFrame，这是 select 的一个变体，它接受 SQL 表达式。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.selectExpr(\*expr)
# MAGIC 
# MAGIC 可传递：
# MAGIC * 可变参数的SQL表达式

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

# 直接查询字段
df.select("Quantity").show(5)
# df.select(df["Quantity"]).show(5)

# 字段的简单运算，如何实现？
df.select(df["Quantity"] + 2).show(5)
# df.select("Quantity" + 2).show(5)
# df.select("Quantity + 2").show(5)

# 字段别名等复杂操作如何实现？
df.select(df["Quantity"].alias("SHULIANG")).show(5)
# df.select("Quantity as SHULIANG").show(5)

# COMMAND ----------

# 直接查询字段
# df.select("Quantity").show(5)
# df.select(df["Quantity"]).show(5)

# 字段的简单运算，如何实现？
# df.select(df["Quantity"] + 2).show(5)
df.selectExpr("Quantity + 2").show(5)
df.selectExpr("Quantity", "Quantity + 2").show(5)

# 字段别名等复杂操作如何实现？
# df.select(df["Quantity"].alias("SHULIANG")).show(5)
df.selectExpr("Quantity as SHULIANG").show(5)
df.selectExpr("Quantity", "Quantity as SHULIANG").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### distinct
# MAGIC 
# MAGIC 功能：返回不包含重复数据的新的DataFrame。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.distinct()

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

df.select(df["Order Number"]).show()

df.select(df["Order Number"]).distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### filter和where
# MAGIC 
# MAGIC 功能：过滤DataFrame内的数据，返回一个过滤后的DataFrame。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.filter()  
# MAGIC df.where()
# MAGIC 
# MAGIC > where和filter在功能上是等价的。  
# MAGIC > :func:`where` is an alias for :func:`filter`.

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

df.filter("Quantity > 30").show()
df.filter(df["Quantity"] > 30).show()

df.where("Quantity > 30").show()
df.where(df["Quantity"] > 30).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### groupBy
# MAGIC 
# MAGIC 功能：按照指定的列进行数据的分组，返回值是**GroupedData对象**。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.groupBy()
# MAGIC 
# MAGIC > 传入参数和select一样，支持多种形式。

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

df.groupBy("Quantity").count().show()
df.groupBy("Item Name").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### GroupedData对象
# MAGIC 
# MAGIC GroupedData对象是一个特殊的DataFrame数据集，其类全名：<class 'pyspark.sql.group.GroupedData'>。
# MAGIC 
# MAGIC 这个对象是经过groupBy后得到的返回值，内部记录了以分组形式存储的数据。
# MAGIC 
# MAGIC GroupedData对象有很多API，比如前面的count方法就是这个对象的内置方法，除此之外，像：min、max、avg、sum、等等许多方法都存在。

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

df.printSchema()
df.show()

# 类全名：<class 'pyspark.sql.group.GroupedData'>
print(type(df.groupBy("Quantity")))

df.groupBy("Quantity").count().show()
df.groupBy("Item Name").min().show()
df.groupBy("Item Name").max().show()
df.groupBy("Item Name").sum().show()
df.groupBy("Item Name").avg().show()


# COMMAND ----------

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.printSchema()

df.groupBy("Quantity").count().show()
df.groupBy("ItemName").count().show()

df.groupBy("ItemName").min("Quantity").show()
df.groupBy("ItemName").max("Quantity").show()
df.groupBy("ItemName").sum("Quantity").show()
df.groupBy("ItemName").avg("Quantity").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### sort和orderBy
# MAGIC 
# MAGIC 功能：对DataFrame的数据进行排序。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.sort(\*cols, \*kwargs)  
# MAGIC * cols：排序的列。
# MAGIC * kwargs：排序参数，用于指定是顺序排序还是倒序排序，接受boolean或List(boolean)。如果是List，则元素个数应该与cols的元素个数相同。
# MAGIC 
# MAGIC > orderBy = sort

# COMMAND ----------

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.show()

# COMMAND ----------

df.sort("ItemName").show()
df.sort(df["ItemName"], ascending=False).show()
df.sort(["ItemName","TotalProducts"], ascending=[False,False]).show()

# COMMAND ----------

df.orderBy("ItemName").show()
df.orderBy(df["ItemName"], ascending=False).show()
df.orderBy(["ItemName","TotalProducts"], ascending=[False,False]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### limit
# MAGIC 
# MAGIC 功能：限制返回的记录数。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.limit(num)
# MAGIC * num：指定限制返回的记录数

# COMMAND ----------

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.show() # 20
df.show(5) # 5

df.limit(5).show() # 20 -> 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### alias
# MAGIC 
# MAGIC 功能：返回一个带有别名的DataFrame，并且可以根据别名访问DataFrame中的数据。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.alias(alias)
# MAGIC * alias：指定别名的名称

# COMMAND ----------

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df2 = df.alias("df_name")

df2.printSchema()
df2.show()

df2.select("ItemName").show()
df2.select("df_name.ItemName").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### join
# MAGIC 
# MAGIC 功能：将两个DataFrame进行关联操作返回新的DataFrame。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.join(other, on=None, how=None)
# MAGIC * other：与df进行join操作的另一个DataFrame
# MAGIC * on：join操作的关联条件
# MAGIC * how：join操作的关联方式，支持 inner、outer、cross、full、fullouter、left、leftouter、right、rightouter、semi、anti、...
# MAGIC   * 默认是 inner

# COMMAND ----------

from pyspark.sql.functions import col
df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

display(df)

df1 = df.where("OrderNumber in ('16116','16117')").alias("df1_name")
df2 = df.where("OrderNumber in ('16117','16118')").alias("df2_name")

display(df1.join(df2))
display(df1.join(df2,"OrderNumber"))
display(df1.join(df2,["OrderNumber","ItemName"]))
display(df1.join(df2,["OrderNumber","ItemName"],"left"))
display(df1.join(df2,[col("df1_name.OrderNumber") == col("df2_name.OrderNumber"),col("df1_name.ItemName") == col("df2_name.ItemName")],"right"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL语法风格
# MAGIC 
# MAGIC SQL风格就是使用SQL语句处理DataFrame的数据，比如：spark.sql("SELECT * FROM xxx")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 注册DataFrame成为表
# MAGIC 
# MAGIC DataFrame的一个强大之处就是我们可以将它看作是一个关系型数据表，然后可以通过在程序中使用spark.sql()来执行SQL语句查询，结果返回一个DataFrame。
# MAGIC 
# MAGIC 如果想使用SQL风格的语法，需要将DataFrame注册成表，采用如下的方式：
# MAGIC ```
# MAGIC df.createTempView()                注册一个临时表，如果表已存在则报错
# MAGIC df.createOrReplaceTempView()       注册一个临时表，如果存在则进行替换
# MAGIC df.createGlobalTempView()          注册一个全局表，如果表已存在则报错
# MAGIC df.createOrReplaceGlobalTempView() 注册一个全局表，如果存在则进行替换
# MAGIC ```
# MAGIC 临时表只能在当前SparkSession中使用；全局表（Global）可以跨SparkSession使用，使用时需要用global_temp做前缀。

# COMMAND ----------

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

# df.createTempView("temp_orders")
# Temporary view 'temp_orders' already exists
# df.createTempView("temp_orders")
df.createOrReplaceTempView("temp_orders")
# df.createGlobalTempView("temp_orders_global")
df.createOrReplaceGlobalTempView("temp_orders_global")

# COMMAND ----------

# MAGIC %md
# MAGIC 注册好表后，可以通过spark.sql(sql)来执行sql查询，返回一个新的DataFrame。

# COMMAND ----------

spark.sql("select * from temp_orders").show()

# COMMAND ----------

spark.sql("select * from global_temp.temp_orders_global").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_orders

# COMMAND ----------

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.createTempView("orders")

spark.sql("select ItemName,Quantity from orders").show()
spark.sql("select ItemName,max(Quantity),min(Quantity),sum(Quantity) from orders group by ItemName").show()
spark.sql("select ItemName,max(Quantity) as MaxQuantity,min(Quantity) as MinQuantity,sum(Quantity) from orders as t1 group by ItemName").show()
spark.sql("select distinct ItemName,Quantity,Quantity+5,Quantity*5 from orders where Quantity > 20").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame的函数

# COMMAND ----------

# MAGIC %md
# MAGIC ### 内置函数
# MAGIC 
# MAGIC PySpark提供了一个包：pyspark.sql.functions。这个包里面提供了一系列的计算函数供SparkSQL使用。
# MAGIC 
# MAGIC 使用之前需要先导入相关的包。

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### 统计函数
# MAGIC 
# MAGIC **count、max、min、sum、avg、mean**
# MAGIC * count：统计数量
# MAGIC * max：取最大值
# MAGIC * min：取最小值
# MAGIC * sum：求和
# MAGIC * avg：求均值
# MAGIC * mean：求均值
# MAGIC 
# MAGIC 说明：
# MAGIC * 这些函数都接收一个Column作为参数
# MAGIC * 这些函数都返回一个Column
# MAGIC * avg和mean效果是一样的，都是avg
# MAGIC * 返回的是一个聚合值

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.printSchema()
df.show()

df.select(F.count("TotalProducts"), F.max("TotalProducts"), F.min("TotalProducts"), F.sum("TotalProducts"),
          F.avg("TotalProducts"), F.mean("TotalProducts")).show()

df.createOrReplaceTempView("orders")
spark.sql("select count(TotalProducts),max(TotalProducts),min(TotalProducts),sum(TotalProducts),avg(TotalProducts),mean(TotalProducts) from orders").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 简单计算函数
# MAGIC 
# MAGIC **abs、exp、sqrt、sin、cos、rand、round、ceil、floor、cbrt**
# MAGIC * abs：取绝对值
# MAGIC * exp：取指数
# MAGIC * sqrt：开平方
# MAGIC * sin：正弦值
# MAGIC * cos：余弦值
# MAGIC * rand：生成随机数
# MAGIC * round：四舍五入
# MAGIC * ceil：向上取整
# MAGIC * floor：向下取整
# MAGIC * cbrt：开三次方
# MAGIC 
# MAGIC 说明：
# MAGIC * 这些函数都接收一个Column作为参数(rand不需要参数)
# MAGIC * 这些函数都返回一个Column
# MAGIC * 返回的是一个单值

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.printSchema()
df.show()

display(df.select(F.abs("TotalProducts"), F.exp("TotalProducts"), F.sqrt("TotalProducts"), F.sin("TotalProducts"), F.abs(F.sin("TotalProducts")), F.cos("TotalProducts"), F.abs(F.cos("TotalProducts")), F.rand()))

display(df.select(F.round("TotalProducts"), F.round(F.cos("TotalProducts")), F.ceil("TotalProducts"), F.ceil(F.cos("TotalProducts")), F.floor("TotalProducts"), F.floor(F.cos("TotalProducts")), F.cbrt("TotalProducts")))

df.createOrReplaceTempView("orders")
display(spark.sql("select abs(TotalProducts),exp(TotalProducts),sqrt(TotalProducts),sin(TotalProducts),cos(TotalProducts),abs(cos(TotalProducts)),rand() from orders"))
display(spark.sql("select round(TotalProducts),round(cos(TotalProducts)),ceil(TotalProducts),ceil(cos(TotalProducts)),floor(TotalProducts),floor(cos(TotalProducts)),cbrt(TotalProducts) from orders"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### 时间相关的函数
# MAGIC 
# MAGIC **current_date、current_timestamp、date_add、date_sub、datediff、dayofmonth、dayofweek、dayofyear、last_day、year、month**
# MAGIC * current_date：返回当前日期
# MAGIC * current_timestamp：返回当前时间戳
# MAGIC * date_add：返回指定日期加几天的日期
# MAGIC * date_sub：返回指定日期减几天的日期
# MAGIC * datediff：返回两个日期相差的天数
# MAGIC * dayofmonth：返回指定日期在所在月的第几天
# MAGIC * dayofweek：返回指定日期在所在周的第几天
# MAGIC * dayofyear：返回指定日期在所在年的第几天
# MAGIC * last_day：返回指定日期在所在月的最后一天
# MAGIC * year：返回指定日期所在的年
# MAGIC * month：返回指定日期所在的月

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

# current_date、current_timestamp、date_add、date_sub、datediff、dayofmonth、dayofweek、dayofyear、last_day、year、month
display(df.select(F.current_date(), F.current_timestamp(), F.date_add(F.current_date(), 5), F.date_sub(F.current_date(), 5), F.datediff(F.date_add(F.current_date(), 5), F.date_sub(F.current_date(), 5)), F.dayofmonth(F.current_date()), F.dayofweek(F.current_date()), F.dayofyear(F.current_date()), F.last_day(F.current_date()), F.year(F.current_date()), F.month(F.current_date())))

df.createOrReplaceTempView("orders")
display(spark.sql("select current_date(), current_timestamp(), date_add(current_date(), 5), date_sub(current_date(), 5), datediff(date_add(current_date(), 5), date_sub(current_date(), 5)), dayofmonth(current_date()), dayofweek(current_date()), dayofyear(current_date()), last_day(current_date()), year(current_date()), month(current_date()) from orders"))


# COMMAND ----------

# MAGIC %md
# MAGIC #### 炸裂函数
# MAGIC 
# MAGIC **explode**
# MAGIC * explode(expr)：通过取消嵌套 expr 返回行。expr：一个 ARRAY 或 MAP 或表达式

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.where("OrderNumber = 16118").createOrReplaceTempView("orders")

display(spark.sql("select * from orders"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 将Array数据炸裂成多行
# MAGIC select explode(array(10,20,30,35)),t.* from orders t

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 将Map数据炸裂成多行
# MAGIC select explode(map(10,'10a',20,'20a',30,'30a')),t.* from orders t

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 根据表达式结果炸裂成多行，需要表达式的返回结果是Array或Map
# MAGIC select explode(split(ItemName, " ")),t.* from orders t

# COMMAND ----------

# MAGIC %md
# MAGIC #### 判断相关的函数
# MAGIC 
# MAGIC **is distinct from、isfalse、isnan、isnotnull、isnull、istrue、forall**
# MAGIC * is distinct from：判断两个表达式是否具有不同的值
# MAGIC * isfalse：判断表达式的值是否是false
# MAGIC * isnan：判断表达式是否是 NaN
# MAGIC * isnotnull：判断表达式是否非空，等效于 is not null
# MAGIC * isnull：判断表达式是否是空，等效于 is null
# MAGIC * istrue：判断表达式的值是否是true
# MAGIC * forall：判断表达式是否对数组中的所有元素都有效
# MAGIC 
# MAGIC > 由于判断中包含对空值、NaN等的判断，所以我们切换一下数据集，不使用`restaurant-1-orders`，而是使用`泰坦尼克号数据集`进行

# COMMAND ----------

df = spark.read.csv("/mnt/databrickscontainer1/taitanic_train.csv", header=True)

df.createOrReplaceTempView("taitanic")

# 先看一下原始数据
display(spark.sql("select * from taitanic"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select age, case when age is null then 'NaN' else age end as age1, age is distinct from 22, age is not distinct from 22, isnan(age), isnan(cast(case when age is null then 'NaN' else age end as double)) as age_nan,isnotnull(age), isnull(age), embarked, forall(array(embarked), x -> x in ('S','C','Q')) as for1, forall(array(embarked), x -> x in ('S','C')) as for2 from taitanic

# COMMAND ----------

# MAGIC %md
# MAGIC ### 窗口函数
# MAGIC 
# MAGIC **窗口：** 可以理解为满足指定条件的数据记录的集合。
# MAGIC 
# MAGIC **窗口函数：** 在满足指定条件的记录集合（一个窗口）上执行的特殊函数，对于每条记录都要在此窗口内执行函数。
# MAGIC 
# MAGIC **静态窗口：** 有的窗口函数的窗口大小是固定的，这种属于静态窗口。
# MAGIC 
# MAGIC **滑动窗口：** 有的窗口函数的窗口大小是不固定的，这种属于滑动窗口。
# MAGIC 
# MAGIC 窗口函数最明显的特征就是带有over()关键字。
# MAGIC 
# MAGIC 窗口函数使用order by关键字对数据进行排序，相同数据属于一个窗口；同时可以使用partition by进行数据分区，不同分区的数据独立进行窗口函数的调用。
# MAGIC 
# MAGIC 窗口函数同时具备了普通语句的group by功能（用partition by实现的），也具备普通语句的order by功能（用order by实现的）。但是与普通group by不同的是：group by语句会将相同组的数据聚合成一条数据，属于多对一的关系；而partition by在分组之后会保留原有数据的所有记录，不会聚合成一条，属于一对一的关系。
# MAGIC 
# MAGIC 窗口函数的引入是为了**既显示聚合前的数据，又显示聚合后的结果**，只是在窗口函数所在的查询位置上添加一列来展示聚合结果。
# MAGIC 
# MAGIC 普通聚合函数也可以用于窗口函数中，赋予它窗口函数的功能。由于普通聚合函数只返回一个值，但是窗口函数返回的是多行记录，所以一个窗口中所有记录的聚合结果列的值都是一样的。
# MAGIC 
# MAGIC **row_number() over()、rank() over()、dense_rank() over()、ntile() over()、avgs() over()**
# MAGIC * 排序类型
# MAGIC   * row_number：返回行号
# MAGIC   * rank：返回分组排序号，序号可能不连续
# MAGIC   * dense_rank：返回分组排序号，序号连续
# MAGIC * 聚合类型
# MAGIC   * sum
# MAGIC   * count
# MAGIC   * avg
# MAGIC   * max
# MAGIC   * min
# MAGIC * 分区类型
# MAGIC   * ntile

# COMMAND ----------

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.createOrReplaceTempView("orders")

# 先看一下原始数据
spark.sql("select * from orders").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 排序类型窗口函数

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 对全表按“订单日期”字段排序，返回行号
# MAGIC select *,  row_number() over(order by OrderDate desc) as rn from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 按“项目名称”进行分组，按“订单日期”字段排序，返回分组行号
# MAGIC select *,  row_number() over(partition by ItemName order by cast(OrderDate as date) desc) as rn2 from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 对全表按“订单日期”字段排序，返回分组排序
# MAGIC select *, rank() over(order by OrderDate) as rn1, dense_rank() over(order by OrderDate) as rn2 from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 按“项目名称”进行分组，按“数量”字段排序，返回分组排序
# MAGIC select *, rank() over(partition by ItemName order by Quantity) as rn1, dense_rank() over(partition by ItemName order by Quantity) as rn2 from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 聚合类型窗口函数

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 对全表按“订单日期”字段排序，对产品价格进行聚合
# MAGIC select *, sum(cast(ProductPrice as double)) over(order by OrderDate) as s, count(cast(ProductPrice as double)) over(order by OrderDate) as c, avg(cast(ProductPrice as double)) over(order by OrderDate) as a from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 按“项目名称”进行分组，按“数量”字段排序，对产品价格进行聚合
# MAGIC select *, sum(cast(ProductPrice as double)) over(partition by ItemName order by Quantity) as s, count(cast(ProductPrice as double)) over(partition by ItemName order by Quantity) as c, avg(cast(ProductPrice as double)) over(partition by ItemName order by Quantity) as a from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 自定义函数
# MAGIC 
# MAGIC 无论是Hive还是SparkSQL在做数据分析处理时，往往需要使用函数。前面我们已经了解了SparkSQL在pyspark.sql.functions中已经提供了很多实现公共功能的函数，那如果我们需要按照自己的需求来设计函数该怎么办呢？SparkSQL与Hive一样支持自定义函数：UDF和UDAF，尤其时UDF在项目中使用最广泛。
# MAGIC 
# MAGIC Hive支持三种自定义函数：
# MAGIC * UDF(User-Defined Function)，函数
# MAGIC   * 一对一关系，输入一个值经过函数以后输出一个值
# MAGIC * UDAF(User-Defined Aggregation Function)，聚合函数
# MAGIC   * 多对一关系，输入多行值输出一个值，通常与group by联合使用
# MAGIC * UDTF(User-Defined Table-Generation Function)
# MAGIC   * 一对多关系，输入一个值输出多行值
# MAGIC 
# MAGIC 在SparkSQL中，目前仅支持UDF函数和UDAF函数，而Python仅支持UDF函数。
# MAGIC 
# MAGIC 在SparkSQL中定义自定义函数有两种方式：
# MAGIC * sparksession.udf.register(参数1, 参数2, 参数3)
# MAGIC   * 注册的UDF可以用于DSL和SQL风格，其中返回值用于DSL风格，参数内的名字用于SQL风格
# MAGIC   * 参数1：UDF名称，用于SQL风格调用
# MAGIC   * 参数2：自定义的函数的函数名称
# MAGIC   * 参数3：声明UDF的返回值类型
# MAGIC * pyspark.sql.functions.udf(参数1, 参数2)
# MAGIC   * 仅能用于DSL风格
# MAGIC   * 参数1：自定义的函数的函数名称
# MAGIC   * 参数2：声明UDF的返回值类型

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDF
# MAGIC 
# MAGIC 下面我们自定义一个函数，让价格加上5。

# COMMAND ----------

# 定义一个将价格加5的函数
def price_add_5(data):
    return float(data) + 5

# COMMAND ----------

from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F

# 通过方式1进行函数的注册
# 参数1："udf1"，用于SQL风格的函数调用
# 参数2：price_add_5，自定义函数的函数名称
# 参数3：DoubleType()，声明UDF的返回值类型
# 返回值：udf2，用于DSL风格的函数调用
udf2 = spark.udf.register("udf1", price_add_5, DoubleType())

# 通过方式2进行函数的注册
# 参数1：price_add_5，自定义函数的函数名称
# 参数2：DoubleType()，声明UDF的返回值类型
# 返回值：udf3，用于DSL风格的函数调用
udf3 = F.udf(price_add_5, DoubleType())

# COMMAND ----------

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.createOrReplaceTempView("orders")

# 先看一下原始数据
spark.sql("select * from orders").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 通过方式1定义的UDF的参数1可以用于SQL风格
# MAGIC select t.*, udf1(ProductPrice) from orders t

# COMMAND ----------

# 通过方式1定义的UDF的返回值可以用于DSL风格
df.select("ProductPrice", udf2("ProductPrice")).show()

# COMMAND ----------

# 通过方式2定义的UDF的返回值可以用于DSL风格
df.select("ProductPrice", udf3("ProductPrice")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 但是，两种定义方式的返回值对应的函数名称，不可以用于SQL风格
# MAGIC select t.*, udf2(ProductPrice), udf3(ProductPrice) from orders t

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDAF
# MAGIC 
# MAGIC Python并不支持UDAF，要实现UDAF需要使用Scala或Java进行代码编写。
# MAGIC 
# MAGIC SparkSQL提供了内置函数 avg/mean，计算逻辑是：![](https://www.zhihu.com/equation?tex=Avg%3D%5Cfrac%7B%5Csum_%7Bi%3D1%7D%5En+x_i%7Dn)。
# MAGIC 
# MAGIC 我们自定义一个函数，求分组中最大值与最小值的平均值：![](https://www.zhihu.com/equation?tex=MyAvg%3D%5Cfrac%7Bmax%28x_i%29%2Bmin%28x_i%29%7D2)。

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.math.{max, min}
# MAGIC import org.apache.spark.sql.{Encoder, Encoders}
# MAGIC import org.apache.spark.sql.expressions.Aggregator
# MAGIC 
# MAGIC case class Average(var min: Double, var max: Double)
# MAGIC 
# MAGIC object MyAggregator extends Aggregator[Double, Average, Double] {
# MAGIC   
# MAGIC   def bufferEncoder: org.apache.spark.sql.Encoder[Average] = Encoders.product
# MAGIC   def outputEncoder: org.apache.spark.sql.Encoder[Double] = Encoders.scalaDouble
# MAGIC   
# MAGIC   // 初始化缓存数据
# MAGIC   def zero: Average = Average(Double.MaxValue, Double.MinValue)
# MAGIC   
# MAGIC   // 相同分区下，对下一个值进行聚合的逻辑
# MAGIC   // 缓存数据用于存放最大值、最小值
# MAGIC   def reduce(buffer: Average, value: Double): Average = Average(min(buffer.min, value), max(buffer.max, value))
# MAGIC   
# MAGIC   // 不同分区之间的数据合并
# MAGIC   def merge(buffer1: Average,buffer2: Average): Average = Average(min(buffer1.min, buffer2.min), max(buffer1.max, buffer2.max))
# MAGIC   
# MAGIC   // 聚合结束后，返回什么
# MAGIC   // 返回最大值、最小值的平均值 (reduction.min + reduction.max) / 2
# MAGIC   def finish(reduction: Average): Double = (reduction.min + reduction.max) / 2
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC spark.udf.register("MyAvg",udaf(MyAggregator))

# COMMAND ----------

# MAGIC %scala
# MAGIC // 由于单个产品的价格是相同的，所以取最大值、最小值的结果是一样的，求平均就没什么意义
# MAGIC // 所以我们将单价乘以数量，得到的是每个订单的购买总价，由于订单中的数量不同，所以总价就不同，可以看出效果
# MAGIC spark.sql("select ItemName, avg(Quantity * ProductPrice) as avg, max(Quantity * ProductPrice) as max, min(Quantity * ProductPrice) as min, MyAvg(Quantity * ProductPrice) as myavg from orders group by ItemName").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame的数据清洗
# MAGIC 
# MAGIC 上面我们处理的数据实际上都是已经被处理好的规整数据，但在实际生产中，数据可能是杂乱无章的，这就需要我们先对数据进行清洗，整理为符合处理要求的规整数据。

# COMMAND ----------

# MAGIC %md
# MAGIC #### 删除重复行
# MAGIC 
# MAGIC **dropDuplicates()**
# MAGIC 
# MAGIC 功能：对DataFrame的数据进行去重，如果重复数据有多条，取第一条。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.dropDuplicates(subset=None)  
# MAGIC * subset：去重的字段子集

# COMMAND ----------

# |Order Number|      Order Date|           Item Name|Quantity|Product Price|Total products|
df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

# 构建一个包含重复数据的DataFrame
dfDuplicates = df.select(df["Order Number"], df["Item Name"], df["Quantity"], df["Total products"])

# 缓存一下数据
dfDuplicates.cache()

# 展示原始记录数
print(dfDuplicates.count())

# 展示有重复的数据
dfDuplicates.groupBy(df["Order Number"], df["Item Name"], df["Quantity"], df["Total products"]).count().where("count > 1").show()

# 去除重复行
dfDistinct = dfDuplicates.dropDuplicates()

# 缓存一下
dfDistinct.cache()

# 展示去重后的记录数
print(dfDistinct.count())

# 无重复的数据
dfDistinct.groupBy(df["Order Number"], df["Item Name"], df["Quantity"], df["Total products"]).count().where("count > 1").show()


# COMMAND ----------

display(dfDuplicates.groupBy(df["Order Number"], df["Item Name"], df["Quantity"], df["Total products"]).count().where("count > 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC 去重可以指定字段，重复的数据保留第一条。

# COMMAND ----------

# |Order Number|      Order Date|           Item Name|Quantity|Product Price|Total products|
df = spark.read.format("csv").load("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

# use '&' for 'and', '|' for 'or', '~' for 'not'
dfTest = df.where(((df["Order Number"] == "7193") | (df["Order Number"] == "9360")) & (df["Item Name"] == "Bhuna"))

dfTest.show()

dfTest.dropDuplicates(["Item Name"]).show()
dfTest.dropDuplicates(["Quantity"]).show()
dfTest.dropDuplicates(["Item Name", "Order Date"]).show()

# COMMAND ----------

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

display(df)

display(df.select("OrderNumber","OrderDate"))
display(df.select("OrderNumber","OrderDate").dropDuplicates())

display(df.orderBy("OrderNumber","ItemName"))
display(df.orderBy("OrderNumber","ItemName").dropDuplicates(["OrderNumber"]))
display(df.where("OrderNumber = 10001"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 删除有缺失值的行
# MAGIC 
# MAGIC **dropna()**
# MAGIC 
# MAGIC 功能：如果数据中包含null，通过dropna来判断，符合条件就删除这一行数据。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.dropna(how, thresh, subset)  
# MAGIC * how：删除方式，"any" 表示只要有字段为 null 就删除，"all" 表示该行所有字段都为 null 才删除。默认值是any  
# MAGIC * thresh：指定阈值进行删除，表示最少有多少列包含有效数据，该行数据才保留  
# MAGIC * subset：指定字段列表，表示需要在这些字段中满足上面的条件，该行数据才保留

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/taitanic_train.csv", header=True)

# 展示原始数据
display(df)

# COMMAND ----------

# 只有所有字段都是null才删除
display(df.dropna("all"))

# 默认值any，只要有字段为null就删除
display(df.dropna())

# 指定需要有3列是有效数据的才保留，由于泰坦尼克号数据集有12个字段，都满足3个有效字段，所以不会删除数据
display(df.dropna(thresh=3))

# 泰坦尼克号数据中有空值的字段是 "Age", "Cabin", "Embarked"
# 指定4个字段中有3个有效则保留，则会允许一个字段为null的数据留下来
display(df.dropna(thresh=3, subset=["Sex", "Age", "Cabin", "Embarked"]))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 删除列
# MAGIC 
# MAGIC **drop()**
# MAGIC 
# MAGIC 功能：删除指定的列，返回一个不包含指定列的新的DataFrame。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.drop(*cols) 
# MAGIC * cols：指定的需要删除的列
# MAGIC 
# MAGIC > 该功能可以用 df.select(指定列之外的列) 来实现。  
# MAGIC > 但是当DataFrame的列太多的时候，用select方式需要书写的列名太长，用drop就比较方便。

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/taitanic_train.csv", header=True)

# 展示原始数据
display(df)

# 删除指定的列
display(df.drop(df["Cabin"]))

# 可以用select实现，只是代码书写量大
display(df.select("PassengerId","Survived","Pclass","Name","Sex","Age","SibSp","Parch","Ticket","Fare","Embarked"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 填充缺失值
# MAGIC 
# MAGIC **fillna()**
# MAGIC 
# MAGIC 如果缺失的数据比较少，删除行或列造成数据丢失比较多，可以使用填充缺失值的方法来规整数据。
# MAGIC 
# MAGIC 功能：根据参数规则来填充null值。
# MAGIC 
# MAGIC 语法：  
# MAGIC df.fillna(value, subset)  
# MAGIC * value：填充的值，或者规则
# MAGIC * subset：指定填充的列

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/databrickscontainer1/taitanic_train.csv", header=True)

# 展示原始数据
display(df)

# 对所有缺失值填充指定的值
display(df.fillna("loss"))

# 对指定的列填充缺失值
display(df.fillna("loss", subset=["Cabin"]))

# 按照指定规则进行缺失值填充
display(df.fillna({"Age": 99, "Cabin": "cabin_loss", "Embarked": "Z"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame的持久化
# MAGIC 
# MAGIC 在学习SparkCore的时候，我们知道RDD的数据是过程数据，在下一次要用到RDD的数据的时候，再根据血缘关系，从头重新处理一遍RDD的数据。
# MAGIC 
# MAGIC 在SparkSQL模块中，DataFrame的数据也是过程数据。

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

df.show()

df.createOrReplaceTempView("orders")

df2 = spark.sql("select OrderNumber, ItemName, current_timestamp() as tm from orders")

df3 = df2.select("OrderNumber","ItemName","tm", F.dayofyear("tm"))

# COMMAND ----------

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 多执行几次上面的代码，可以看到df3的数据一直都在发生变化，可以知道DataFrame的数据也需要从头处理一遍。
# MAGIC 
# MAGIC DataFrame也像RDD一样可以进行数据持久化，同样提供了`cache`、`persist`、`checkpoint`来进行数据的持久化。

# COMMAND ----------

df2.cache()

# COMMAND ----------

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 我们将df2或df3进行持久化后，上面的数据就不会发生变化了。

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrame的数据写出
# MAGIC 
# MAGIC 与读取外部数据相对应，可以通过SparkSQL的统一API进行DataFrame数据的写出。
# MAGIC 
# MAGIC 支持将数据写出到：
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
# MAGIC spark.write.mode("append|overwrite|ignore|error").format("text|csv|json|parquet|orc|avro|jdbc|......")
# MAGIC .option("K", "V") # option可选
# MAGIC .save("文件的路径, 支持本地文件系统和HDFS")
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import StructType, StringType

schema = StructType().\
add("OrderNumber", StringType(), nullable=False).\
add("OrderDate", StringType(), nullable=False).\
add("ItemName", StringType(), nullable=False).\
add("Quantity", StringType(), nullable=False).\
add("ProductPrice", StringType(), nullable=False).\
add("TotalProducts", StringType(), nullable=False)

df = spark.read.csv("/mnt/databrickscontainer1/restaurant-1-orders.csv", schema=schema, header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 文本类型数据文件
# MAGIC 
# MAGIC 文本类型的数据可以直接用简单的文本编辑器打开进行查看或编辑，比如：text文件、csv文件、json文件等。

# COMMAND ----------

# MAGIC %md
# MAGIC #### text
# MAGIC 
# MAGIC Text data source supports only a single column.
# MAGIC 
# MAGIC 写出到文本类型的文件，仅支持一个列，有多余的列则会报错。

# COMMAND ----------

df.where("OrderNumber=16118").select("ItemName").write.mode("overwrite").format("text").save("/mnt/databrickscontainer1/restaurant-1-orders-text")

# COMMAND ----------

# 将 write.format("text").save(path) 合并为 write.text(path)
df.where("OrderNumber=16118").select("ItemName").write.mode("overwrite").text("/mnt/databrickscontainer1/restaurant-1-orders-text2")

# COMMAND ----------

# MAGIC %md
# MAGIC #### csv & json

# COMMAND ----------

df.where("OrderNumber=16118").write.mode("overwrite").format("csv").option("sep", ",").option("header", True).save("/mnt/databrickscontainer1/restaurant-1-orders-csv")

df.where("OrderNumber=16118").write.mode("overwrite").format("json").save("/mnt/databrickscontainer1/restaurant-1-orders-json")

# COMMAND ----------

# 将 write.format("csv").save(path) 合并为 write.csv(path)
df.where("OrderNumber=16118").write.mode("overwrite").csv("/mnt/databrickscontainer1/restaurant-1-orders-csv2", sep=",", header=True)
# 将 write.format("json").save(path) 合并为 write.json(path)
df.where("OrderNumber=16118").write.mode("overwrite").json("/mnt/databrickscontainer1/restaurant-1-orders-json2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 自带schema的数据文件
# MAGIC 
# MAGIC 在大数据环境中，还有其他各种各样的数据格式，比如：Parquet、Avro、ORC。

# COMMAND ----------

# MAGIC %md
# MAGIC #### parquet & orc

# COMMAND ----------

df.write.mode("overwrite").format("parquet").save("/mnt/databrickscontainer1/restaurant-1-orders-parquet")
df.write.mode("overwrite").format("orc").save("/mnt/databrickscontainer1/restaurant-1-orders-orc")

# COMMAND ----------

# 将 write.format("parquet").save(path) 合并为 write.parquet(path)
df.write.mode("overwrite").parquet("/mnt/databrickscontainer1/restaurant-1-orders-parquet2")
# 将 write.format("orc").save(path) 合并为 write.orc(path)
df.write.mode("overwrite").orc("/mnt/databrickscontainer1/restaurant-1-orders-orc2")

# COMMAND ----------

# MAGIC %md
# MAGIC #### avro
# MAGIC 
# MAGIC avro文件不支持不合格的列名称，比如包含空格："Order Number"

# COMMAND ----------

df.write.mode("overwrite").format("avro").save("/mnt/databrickscontainer1/restaurant-1-orders-avro")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 传统的结构化数据源

# COMMAND ----------

# MAGIC %md
# MAGIC #### jdbc
# MAGIC 
# MAGIC 我们可以通过jdbc链接将DataFrame的数据写入数据库中。
# MAGIC 
# MAGIC 需要指定一些参数：
# MAGIC * url：数据库的链接字符串，如果数据库表中的数据有中文，建议使用 useUnicode=true 来确保传输中不出现乱码
# MAGIC * user：连接数据库的用户
# MAGIC * password：连接数据库的密码
# MAGIC * dbtable：指定要写入的表名称

# COMMAND ----------

df.write.mode("overwrite").format("jdbc").option("url","jdbc:mysql://wux-mysql.mysql.database.azure.com:3306/spark?useSSL=true&requireSSL=false").option("user","wux_labs@wux-mysql").option("password","Pa55w.rd").option("dbtable","spark_write_test").option("showSql",True).save()
