// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # DataSet介绍
// MAGIC 
// MAGIC DataSet是Spark1.6引入的分布式数据集合，Spark2.0合并DataSet和DataFrame数据集合API，DataFrame变成DataSet的子集，DataFrame=DataSet[Row]。
// MAGIC 
// MAGIC DataSet是一个强类型，并且类型安全的数据容器，并且提供了结构化查询API和类似RDD一样的命令式API 。

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # DataSet的创建

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 通过序列创建DataSet

// COMMAND ----------

val ds = spark.range(1, 10, 2)

ds.show()

// COMMAND ----------

val ds = spark.createDataset(0 to 10)

ds.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 通过集合创建DataSet

// COMMAND ----------

case class Person(name: String, age: Int)

val ds = spark.createDataset(Seq(Person("wux",10),Person("labs",20)))

ds.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 通过RDD创建DataSet

// COMMAND ----------

val rdd = sc.parallelize(List(1,2,3,4,5), 4)

val ds = rdd.toDS

ds.show()

// COMMAND ----------

val rdd = sc.parallelize(List(Person("wux",10),Person("labs",20)), 4)

val ds = rdd.toDS

ds.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 通过DataFrame创建DataSet

// COMMAND ----------

case class Order (
val OrderNumber: String,
val OrderDate: String,
val ItemName: String,
val Quantity: Integer,
val ProductPrice: Double,
val TotalProducts: Integer
)

val df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

val ds = df.as[Order]

ds.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # DataSet的底层
// MAGIC 
// MAGIC DataSet 最底层处理的是对象的序列化形式，通过查看 DataSet 生成的物理执行计划，也就是最终处理的RDD，就可以判定 DataSet 底层处理的是什么形式的数据。

// COMMAND ----------

val rdd = ds.queryExecution.toRdd

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC dataset.queryExecution.toRdd 这个 API 可以看到 DataSet 底层执行的 RDD，这个 RDD 中的范型是 catalyst.InternalRow ，InternalRow 又称为 Catalyst Row ，是 DataSet 底层的数据结构，也就是说，无论 DataSet 的范型是什么，无论是 DataSet[Order] 还是其它的，其最底层进行处理的数据结构都是 InternalRow 。
// MAGIC 
// MAGIC 所以， DataSet 的范型对象在执行之前，需要通过 Encoder 转换为 InternalRow，在输入之前，需要把 InternalRow 通过 Decoder 转换为范型对象。
// MAGIC 
// MAGIC * DataSet 是一个 Spark 组件，其底层还是 RDD 。
// MAGIC * DataSet 提供了访问对象中某个字段的能力，不用像 RDD 一样每次都要针对整个对象做操作。

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # DataSet与DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 相同的地方

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 相同的DSL访问

// COMMAND ----------

df.printSchema()

ds.printSchema()

// COMMAND ----------

df.describe().show()

ds.describe().show()

// COMMAND ----------

df.select("OrderNumber","ItemName").show()

ds.select("OrderNumber","ItemName").show()

// COMMAND ----------

df.filter("OrderNumber = 16117").show()

ds.filter("OrderNumber = 16118").show()

// COMMAND ----------

df.sort(df("ProductPrice")).show()

ds.sort(ds("ProductPrice").desc).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 相同的SQL访问

// COMMAND ----------

df.createOrReplaceTempView("df")

spark.sql("select * from df where OrderNumber = 16117").show()

ds.createOrReplaceTempView("ds")

spark.sql("select * from ds where OrderNumber = 16118").show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 不同的地方

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 数据类型的不同

// COMMAND ----------

val ds = spark.range(1, 10, 2)

val df = ds.toDF()

val df1 = df.first()

val ds1 = ds.first()

// COMMAND ----------

case class Person(name: String, age: Int)

val ds = spark.createDataset(Seq(Person("wux",10),Person("labs",20)))

val df = ds.toDF()

val df1 = df.first()

val ds1 = ds.first()

// COMMAND ----------

val rdd = sc.textFile("/mnt/databrickscontainer1/restaurant-1-orders.csv")

val ds = rdd.toDS

val df = ds.toDF()

val df1 = df.first()

val ds1 = ds.first()

// COMMAND ----------

case class Order (
val OrderNumber: String,
val OrderDate: String,
val ItemName: String,
val Quantity: Integer,
val ProductPrice: Double,
val TotalProducts: Integer
)

val df = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

val ds = df.as[Order]

val df1 = df.first()

val ds1 = ds.first()

// COMMAND ----------

df.map(x => x.getClass().getName()).show(3, false)

ds.map(x => x.getClass().getName()).show(3, false)

ds.toDF().map(x => x.getClass().getName()).show(3, false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### 操作方式的不同

// COMMAND ----------

df.printSchema()
df.map(x => x.getString(2)).show(5)

ds.printSchema()
ds.map(x => x.ItemName).show(5)

// df.map(x => x.ItemName).show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### DataSet提供了编译时类型检查

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## 相互转换
// MAGIC 
// MAGIC DataFrame和DataSet之间可以相互转换。
// MAGIC 
// MAGIC 
// MAGIC **DataFrame转为DataSet**
// MAGIC 
// MAGIC df.as[ElementType] 这样可以把DataFrame转化为DataSet。
// MAGIC 
// MAGIC **DataSet转为DataFrame**
// MAGIC 
// MAGIC ds.toDF() 这样可以把DataSet转化为DataFrame。

// COMMAND ----------

case class Order (
val OrderNumber: String,
val OrderDate: String,
val ItemName: String,
val Quantity: Integer,
val ProductPrice: Double,
val TotalProducts: Integer
)

val df1 = spark.read.parquet("/mnt/databrickscontainer1/restaurant-1-orders.parquet")

val ds1 = df1.as[Order]

val df2 = ds1.toDF()
