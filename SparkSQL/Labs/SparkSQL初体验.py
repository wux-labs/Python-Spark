# Databricks notebook source
# MAGIC %md
# MAGIC # SparkSQL初体验

# COMMAND ----------

# MAGIC %md
# MAGIC ## SparkCore的入口及数据抽象

# COMMAND ----------

# MAGIC %md
# MAGIC ### SparkContext对象

# COMMAND ----------

sc

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDD数据抽象

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9])

print(type(rdd))
print(rdd.glom().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## SparkSQL的入口及数据抽象

# COMMAND ----------

# MAGIC %md
# MAGIC ### SparkSession对象

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### 从SparkSession获取SparkContext

# COMMAND ----------

spark.sparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC ### 使用RDD来存放数据

# COMMAND ----------

rdd = spark.sparkContext.textFile("/mnt/databrickscontainer1/restaurant-1-orders.csv",4)
datas = rdd.collect()
for data in datas:
    print(data)

# COMMAND ----------

# MAGIC %md
# MAGIC 要取数据怎么办？

# COMMAND ----------

print(rdd.filter(lambda x: x.split(",")[0] == "16115").collect())
print(rdd.filter(lambda x: x.split(",")[3] == "25").collect())
print(rdd.filter(lambda x: x.split(",")[3] == "25").map(lambda x:x.split(",")[2]).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 使用DataFrame来存放数据

# COMMAND ----------

df = spark.read.csv("/mnt/databrickscontainer1/restaurant-1-orders.csv", header=True)

print(type(df))
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame的API操作

# COMMAND ----------

df.where("Quantity=5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame的SQL操作
# MAGIC 
# MAGIC 需要先将DataFrame注册成表，给一个名称。

# COMMAND ----------

df.createOrReplaceTempView("restaurant_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC 代码中使用sql。

# COMMAND ----------

spark.sql("select * from restaurant_orders where Quantity = 10").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks还支持直接写SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from restaurant_orders where Quantity > 20
