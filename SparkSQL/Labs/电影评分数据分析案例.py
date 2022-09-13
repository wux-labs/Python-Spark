# Databricks notebook source
# MAGIC %md
# MAGIC # 电影评分数据分析案例
# MAGIC 
# MAGIC 电影评分数据集：
# MAGIC ```
# MAGIC u.data -- 
# MAGIC The full u data set, 100000 ratings by 943 users on 1682 items.
# MAGIC Each user has rated at least 20 movies.
# MAGIC Users and items are numbered consecutively from 1.
# MAGIC The data is randomly ordered.
# MAGIC This is a tab separated list of:
# MAGIC user id | item id | rating | timestamp.
# MAGIC ```
# MAGIC 我们可以了解到：
# MAGIC * 数据集包含 943个用户对1682部电影的100000条评分数据
# MAGIC * 每个用户至少对20部电影进行了评分
# MAGIC * 电影ID和用户ID都是从1开始的连续编号
# MAGIC * 数据是随机排序的
# MAGIC * 数据用tab进行分割，并且包含：用户ID、电影ID、评分、时间
# MAGIC 
# MAGIC 需求：
# MAGIC * 查询用户平均分
# MAGIC * 查询电影平均分
# MAGIC * 查询大于平均分的电影的数量
# MAGIC * 查询高分电影（>3）中打分次数最多的用户，并求出此人打的平均分
# MAGIC * 查询每个用户的平均打分、最低打分、最高打分
# MAGIC * 查询被评分超过100次的电影的平均分排名前10的电影

# COMMAND ----------

# MAGIC %md
# MAGIC ## 加载数据并简单看一下

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType

schema = StructType().add("user_id", StringType(), nullable=True).\
add("movie_id", StringType(), nullable=True).\
add("rank", IntegerType(), nullable=True).\
add("ts", StringType(), nullable=True)

df = spark.read.csv("/mnt/databrickscontainer1/u.data", schema=schema, sep="\t")

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用DSL语法风格

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询用户平均分

# COMMAND ----------

display(df.groupBy("user_id").agg(F.mean("rank"), F.round(F.mean("rank"), 2).alias("avg_rank")).orderBy("avg_rank",ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询电影平均分

# COMMAND ----------

display(df.groupBy("movie_id").agg(F.mean("rank"), F.round(F.mean("rank"), 2).alias("avg_rank")).orderBy("avg_rank",ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询大于平均分的电影的数量

# COMMAND ----------

print(df.where(df["rank"] > df.select(F.mean("rank").alias("rank")).first()["rank"]).groupBy("movie_id").count().count())

display(df.where(df["rank"] > df.select(F.mean("rank").alias("rank")).first()["rank"]).groupBy("movie_id").count())

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询高分电影（>3）中打分次数最多的用户，并求出此人打的平均分

# COMMAND ----------

print(df.where("rank > 3").groupBy("user_id").count().orderBy("count", ascending=False).first()["user_id"])

display(df.where(df["user_id"] == df.where("rank > 3").groupBy("user_id").count().orderBy("count", ascending=False).first()["user_id"]).agg(F.mean("rank")))

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询每个用户的平均打分、最低打分、最高打分

# COMMAND ----------

display(df.groupBy("user_id").agg(F.mean("rank"), F.min("rank"), F.max("rank")))

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询被评分超过100次的电影的平均分排名前10的电影

# COMMAND ----------

df.groupBy("movie_id").agg(F.count("movie_id").alias("count"),F.mean("rank").alias("mean_rank")).show()

# COMMAND ----------

df.groupBy("movie_id").agg(F.count("movie_id").alias("count"),F.mean("rank").alias("mean_rank")).where("count > 100").show()

# COMMAND ----------

display(df.groupBy("movie_id").agg(F.count("movie_id").alias("count"),F.mean("rank").alias("mean_rank")).where("count > 100").orderBy("mean_rank", ascending=False).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用SQL语法风格

# COMMAND ----------

df.createOrReplaceTempView("movie_rank")

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询用户平均分

# COMMAND ----------

# MAGIC %sql
# MAGIC select user_id, mean(rank), round(mean(rank),2) from movie_rank group by user_id order by mean(rank) desc

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询电影平均分

# COMMAND ----------

# MAGIC %sql
# MAGIC select movie_id, mean(rank), round(mean(rank),2) from movie_rank group by movie_id order by mean(rank) desc

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询大于平均分的电影的数量

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select avg(rank) from movie_rank

# COMMAND ----------

# MAGIC %sql
# MAGIC select movie_id,count(*) from movie_rank where rank > (select avg(rank) from movie_rank) group by movie_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from (
# MAGIC select movie_id,count(*) from movie_rank where rank > (select avg(rank) from movie_rank) group by movie_id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询高分电影（>3）中打分次数最多的用户，并求出此人打的平均分

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select user_id from movie_rank where rank > 3 group by user_id order by count(1) desc limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select user_id, avg(rank), round(avg(rank), 2) from movie_rank where user_id = (select user_id from movie_rank where rank > 3 group by user_id order by count(1) desc limit 1) group by user_id

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询每个用户的平均打分、最低打分、最高打分

# COMMAND ----------

# MAGIC %sql
# MAGIC select user_id, avg(rank), min(rank), max(rank) from movie_rank group by user_id

# COMMAND ----------

# MAGIC %md
# MAGIC * 查询被评分超过100次的电影的平均分排名前10的电影

# COMMAND ----------

# MAGIC %sql
# MAGIC select movie_id, count(*), mean(rank) from movie_rank group by movie_id having count(1) > 100 order by 3 desc limit 10
