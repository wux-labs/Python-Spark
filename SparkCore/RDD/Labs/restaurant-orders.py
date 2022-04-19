# Databricks notebook source
# MAGIC %md
# MAGIC # restaurant-orders
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
