# Databricks notebook source
# MAGIC %md
# MAGIC # restaurant-orders
# MAGIC 
# MAGIC 从订单列表中统计 "Plain Papadum","Butter Chicken","Bengal Fry Fish" 这三样产品的销售总数。

# COMMAND ----------

import datetime

# 读取数据文件
fileRdd = sc.textFile("/mnt/databrickscontainer1/restaurant-1-orders.csv",4)

# 广播变量，减少内存中相同数据的份数
items = sc.broadcast(["Plain Papadum","Butter Chicken","Bengal Fry Fish"])
# 累加器
totals = sc.accumulator(0)

# 将订单中的标题行去掉
dataRdd = fileRdd.map(lambda x: x.split(",")).filter(lambda x: x[0] != "Order Number")

# print(dataRdd.map(lambda x:x[2]).distinct().collect())
# print(dataRdd.map(lambda x: x[2] in items.value).collect())

# 使用广播变量，过滤出列表中列出的产品及销售数量
saleRdd = dataRdd.filter(lambda x: x[2] in items.value).map(lambda x: (x[2],int(x[3])))

# 仅保留销售数量
quantityRdd = saleRdd.map(lambda x: x[1])

# 缓存一下
quantityRdd.cache()

# 使用reduce计算数量
print(quantityRdd.reduce(lambda a,b: a + b))
# 使用fold计算数量
print(quantityRdd.fold(0, lambda a,b: a + b))
# 使用累加器计算数量
print(totals)
def sum_func(data):
    global totals
    totals += data

quantityRdd.foreach(sum_func)
print(totals)
