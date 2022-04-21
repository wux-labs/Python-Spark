# Databricks notebook source
# MAGIC %md
# MAGIC # 共享变量

# COMMAND ----------

# MAGIC %md
# MAGIC ## 广播变量

# COMMAND ----------

from pyspark.taskcontext import TaskContext
import socket
import threading

rdd = sc.parallelize(range(1,1001),10)

cst = "a"
lst = ["a","b","c"].copy()

# Driver内的
print([(socket.gethostbyname(socket.gethostname()) + "__" + str(threading.currentThread().ident) + "__" + str(id(lst)))])

# Executor内的
# 同一个Executor内的不同分区，常量是共享的
print(rdd.map(lambda x: (socket.gethostbyname(socket.gethostname()) + "__" + str(threading.currentThread().ident) + "__" + str(id(cst)))).distinct().collect())
# 同一个Executor内的不同分区，常量是共享的
print(rdd.map(lambda x: (socket.gethostbyname(socket.gethostname()) + "__" + str(threading.currentThread().ident) + "__" + str(id(lst[0])))).distinct().collect())
# 同一个Executor内的不同分区，发送的对象可能会存在多份相同的数据
print(rdd.map(lambda x: (socket.gethostbyname(socket.gethostname()) + "__" + str(threading.currentThread().ident) + "__" + str(id(lst)))).distinct().collect())


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 从上面的案例我们可以看出，本地list对象被发送到每个分区的处理线程上使用，也就是executor内（即便是在单机环境，也会发送到对应的线程上），可能会存放多份一样的数据。
# MAGIC 
# MAGIC executor是进程，进程内的资源可以共享的，这多份一样的数据就没有必要了，它造成了内存的浪费。
# MAGIC 
# MAGIC **解决方案 -- 广播变量**
# MAGIC 
# MAGIC 如果将本地list对象标记为广播变量对象，那么当上述场景出现时，Spark只会：
# MAGIC * 给每个executor一份数据，以节省内存。而不是像上面那样每个分区的处理线程都放一份。

# COMMAND ----------

from pyspark.taskcontext import TaskContext
import socket
import threading

rdd = sc.parallelize(range(1,1001),10)

cst = "a"
lst = sc.broadcast(["a","b","c"].copy())

# Driver内的
print([(socket.gethostbyname(socket.gethostname()) + "__" + str(threading.currentThread().ident) + "__" + str(id(lst)))])

# Executor内的
# 同一个Executor内的不同分区，常量是共享的
print(rdd.map(lambda x: (socket.gethostbyname(socket.gethostname()) + "__" + str(threading.currentThread().ident) + "__" + str(id(cst)))).distinct().collect())
# 同一个Executor内的不同分区，常量是共享的
print(rdd.map(lambda x: (socket.gethostbyname(socket.gethostname()) + "__" + str(threading.currentThread().ident) + "__" + str(id(lst.value[0])))).distinct().collect())
# 同一个Executor内的不同分区，发送的对象可能会存在多份相同的数据
print(rdd.map(lambda x: (socket.gethostbyname(socket.gethostname()) + "__" + str(threading.currentThread().ident) + "__" + str(id(lst.value)))).distinct().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 累加器

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)

totals = 10

def map_func(data):
    global totals
    totals += 1
    return (data, totals)

print(rdd.map(map_func).glom().collect())
print(totals)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 从上面的例子我们看出，totals在Driver中初始化，并且在Executor中需要的时候会从Driver发送到Executor。
# MAGIC 
# MAGIC 但是在Executor中运算完成后，Executor中的totals无论变成多少，都不会影响Driver上的totals的值。
# MAGIC 
# MAGIC 我们有时候希望在Executor上运行的是统计逻辑，最终的统计结果会在Driver上也体现出来。
# MAGIC 
# MAGIC **解决方案 -- 累加器**
# MAGIC 
# MAGIC 累加器对象，构建方式：sc.accumulator(初始值)
# MAGIC 
# MAGIC 这个对象唯一不同的是：这个对象可以从各个Executor中收集他们的执行结果，作用回自己身上。

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)

totals = sc.accumulator(10)

def map_func(data):
    global totals
    totals += 1
    return (data, totals)

print(rdd.map(map_func).glom().collect())
print(totals.value)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 注意事项
# MAGIC 
# MAGIC 因为RDD是过程数据，如果RDD上执行多次Action，那么RDD可能会构建多次。
# MAGIC 
# MAGIC 如果累加器累加代码存在于重新构建的步骤中，累加器累加代码就可能被多次执行。

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)

totals = sc.accumulator(10)

def map_func(data):
    global totals
    totals += 1
    return (data, totals)

rdd2 = rdd.map(map_func)
print(rdd2.glom().collect())
print(rdd2.glom().collect())
print(rdd2.glom().collect())
print(totals.value)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 解决这个问题，可以使用缓存或者CheckPoint。

# COMMAND ----------

rdd = sc.parallelize([1,2,3,4,5,6,7,8,9],3)

totals = sc.accumulator(10)

def map_func(data):
    global totals
    totals += 1
    return (data, totals)

rdd2 = rdd.map(map_func)
rdd2.cache()

print(rdd2.glom().collect())
print(rdd2.glom().collect())
print(rdd2.glom().collect())
print(totals.value)
