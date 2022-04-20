# Databricks notebook source
# MAGIC %md
# MAGIC # RDD的持久化

# COMMAND ----------

# MAGIC %md
# MAGIC ## RDD的数据是过程数据
# MAGIC 
# MAGIC RDD之间进行相互迭代计算（Transformation转换），当执行开始后，新RDD生成，老RDD消失。
# MAGIC 
# MAGIC RDD的数据只是过程数据，只在处理的过程中存在，一旦处理完成，就不见了。
# MAGIC 
# MAGIC 在下一次要用到RDD的数据的时候，再根据血缘关系，从头重新处理一遍RDD的数据。
# MAGIC 
# MAGIC > 这个特性可以最大化的利用资源，老的RDD从内存中清理，给后续的计算腾出内存空间。

# COMMAND ----------

import numpy as np
import datetime
import time

rdd1 = sc.parallelize([1,2,3])

rdd2 = rdd1.map(lambda x: (x, np.random.randint(500)))

rdd3 = rdd2.map(lambda x: (x[0], x[1], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

time.sleep(5)
result1 = rdd3.collect()
print(result1)
print(result1)
print(result1)

time.sleep(5)
result2 = rdd3.collect()
print(result2)
print(result2)
print(result2)


time.sleep(5)
result3 = rdd3.collect()
print(result3)
print(result3)
print(result3)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 缓存
# MAGIC 
# MAGIC 对于上述的场景 rdd1 -> rdd2 -> rdd3 执行了多次，需要优化。
# MAGIC 
# MAGIC 如果rdd3不消失，那么 rdd1 -> rdd2 -> rdd3 就不会执行多次了。
# MAGIC 
# MAGIC **RDD的缓存技术：** Spark提供了缓存API，可以让我们通过调用API，将指定的RDD的数据保留下来。
# MAGIC 
# MAGIC ```
# MAGIC rdd3.cache()                                    # 缓存到内存中
# MAGIC rdd3.persist(StorageLevel.MEMORY_ONLY)          # 仅内存缓存
# MAGIC rdd3.persist(StorageLevel.MEMORY_ONLY_2)        # 仅内存缓存，2个副本
# MAGIC rdd3.persist(StorageLevel.DISK_ONLY)            # 仅硬盘缓存
# MAGIC rdd3.persist(StorageLevel.DISK_ONLY_2)          # 仅硬盘缓存，2个副本
# MAGIC rdd3.persist(StorageLevel.DISK_ONLY_3)          # 仅硬盘缓存，3个副本
# MAGIC rdd3.persist(StorageLevel.MEMORY_AND_DISK)      # 先放内存，内存不够放硬盘
# MAGIC rdd3.persist(StorageLevel.MEMORY_AND_DISK_2)    # 先放内存，内存不够放硬盘，2个副本
# MAGIC rdd3.persist(StorageLevel.OFF_HEAP)             # 堆外内存（系统内存）
# MAGIC 
# MAGIC # 一般建议使用 rdd3.persist(StorageLevel.MEMORY_AND_DISK)
# MAGIC 
# MAGIC # 主动清理缓存
# MAGIC rdd3.unpersist()
# MAGIC ```

# COMMAND ----------

import numpy as np
import datetime
import time

rdd1 = sc.parallelize([1,2,3])

rdd2 = rdd1.map(lambda x: (x, np.random.randint(500)))

rdd3 = rdd2.map(lambda x: (x[0], x[1], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

# 仅缓存到内存
rdd3.cache()

time.sleep(5)
result1 = rdd3.collect()
print(result1)
print(result1)
print(result1)

time.sleep(5)
result2 = rdd3.collect()
print(result2)
print(result2)
print(result2)


time.sleep(5)
result3 = rdd3.collect()
print(result3)
print(result3)
print(result3)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 缓存的特点
# MAGIC 
# MAGIC * 缓存技术可以将RDD过程数据持久化保存到内存或者硬盘上
# MAGIC * 这个保存在设定上是不安全的
# MAGIC 
# MAGIC > 缓存的数据在设计上认为有丢失的风险。
# MAGIC 
# MAGIC 所以，缓存有一个特点，就是**会保留RDD之间的血缘关系**。
# MAGIC 
# MAGIC 一旦缓存丢失，可以基于血缘关系记录，重新计算这个RDD的数据。
# MAGIC 
# MAGIC > **缓存如何丢失：**  
# MAGIC > 在内存中的缓存时不安全的，比如断电\内存不足，会把缓存清理掉，释放资源给计算  
# MAGIC > 硬盘中的数据也有可能因硬盘损坏而丢失
# MAGIC 
# MAGIC RDD的数据是按照分区，分别缓存到Executor的内存或硬盘，是分散缓存的。

# COMMAND ----------

# MAGIC %md
# MAGIC ## CheckPoint
# MAGIC 
# MAGIC CheckPoint技术，也是将RDD的数据保存下来，但是它**仅支持硬盘存储**。
# MAGIC 
# MAGIC 并且，CheckPoint：
# MAGIC * 它被设计认为是安全的
# MAGIC * 它不保留血缘关系
# MAGIC 
# MAGIC CheckPoint是将各个分区的数据集中保存到硬盘，而不是分散存储。
# MAGIC 
# MAGIC 一般将CheckPoint的数据保存到HDFS上，这样数据就比较安全了。
# MAGIC 
# MAGIC CheckPoint不会立即执行，需要先执行Action，并且CheckPoint在代码中需要放到Action之前。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 缓存和CheckPoint的对比
# MAGIC 
# MAGIC * CheckPoint不管分区数量多少，风险是一样的；缓存的分区越多风险越高。
# MAGIC * CheckPoint支持写入HDFS；缓存不支持写入HDFS。
# MAGIC * CheckPoint不支持内存；缓存支持写内存，如果缓存写内存，性能比CheckPoint要好一些。
# MAGIC * CheckPoint因为设计认为是安全的，所以不保留血缘关系；缓存因为设计认为是不安全的，所以保留血缘关系。

# COMMAND ----------

import numpy as np
import datetime
import time

sc.setCheckpointDir("/mnt/databrickscontainer1/checkpoint")

rdd1 = sc.parallelize([1,2,3])

rdd2 = rdd1.map(lambda x: (x, np.random.randint(500)))

rdd3 = rdd2.map(lambda x: (x[0], x[1], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

rdd3.checkpoint()

time.sleep(5)
result1 = rdd3.collect()
print(result1)
print(result1)
print(result1)

time.sleep(5)
result2 = rdd3.collect()
print(result2)
print(result2)
print(result2)

time.sleep(5)
result3 = rdd3.collect()
print(result3)
print(result3)
print(result3)

time.sleep(5)
result4 = rdd3.collect()
print(result4)
print(result4)
print(result4)

time.sleep(5)
result5 = rdd3.collect()
print(result5)
print(result5)
print(result5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 注意
# MAGIC 
# MAGIC CheckPoint是一种重量级的使用，也就是RDD的重新计算成本很高的时候，或者数据量很大，我们采用CheckPoint比较合适。
# MAGIC 
# MAGIC 如果数据量小，或者RDD重新计算是非常快的，用CheckPoint就没啥必要，直接缓存就可以了。
# MAGIC 
# MAGIC **CheckPoint需要放在对应RDD的Action之前，对RDD才有持久化的效果，放在Action之后，即便后续还有RDD上的Action操作，CheckPoint也不起作用；缓存会对缓存语句后面的Action起作用。**
# MAGIC 
# MAGIC 其他RDD的Action对当前RDD的CheckPoint没有影响。
# MAGIC 
# MAGIC > Cache和CheckPoint两个API都不是Action类型的  
# MAGIC > 所以想要他们工作，必须在后面接上Action  
# MAGIC > 接Action算子的目的是让RDD有数据，而不是为了让Cache和CheckPoint工作

# COMMAND ----------

import numpy as np
import datetime
import time

sc.setCheckpointDir("/mnt/databrickscontainer1/checkpoint")

rdd1 = sc.parallelize([1,2,3])

rdd2 = rdd1.map(lambda x: (x, np.random.randint(500)))

rdd3 = rdd2.map(lambda x: (x[0], x[1], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

# 放在Action之前，看看后续RDD的Action的结果，体会一下Cache与CheckPoint的持久化效果
# rdd3.cache()
# rdd3.checkpoint()

time.sleep(5)
result1 = rdd3.collect()
print(result1)
print(result1)
print(result1)

# 放在Action之后，看看后续RDD的Action的结果，体会一下Cache与CheckPoint的持久化效果
rdd3.cache()
# rdd3.checkpoint()

time.sleep(5)
result2 = rdd3.collect()
print(result2)
print(result2)
print(result2)

time.sleep(5)
result3 = rdd3.collect()
print(result3)
print(result3)
print(result3)

time.sleep(5)
result4 = rdd3.collect()
print(result4)
print(result4)
print(result4)

time.sleep(5)
result5 = rdd3.collect()
print(result5)
print(result5)
print(result5)

# COMMAND ----------

import numpy as np
import datetime
import time

sc.setCheckpointDir("/mnt/databrickscontainer1/checkpoint")

rdd1 = sc.parallelize([1,2,3])

rdd2 = rdd1.map(lambda x: (x, np.random.randint(500)))

rdd3 = rdd2.map(lambda x: (x[0], x[1], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

time.sleep(5)
result1 = rdd2.collect()
print(result1)
print(result1)
print(result1)

# 如果CheckPoint放在其他RDD的Action之后，有没有影响？
rdd3.checkpoint()

time.sleep(5)
result2 = rdd3.collect()
print(result2)
print(result2)
print(result2)

time.sleep(5)
result3 = rdd3.collect()
print(result3)
print(result3)
print(result3)

time.sleep(5)
result4 = rdd3.collect()
print(result4)
print(result4)
print(result4)

time.sleep(5)
result5 = rdd3.collect()
print(result5)
print(result5)
print(result5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 清理缓存
# MAGIC 
# MAGIC 如果不需要保留缓存了，需要手动清理以释放资源。
# MAGIC 
# MAGIC * unpersist()

# COMMAND ----------

import numpy as np
import datetime
import time

rdd1 = sc.parallelize([1,2,3])

rdd2 = rdd1.map(lambda x: (x, np.random.randint(500)))

rdd3 = rdd2.map(lambda x: (x[0], x[1], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

rdd3.cache()

time.sleep(5)
result1 = rdd3.collect()
print(result1)
print(result1)
print(result1)

time.sleep(5)
result2 = rdd3.collect()
print(result2)
print(result2)
print(result2)

time.sleep(5)
result3 = rdd3.collect()
print(result3)
print(result3)
print(result3)

rdd3.unpersist()

time.sleep(5)
result4 = rdd3.collect()
print(result4)
print(result4)
print(result4)

time.sleep(5)
result5 = rdd3.collect()
print(result5)
print(result5)
print(result5)
