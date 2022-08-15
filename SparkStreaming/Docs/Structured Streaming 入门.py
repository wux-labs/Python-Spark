# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # 概述
# MAGIC 
# MAGIC 结构化流（Structured Streaming）式处理是在Spark SQL引擎上构建的可扩展且容错的流处理引擎。可以像对静态数据表达批处理计算一样表达流式计算。Spark SQL引擎将负责以增量方式连续运行它，并在流数据继续到达时更新最终结果。系统通过检查点和预写日志确保端到端的一次容错保证。
# MAGIC 
# MAGIC > 简而言之，结构化流式处理提供快速、可扩展、容错、端到端的一次性流处理，而无需用户对流进行推理。

# COMMAND ----------


