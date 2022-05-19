# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 创建事件中心命名空间：`wux-event-hubs`，通过`Shared access policies`获取连接字符串。
# MAGIC 
# MAGIC 创建事件中心：`eventhub1`
# MAGIC 
# MAGIC 安装事件中心的 Python 包

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install azure-eventhub

# COMMAND ----------

import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

producer = EventHubProducerClient.from_connection_string(conn_str="Endpoint=sb://wux-event-hubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xHOOPZpJFaeoY0NwoqQfbyIOthHxaNvWns/Q5gx5R0s=", eventhub_name="eventhub1")

# COMMAND ----------

# Create a batch.
event_data_batch = await producer.create_batch()

# Add events to the batch.
event_data_batch.add(EventData('First event2'))
event_data_batch.add(EventData('Second event2'))
event_data_batch.add(EventData('Third event'))

# Send the batch of events to the event hub.
await producer.send_batch(event_data_batch)
