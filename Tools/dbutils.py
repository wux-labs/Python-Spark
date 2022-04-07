# Databricks notebook source
# MAGIC %md
# MAGIC `dbutils`工具的简单命令
# MAGIC * 挂载 Azure Blob存储容器  
# MAGIC dbutils.fs.mount(source = ..., mount_point = ..., extra_configs = {})
# MAGIC * 列出目录下的文件  
# MAGIC dbutils.fs.ls("/FileStore/shared_uploads/wux_labs@outlook.com")
# MAGIC * 删除指定文件  
# MAGIC dbutils.fs.rm("/FileStore/shared_uploads/wux_labs@outlook.com/WordCount.txt")
# MAGIC * 卸载挂载点  
# MAGIC dbutils.fs.unmount(mount_point)

# COMMAND ----------

blob_account_name = "databricksstorage01"
blob_container_name = "databrickscontainer01"
blob_sas_token = r"sas_token"

wasbs_endpoint = 'wasbs://%s@%s.blob.core.windows.net' % (blob_container_name, blob_account_name)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)

dfs_endpoint = wasbs_endpoint

# COMMAND ----------

dbutils.fs.mount(
  source = wasbs_endpoint,
  mount_point = "/mnt/%s" % blob_container_name,
  extra_configs = {'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name):blob_sas_token}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/%s" % blob_container_name)

# COMMAND ----------

dbutils.fs.unmount("/mnt/%s" % blob_container_name)
