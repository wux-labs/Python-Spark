# Databricks notebook source
# MAGIC %md
# MAGIC 常量设置

# COMMAND ----------

blob_account_name = "databricksaccount1"
blob_container_name = "databrickscontainer1"
blob_sas_token = r""

wasbs_endpoint = 'wasbs://%s@%s.blob.core.windows.net' % (blob_container_name, blob_account_name)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)

dfs_endpoint = wasbs_endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC 挂载 Azure Blob 存储容器

# COMMAND ----------

dbutils.fs.mount(
  source = wasbs_endpoint,
  mount_point = "/mnt/%s" % blob_container_name,
  extra_configs = {'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name):blob_sas_token}
)
