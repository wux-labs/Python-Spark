# Databricks notebook source
# MAGIC %md
# MAGIC 常量设置

# COMMAND ----------

blob_account_name = "databricksaccount1"
blob_container_name = "databrickscontainer1"
blob_sas_token = r"sp=racwdli&st=2022-06-06T15:39:22Z&se=2022-06-29T23:39:22Z&sv=2020-08-04&sr=c&sig=3IN9wmkA72G6buSHDlZah52Nr2sDvRvh0LaWSjjSoGo%3D"

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
