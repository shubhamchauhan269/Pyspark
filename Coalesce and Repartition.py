# Databricks notebook source
df = spark.read.csv(path = '/FileStore/Iris2.csv',header = True)


# df.rdd.getNumPartitions()

# spark.conf.get("spark.sql.files.maxPartitionBytes")

# In case of compressed file we always have single partitions

# COMMAND ----------

df2 = df.repartition(2).write.mode('overwrite').partitionBy('Species').parquet('/dbfs/FileStore/')

# COMMAND ----------

# MAGIC %fs rm -r '/dbfs/FileStore/'

# COMMAND ----------

df2 = df.repartition(2).write.mode('overwrite').parquet('/dbfs/FileStore/')

# COMMAND ----------

df3 = df.coalesce(1).write.mode('overwrite').partitionBy('Species').parquet('/dbfs/FileStore/')
