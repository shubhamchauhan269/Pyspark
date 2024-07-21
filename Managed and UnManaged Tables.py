# Databricks notebook source

databricksuser = spark.sql("select current_user()").collect()[0][0].split('@')[0].replace(".","_")
# print(databricksuser)


db_name = "demo_train_{}_db".format(str(databricksuser))
t_name = "Iris_Managed"



# COMMAND ----------

spark.sql("Drop database if exists demo_train_{}_db cascade".format(str(databricksuser)))
spark.sql("Create database if not exists demo_train_{}_db".format(str(databricksuser)))
spark.sql("use demo_train_{}_db".format(str(databricksuser)))


# COMMAND ----------

df = spark.read.csv(path = '/FileStore/Iris.csv',header=True)

df.write.mode("overwrite").saveAsTable(t_name)


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE Table Extended Iris_Managed
# MAGIC
# MAGIC -- select * from Iris_Managed

# COMMAND ----------


#UN_Managed Table as we have given the path for the table
df.write.option('path','/FileStore/tables').saveAsTable("Iris_Unmanaged")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- describe table extended Iris_Unmanaged
# MAGIC select * From Iris_Unmanaged

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/tables')


# COMMAND ----------

spark.read.delta('dbfs:/FileStore/tables/part-00000-6b304583-1e94-4c42-838c-9623fa8ab13a-c000.snappy.parquet').show(5)


