# Databricks notebook source

# Read a CSV  file in Spark 
df1 = spark.read.csv(path='/FileStore/Quality_Snowflake.csv',header= True)
# df1.select("EVAL_ID").distinct().show()
# display(df1)
# df1.show()

#Changing the DataType of Column 
df = df1.withColumn('MAX_Q_SCORE', df1['MAX_Q_SCORE'].cast('float')) 
#Creating the table  and keeping overwriteschema = true so that it can handle change in schema
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("QuaitySnowflake")
display(spark.read.table("QuaitySnowflake").select("msid","MAX_Q_SCORE").groupBy("msid").sum("MAX_Q_SCORE"))


