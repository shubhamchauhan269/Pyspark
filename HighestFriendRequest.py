# Databricks notebook source
from  pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, BooleanType
df = spark.read.csv(path = '/FileStore/friendreqest.csv',header=True)


df = df.withColumn('requester_id',col("requester_id").cast(IntegerType()))
df = df.withColumn('accepter_id',col("accepter_id").cast(IntegerType()))
df = df.drop('_c2')
display(df)

sqldf = spark.sql('''
                  with cte as
                    (
                        select requester_id as id,count(*) as num from {tdf} group by requester_id
                        Union all
                        select accepter_id as id,count(*) as num from {tdf} group by accepter_id


                    ),cte2(
                    select  id,sum(num) as num from cte
                    group by id
                    )
                    select id,num from cte2 order by num desc limit 1
                    
                  ''',tdf = df)

display(sqldf)

df1 = df.groupBy("requester_id").count()
df2 = df.groupBy("accepter_id").count()

df3 = df1.union(df2)

df4 = df3.filter(df3['requester_id'].isNotNull()).groupBy("requester_id").agg(sum('count').alias('num'))
display(df4.orderBy(desc('num')).head())


