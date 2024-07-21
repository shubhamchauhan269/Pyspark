# Databricks notebook source
from  pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, BooleanType
df = spark.read.csv(path = '/FileStore/swapname.csv',header = 'True')

display(df)

sqldf = spark.sql('''
                  select id,case when id%2 =1 then coalesce(lead(Student)over(order by id),Student) 
                  else lag(Student)over(order by id)  end as Stu from {adf}
                  ''',adf = df)

# display(sqldf)


# dfodd = df.withColumn('Studentodd',func.lag(df['student']).over(Window.orderBy('id')))
# display(dfodd)

a = df.withColumn(
    'NewStudent',
    func.when(
        df.Id%2==1,
        func.coalesce(func.lead(df.Student).over(Window.orderBy(df.Id)),df.Student)
       
    ).otherwise(func.lag(df.Student).over(Window.orderBy(df.Id)))
)
display(a)

