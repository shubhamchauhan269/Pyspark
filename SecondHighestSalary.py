# Databricks notebook source
from  pyspark.sql.functions import *

# Need to import to use date time 
from datetime import datetime, date 
from pyspark.sql import Row 
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, BooleanType

df = spark.read.csv(path = '/FileStore/SecondHighestSalrycsvfi2.csv',header = True)

df = df.withColumn('salary',col("salary").cast(IntegerType()))
display(df)

df2 = spark.sql('''
               with cte as 
               (
               select *,row_number()over(order by salary desc) as rn 
               from {empdf}
               )
               select salary from cte where rn = 2
               ''',empdf= df)

dataf = df.withColumn('rn',func.row_number().over(Window.orderBy(desc('salary'))))

dfmain = dataf.withColumn('SecondHighestSalary',when(dataf.rn==2,dataf.salary).otherwise('NULL'))

dfmain.select(dfmain['SecondHighestSalary']).orderBy(asc(dfmain.SecondHighestSalary)).head()

