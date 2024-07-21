# Databricks notebook source
from  pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, BooleanType

df = spark.sql("select * from customer")

df2 = spark.sql("select * from productid")

# display(df)
# display(df2)

df3 = df.groupBy("name").agg(count("product_key").alias('Count'))

display(df3.filter(df3.Count ==2).orderBy(desc('name')))
