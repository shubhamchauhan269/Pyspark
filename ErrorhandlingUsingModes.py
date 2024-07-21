# Databricks notebook source
#How to Handle Bad records

from  pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import IntegerType, BooleanType,DoubleType

iris_schema = StructType([
    StructField("Id",IntegerType(),False),
    StructField("SepalLengthCm",DoubleType(),False),
    StructField("SepalWidthCm",DoubleType(),False),
    StructField("PetalLengthCm",DoubleType(),False),
    StructField("PetalWidthCm",DoubleType(),False),
    StructField("Species",StringType(),True),
    StructField("Corrupt_Records",StringType(),True)
]
                         )

df = spark.read.csv(path = '/FileStore/Iris2.csv' ,header = True)
# display(df.printSchema())
# df.show(21)

df_permissive = (spark.read.option("mode","PERMISSIVE").option("columnNameofCorruptRecord","Corrupt_Records").csv(path= '/FileStore/Iris2.csv',header = True,schema = iris_schema,sep=','))
# df_permissive.show(21,False)


df_MAlformed = spark.read.option("mode","DROPMALFORMED").option("columnNameOfCorruptRecord","Corrupt_Records").csv(path= '/FileStore/Iris2.csv',header = True,schema = iris_schema)
# df_MAlformed.show()

df_FailFast = spark.read.option("mode","FAILFAST").option("columnNameOfCorruptRecord","Corrupt_Records").csv(path= '/FileStore/Iris2.csv',header = True,schema = iris_schema)
# df_FailFast.show()

badrecords = spark.read.option("badRecordPath","dbfs:/FileStore/").csv(path= '/FileStore/Iris2.csv',header = True,schema = iris_schema)
badrecords.show()



