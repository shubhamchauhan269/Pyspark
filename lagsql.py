# Databricks notebook source

from  pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import IntegerType, BooleanType

# df_schema = StructType([
#     StructField("Customerid ",StringType(),True),
#     StructField("Date",StringType(),True),
#     StructField("Sale",IntegerType(),True)
# ])

df = spark.read.csv(path='/FileStore/SALES-2.csv',header= True)

display(df.printSchema())

# columns=["Customerid ", "Date"]
# display(df.orderBy(*columns))
# display(df)

df = df.withColumnRenamed("Customerid ","Customerid")
df = df.withColumn('Date',to_date('Date'))
df = df.withColumn('Sales',col("Sales").cast(IntegerType()))

# display(df.printSchema())
# display(df)

#To Crete a Temo Table
# df.createOrReplaceTempView('sales')

df1 = spark.sql('''

 WITH CTE1 AS
 (
 SELECT CUSTOMERID,AVG(sALES) as AVGSales FROM {salesdf} 
 GROUP BY Customerid

 )
 ,cte as
 (
Select *,COALESCE(DATEDIFF(DAY,LAG(DATE)OVER(PARTITION BY CUSTOMERID ORDER BY CUSTOMERID,DATE),DATE),-1) AS DATELAG From {salesdf} 
)
SELECT cte.CUSTOMERID,AVG(DATELAG) AS AVGLG ,AVGSales FROM  CTE  join cte1 on cte.CUSTOMERID = cte1.CUSTOMERID WHERE DATELAG<>-1
GROUP BY cte.CUSTOMERID,AVGSales

 ''',salesdf=df)
 
# display(df1)

DFAvgSales = df.groupBy('Customerid').agg(avg('Sales').alias('AvgSaleValue')).orderBy('Customerid')

# display(DFAvgSales)

DFAvgLag = df.withColumn('PrevisosDay',func.lag(df['Date']).over(Window.partitionBy('Customerid').orderBy('Customerid','Date')))

result = DFAvgLag.withColumn('LagDiff',datediff(DFAvgLag['Date'],DFAvgLag['PrevisosDay'])).filter(DFAvgLag.PrevisosDay.isNotNull())

FinalAvgLag = result.groupBy('Customerid').agg(avg('LagDiff').alias('AvgLagDiffValue')).orderBy('Customerid')

DFFinal = DFAvgSales.join(FinalAvgLag,FinalAvgLag.Customerid == DFAvgSales.Customerid,'inner').select(DFAvgSales["*"],FinalAvgLag['AvgLagDiffValue'])

display(DFFinal)


