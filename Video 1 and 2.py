# Databricks notebook source
from pyspark.sql.types import *
df = spark.read.csv(path= '/FileStore/Iris.csv',header='True')
# display(df)
# print(df)

#Creating a RDD From DataFrame
# p = df.rdd.map(list).take(15)
# display(p)

# df.collect()
# df.head(5)
# df.select('SepalLengthCm','SepalWidthCm').show()
# df.schema  --This is how we define schema in the dataframe
# df.dtypes #-- To see the datatyes of the dataframe
df1 = df.select(df.Id,df.SepalLengthCm.cast("float"))

# df1.dtypes

# df1.sort("Id",ascending = False).show()
# df.drop('SepalLengthCm').show('') --It will drop the column and show the rest of the dataframe


# df.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("Iris")
Iris_Schema = StructType(
    [
        StructField("SepalLengthCm",FloatType(),True),
        StructField("SepalWidthCm",FloatType(),True),
        StructField("PetalLengthCm",FloatType(),True),
        StructField("PetalWidthCm",FloatType(),True),
        StructField("Species",StringType(),True)
    ]
)

df2 = spark.read.csv(path= '/FileStore/Iris.csv',header='True',schema =Iris_Schema)
df2.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("Iris")

# df2.agg({"SepalWidthCm":"average"}).show()
# df2.agg({"SepalWidthCm":"sum"}).show()
# df2.groupBy("Species").agg({'SepalWidthCm':'sum'}).show()

df.createOrReplaceTempView('Shubham_view')

sqlContext.tables().show()

tempdf = spark.table('Shubham_view')
tempdf.show()

# df2[df2.Species.isin(['setosa'])].show()
# display(spark.read.table('Iris'))

# display(df2.select('Species').distinct())

J1 = spark.read.csv(path= '/FileStore/Iris.csv',header='True')
J2 = spark.read.csv(path= '/FileStore/Iris.csv',header='True')


# J1.join(other =J2,on = 'ID',how ='outer').select(J1.SepalWidthCm,J2.PetalLengthCm).show()
# J1.join(other =J2,on = 'ID',how ='inner').select(J1.SepalWidthCm,J2.PetalLengthCm).show()
# J1.join(other =J2,on = 'ID',how ='full').select(J1.SepalWidthCm,J2.PetalLengthCm).show()

# J1.union(J2).show()

