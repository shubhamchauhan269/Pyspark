# Databricks notebook source
# DBTITLE 0,Introduction to Surragote Key
# Stategies
# 1)Monotonically_Inceasing_id - These are unique ids however they are not in continuation
# 2)ZipWithIndex()  --This will be the first choice to create key
# 2)ZipWithUniqueIndex()
# 4)row_number() Rank OVER

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE TEST01
# MAGIC (
# MAGIC   SurrogateKey BIGINT COMMENT 'SURROGATE KEY',
# MAGIC   ID BIGINT COMMENT 'IDENTIFIER',
# MAGIC   random_number DOUBLE COMMENT "RANDOM NUMBER",
# MAGIC   str1 STRING COMMENT 'STRING VALUE 1',
# MAGIC   str2 STRING COMMENT 'STRING VALUE 2',
# MAGIC   str3 STRING COMMENT 'STRING VALUE 3'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC set test.nrows = 100000;
# MAGIC set test.npartitions = 40

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with cte as (
# MAGIC   select id,rand() random_number,'one' str1,'two' str2,'three' str3 from
# MAGIC   RANGE(0,${test.nrows},1,${test.npartitions})
# MAGIC )
# MAGIC Insert into Test01
# MAGIC select monotonically_Increasing_id() as surrogatekey,* from cte

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * From test01
# MAGIC -- select min(surrogatekey) From test01
# MAGIC -- so here what is happening is if i am executing above command again and again it is inserting same surrogatekey,which is not helpful in generation unique id
# MAGIC --to takle this issue what we can do is while inserting surrogate key 
# MAGIC --select monotonically_Increasing_id()+1+(select max(surrogatekey) from test01) as surrogatekey,* from cte
# MAGIC
# MAGIC select count(*),count(distinct surrogatekey) from test01

# COMMAND ----------

#ZipWithIndex : these only works with RDD.They dont work with DataFrame.Ordering first based on the partitoning index,then ordering of item with in each partitioning is maintained.So the Scanning is done twice.Output generated here will be continous.

#ZipWithUniqueIndex : only scan 1 time and output it generate will not be continous.
