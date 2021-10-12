# Databricks notebook source
import pyspark
import pyspark.sql

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

l = [('Ankit',25),('Jalfaizy',22),('saurabh',20),('Bala',26)]

# COMMAND ----------

l

# COMMAND ----------

print(type(l))

# COMMAND ----------

rdd = sc.parallelize(l)

# COMMAND ----------

print(rdd)

# COMMAND ----------

print(type(rdd))
rdd.collect()

# COMMAND ----------

people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))

# COMMAND ----------

print(type(people))

# COMMAND ----------

people.collect()

# COMMAND ----------

rdd.collect()

# COMMAND ----------

schemaPeople = sqlContext.createDataFrame(people)

# COMMAND ----------

print(schemaPeople)

# COMMAND ----------

print(type(schemaPeople))

# COMMAND ----------

schemaPeople.collect()

# COMMAND ----------

schemaPeople.take(2)

# COMMAND ----------

schemaPeople.printSchema()

# COMMAND ----------

schemaPeople.head()

# COMMAND ----------

schemaPeople.head(2)

# COMMAND ----------

schemaPeople.tail(2)

# COMMAND ----------

schemaPeople.count()

# COMMAND ----------

schemaPeople.columns

# COMMAND ----------

len(schemaPeople.columns)

# COMMAND ----------

schemaPeople.show()

# COMMAND ----------

schemaPeople.registerTempTable( "people" )

# COMMAND ----------

sqlContext.sql('select * from people where age > 20').show(5)

# COMMAND ----------


