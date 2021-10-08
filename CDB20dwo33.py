# Databricks notebook source
import pyspark

from pyspark import SparkContext

sc = SparkContext.getOrCreate()

# COMMAND ----------

## Parellelizing method - you can pass existing objects
#create the python new object
karthick = list(range(1,100000))

# COMMAND ----------

print(karthick)


# COMMAND ----------

print(type(karthick))

# COMMAND ----------

## Parellelizing method - you can pass existing python objects
newrdd = sc.parallelize(karthick)

# COMMAND ----------

print(type(newrdd))

# COMMAND ----------

print(newrdd)

# COMMAND ----------

## Parellelizing method - you can pass objects directly in parellelize method
newrdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

# COMMAND ----------

print(type(newrdd1))

# COMMAND ----------

print(newrdd1)

# COMMAND ----------

newrdd2 = sc.textFile("/FileStore/tables/karthick.txt")

# COMMAND ----------

print(newrdd2)

# COMMAND ----------

newrdd2.collect()

# COMMAND ----------

print(type(newrdd2))

# COMMAND ----------

#create new rdd from  existing rdd
newrdd3 = newrdd2

# COMMAND ----------

print(newrdd3)

# COMMAND ----------

print(type(newrdd3))

# COMMAND ----------

newrdd3.collect()

# COMMAND ----------

newrdd3.count()

# COMMAND ----------

newrdd3.collect()

# COMMAND ----------

newrdd3.take(10)

# COMMAND ----------

newrdd.count()

# COMMAND ----------

newrdd3.first()

# COMMAND ----------

newrdd.take(10)

# COMMAND ----------

newrdd3.saveAsTextFile("/FileStore/cdb21dw018/demo.txt")

# COMMAND ----------

warrdd = sc.textFile("/FileStore/tables/war.txt")

# COMMAND ----------

warrdd.collect()

# COMMAND ----------

warrdd.take(1)

# COMMAND ----------

warrdd.take(3)

# COMMAND ----------

warrdd.count()

# COMMAND ----------

warrdd.saveAsTextFile("/FileStore/wardata")

# COMMAND ----------

warmaprdd = warrdd.map(lambda x: (x,1))

# COMMAND ----------

warmaprdd.take(5)

# COMMAND ----------


