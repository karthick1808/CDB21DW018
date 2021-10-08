# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark import SparkContext

# COMMAND ----------

sc = SparkContext.getOrCreate()

# COMMAND ----------

words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)

# COMMAND ----------

print(words)

# COMMAND ----------

print(type(words))

# COMMAND ----------

words.collect()

# COMMAND ----------

words.count()

# COMMAND ----------

words.take(8)

# COMMAND ----------

newrdd = words.map(lambda x: (x,1))

# COMMAND ----------

newrdd.collect()

# COMMAND ----------

newrdd.take(2)

# COMMAND ----------

words_filter = words.filter(lambda x: "spark" in x)

# COMMAND ----------

words_filter.collect()

# COMMAND ----------

selvam = sc.parallelize([1,2,3,4,5,6,7,8,9,10])

# COMMAND ----------

newrdd1 = selvam.map(lambda x: (x+10))

# COMMAND ----------

newrdd1.collect()

# COMMAND ----------

words.collect()

# COMMAND ----------

words_filter = words.filter(lambda x: 'spark' in x)

# COMMAND ----------

words_filter.collect()

# COMMAND ----------

warrdd = sc.textFile("/FileStore/tables/karthick.txt")

# COMMAND ----------

warrdd.collect()

# COMMAND ----------

warrdd.count()

# COMMAND ----------

karthickflatmap = warrdd.flatMap(lambda x: x.split(" "))

# COMMAND ----------

warrdd.take(1)

# COMMAND ----------

karthickflatmap.take(6)

# COMMAND ----------

warrdd.count()

# COMMAND ----------

karthickflatmap.count()

# COMMAND ----------

nums = sc.parallelize([1, 2, 3, 4, 5])

# COMMAND ----------

nums.collect()

# COMMAND ----------

from operator import add

# COMMAND ----------

adding = nums.reduce(add)

# COMMAND ----------

print(adding)

# COMMAND ----------

print(type(adding))

# COMMAND ----------

x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])

# COMMAND ----------

x.t

# COMMAND ----------

joined = x.join(y)

# COMMAND ----------

joined.collect()

# COMMAND ----------

joined.first()

# COMMAND ----------


