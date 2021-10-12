# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

movies = spark.read.csv("/FileStore/tables/movies-4.csv", header=True)

# COMMAND ----------

print(type(movies))

# COMMAND ----------

movies.collect()

# COMMAND ----------

movies.collect()

# COMMAND ----------

movies.take(2)

# COMMAND ----------

movies.count()

# COMMAND ----------

movies.first()

# COMMAND ----------

movies.head()

# COMMAND ----------

movies.head(3)

# COMMAND ----------

movies.show()

# COMMAND ----------

movies.show(25)

# COMMAND ----------

movies.printSchema()

# COMMAND ----------

karthick = spark.read.csv("/FileStore/tables/imdb.csv", header = True,inferSchema = True)

# COMMAND ----------

karthick.printSchema()

# COMMAND ----------

len(karthick.columns)

# COMMAND ----------

karthick.columns

# COMMAND ----------

karthick.collect()

# COMMAND ----------

karthick.describe('class').show()

# COMMAND ----------

karthick.registerTempTable('selvam')

# COMMAND ----------

sqlContext.sql('select * from selvam where class > 0').show()

# COMMAND ----------


