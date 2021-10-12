# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

train =  spark.read.csv("/FileStore/tables/train-1.csv",header=True,inferSchema=True)

# COMMAND ----------

test =  spark.read.csv("/FileStore/tables/test.csv",header=True,inferSchema=True)

# COMMAND ----------

train.printSchema()

# COMMAND ----------

test.printSchema()

# COMMAND ----------

train.head(5)

# COMMAND ----------

test.head()

# COMMAND ----------

train.tail(1)

# COMMAND ----------

test.tail(5)

# COMMAND ----------

train.show()

# COMMAND ----------

test.show(25)

# COMMAND ----------

train.count(),test.count()

# COMMAND ----------

train.columns,test.columns

# COMMAND ----------

len(train.columns),len(test.columns)

# COMMAND ----------

train.describe().show()

# COMMAND ----------

test.describe().show()

# COMMAND ----------

train.describe('Product_ID').show()

# COMMAND ----------

test.describe('User_ID').show()

# COMMAND ----------

train.select('Product_ID','Age').show()

# COMMAND ----------

test.select('User_ID','Product_ID').show()

# COMMAND ----------

train.select('Product_ID').distinct().count(),test.select('Product_ID').distinct().count()

# COMMAND ----------

diffintraintes = test.select('Product_ID').subtract(train.select('Product_ID'))

# COMMAND ----------

diffintraintes.distinct().count()

# COMMAND ----------

train.select('Age','Gender').show()

# COMMAND ----------

train.select('Age','Gender').count()

# COMMAND ----------

train.crosstab('Age','Gender').show()

# COMMAND ----------

train.select('Age','Gender').dropDuplicates().show()

# COMMAND ----------

train.dropna().count()

# COMMAND ----------

train.count()

# COMMAND ----------

train.show(2)

# COMMAND ----------

train.fillna(-1).show(2)

# COMMAND ----------

train.show(2)

# COMMAND ----------

train.dropna().show(2)

# COMMAND ----------

train.filter(train.Purchase > 15000).show()

# COMMAND ----------

train.filter(train.Purchase > 15000).count()

# COMMAND ----------

train.groupby('Age').count().show()

# COMMAND ----------

train.orderBy(train.Age.desc()).show()

# COMMAND ----------

train.registerTempTable('karthick')

# COMMAND ----------

sqlContext.sql('select * from karthick limit 20').show()

# COMMAND ----------


