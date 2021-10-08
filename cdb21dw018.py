# Databricks notebook source
import pyspark
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
demo = [1,2,3,4,5,6,7,8,9,10]
print(type(demo))
print(demo)
myrdd = sc.parallelize(demo)
print(myrdd)
print(type(myrdd))
myrdd.collect()
myrdd.take(1)
myrdd.take(2)
myrdd.take(3)




