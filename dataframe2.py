# Databricks notebook source
from pyspark.sql import *

# COMMAND ----------

Employee = Row("firstName", "lastName", "email", "salary")
print(Employee)

# COMMAND ----------

print(type(Employee))

# COMMAND ----------

print(Employee[0])
print(Employee[1])
print(Employee[2])
print(Employee[3])

# COMMAND ----------

employee1 = Employee('Basher', 'armbrust', 'bash@edureka.co', 100000)
employee2 = Employee('Daniel', 'meng', 'daniel@stanford.edu', 120000 )
employee3 = Employee('Muriel', None, 'muriel@waterloo.edu', 140000 )
employee4 = Employee('Rachel', 'wendell', 'rach_3@edureka.co', 160000 )
employee5 = Employee('Zach', 'galifianakis', 'zach_g@edureka.co', 160000 )


# COMMAND ----------

print(employee1)
print(employee2)
print(employee3)
print(employee4)

# COMMAND ----------

department1 = Row(id='123456', name='HR')
department2 = Row(id='789012', name='OPS')
department3 = Row(id='345678', name='FN')
department4 = Row(id='901234', name='DEV')

# COMMAND ----------

departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2, employee5])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee1, employee4, employee3])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

# COMMAND ----------

departmentsWithEmployees_Seq = [departmentWithEmployees1, departmentWithEmployees2]
dframe = spark.createDataFrame(departmentsWithEmployees_Seq)
display(dframe)

# COMMAND ----------

dframe.show()

# COMMAND ----------


