from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import upper, lower, length
from pyspark.sql.functions import to_date, year
from pyspark.sql.functions import sum, avg, max, min, count

spark = SparkSession.builder.appName("Day 3").getOrCreate()

df = spark.read.csv("employees.csv", header=True, inferSchema=True)

print("\n========== TOTAL SALARY ==========")
df.groupBy("Department").agg(sum("Salary").alias("Total Salary")).show()

print("\n========== AVERAGE SALARY ==========")
df.groupBy("Department").agg(avg("Salary").alias("Average Salary")).show()

print("\n========== EMPLOYEE COUNT ==========")
df.groupBy("Department").agg(count("EmployeeID").alias("Employee Count")).show()

print("\n========== MAXIMUM SALARY ==========")
df.groupBy("Department").agg(max("Salary").alias("Maximum Salary")).show()

print("\n========== MINIMUM SALARY ==========")
df.groupBy("Department").agg(min("Salary").alias("Minimum Salary")).show()

print("\n========== MULTIPLE AGGREGATIONS ==========")
df.groupBy("Department").agg(
    sum("Salary").alias("Total Salary"),
    avg("Salary").alias("Average Salary"),
    count("EmployeeID").alias("Employee Count"),
    max("Salary").alias("Maximum Salary"),
    min("Salary").alias("Minimum Salary")
).show()

print("\n========== SPARK SQL ==========")
df.createOrReplaceTempView("employees")
spark.sql("SELECT * FROM employees WHERE Salary > 50000").show()


dept = spark.read.csv("departments.csv", header=True, inferSchema=True)
dept.show()

print("\n========== INNER JOIN ==========")
df_joined = df.join(dept, on = "Department", how = "inner")
df_joined.show()