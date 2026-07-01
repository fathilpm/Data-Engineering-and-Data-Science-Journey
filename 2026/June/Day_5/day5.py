from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    count,
    max,
    min,
    year,
    to_date
)

# =====================================================
# Day 5 - Employee Analytics Project
# =====================================================

# Create Spark Session
spark = SparkSession.builder \
    .appName("Employee Analytics Project") \
    .getOrCreate()

# =====================================================
# Load Datasets
# =====================================================

employees = spark.read.csv(
    "employees.csv",
    header=True,
    inferSchema=True
)

departments = spark.read.csv(
    "departments.csv",
    header=True,
    inferSchema=True
)

# =====================================================
# Display Data
# =====================================================

print("\n========== EMPLOYEE DATA ==========")
employees.show()

print("\n========== DEPARTMENT DATA ==========")
departments.show()

# =====================================================
# Data Exploration
# =====================================================

print("\n========== TOTAL EMPLOYEES ==========")
print(employees.dropDuplicates(["EmployeeID"]).count())

print("\n========== SCHEMA ==========")
employees.printSchema()

print("\n========== NUMBER OF DEPARTMENTS ==========")
print(
    employees.select("Department")
    .dropna()
    .distinct()
    .count()
)

print("\n========== DEPARTMENT LIST ==========")
employees.select("Department").dropna().distinct().show()

# =====================================================
# Business Question 1
# Employees in Each Department
# =====================================================

print("\n========== EMPLOYEES PER DEPARTMENT ==========")

employees.groupBy("Department") \
    .agg(count("*").alias("Employee Count")) \
    .show()

# =====================================================
# Business Question 2
# Average Salary by Department
# =====================================================

print("\n========== AVERAGE SALARY BY DEPARTMENT ==========")

employees.dropna(subset=["Department", "Salary"]) \
    .groupBy("Department") \
    .agg(avg("Salary").alias("Average Salary")) \
    .orderBy(col("Average Salary").desc()) \
    .show()

# =====================================================
# Business Question 3
# Highest and Lowest Salary by Department
# =====================================================

print("\n========== SALARY STATISTICS ==========")

employees.groupBy("Department").agg(
    max("Salary").alias("Highest Salary"),
    min("Salary").alias("Lowest Salary")
).show()

# =====================================================
# Business Question 4
# Top 5 Highest Paid Employees
# =====================================================

print("\n========== TOP 5 HIGHEST PAID EMPLOYEES ==========")

employees.dropna(subset=["Salary"]) \
    .orderBy(col("Salary").desc()) \
    .show(5)

# =====================================================
# Business Question 5
# Employees Earning Above Average Salary
# =====================================================

avg_salary = employees.select(avg("Salary")).collect()[0][0]

print("\n========== EMPLOYEES ABOVE AVERAGE SALARY ==========")

employees.filter(
    col("Salary") > avg_salary
).show()

# =====================================================
# Business Question 6
# Joining Year Analysis
# =====================================================

print("\n========== JOINING YEAR ==========")

employees = employees.withColumn(
    "JoiningDate",
    to_date("JoiningDate")
)

employees.select(
    "Name",
    "Department",
    year("JoiningDate").alias("Joining Year")
).show()

# =====================================================
# Business Question 7
# Employees Joined Before 2021
# =====================================================

print("\n========== JOINED BEFORE 2021 ==========")

employees.filter(
    year("JoiningDate") < 2021
).select(
    "Name",
    "Department",
    "JoiningDate"
).show()

# =====================================================
# Business Question 8
# Department Managers
# =====================================================

print("\n========== EMPLOYEE MANAGERS ==========")

employees.join(
    departments,
    on="Department",
    how="inner"
).select(
    "Name",
    "Department",
    "Manager",
    "Salary"
).show()

# =====================================================
# Business Question 9
# Bonus Calculation
# =====================================================

print("\n========== BONUS (10%) ==========")

employees.withColumn(
    "Bonus",
    col("Salary") * 0.10
).select(
    "Name",
    "Salary",
    "Bonus"
).show()

# =====================================================
# Business Question 10
# Spark SQL
# =====================================================

employees.createOrReplaceTempView("employees")

print("\n========== SPARK SQL ==========")

spark.sql("""
SELECT
    Name,
    Department,
    Salary
FROM employees
WHERE Salary > 60000
ORDER BY Salary DESC
""").show()

# =====================================================
# Final Summary
# =====================================================

print("\n========== PROJECT COMPLETED ==========")
print("Employee Analytics Project completed successfully!")

# Stop Spark Session
spark.stop()