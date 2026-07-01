from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import upper, lower, length
from pyspark.sql.functions import to_date, year


spark = SparkSession.builder.appName("Day 3").getOrCreate()

df = spark.read.csv("employees.csv", header=True, inferSchema=True)

df.show()
df.printSchema()

print("\n========== DROP NULL VALUES ==========")
df.dropna().show()

print("\n========== FILL NULL VALUES ==========")
df_filled = df.fillna({"Age": 0, "Salary": 0, "Department": "Unknown"})
df_filled.show()

print("\n========== REMOVE DUPLICATES ==========")
df_filled.dropDuplicates().orderBy("EmployeeID").show()

print("\n========== TYPE CASTING ==========")
df_cast = df.withColumn("Salary", col("Salary").cast("int"))
df_cast.printSchema()

print("\n========== STRING FUNCTIONS ==========")
print("\n========== UPPER CASE ==========")
df.select(upper("Name").alias("Uppercase Name")).show()

print("\n========== LOWER CASE ==========")
df.select(lower("Name").alias("Lowercase Name")).show()

print("\n========== STRING LENGTH ==========")
df.select(length("Name").alias("Name Length")).show()

print("\n========== DATE FUNCTIONS ==========")
df_date = df.withColumn("JoiningDate", to_date("JoiningDate"))
df_date.show()
df_date.printSchema()

print("\n========== EXTRACT YEAR ==========")
df_date.select("Name", year("JoiningDate").alias("Joining Date")).show()


print("\n========== DAY 3 CHALLENGE ==========")
print("\n========== DAY 3 CHALLENGE ==========")
print("Challenge 1: Convert every department name to UPPERCASE.")
df_Departments = df.select("Department").distinct().dropna()
df_Departments.show()

print("Challenge 2: Show the number of letters in each department.")
df_departments_length = df_Departments.withColumn("Department Length", length("Department"))
df_departments_length.show()

print("Challenge 3: Show the year of joining for each employee.")
df_year = df.withColumn("Joining Year", year("JoiningDate")).dropna()
df_year.select("Name", "Joining Year").show()

print("Remove duplicate employeeIDs")
df_unique_emp = df.dropDuplicates(["EmployeeID"])
df_unique_emp.show()


print("Fill missing salaries with 50,000 instead of 0")
df_filled_sal = df.fillna({"Salary": 50000})
df_filled_sal.show()