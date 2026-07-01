from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Day 2 - DataFrame Operations") \
    .getOrCreate()

df = spark.read.csv(
    "employees.csv",
    header=True,
    inferSchema=True
)

df.show()
df.printSchema()

print("\n====== SELECT =======")
df.select("Name", "Salary").show()

print("\n======= FILTER =======")
df.filter(df.Salary > 50000).show()

print("\n========== WHERE ==========")
df.where(df.Department == "IT").show()

print("\n========== MULTIPLE CONDITIONS ==========")

df.filter(
    (df.Department == "IT") &
    (df.Salary > 55000)
).show()


print("\n========== ADD BONUS COLUMN ==========")

df_bonus = df.withColumn(
    "Bonus",
    col("Salary") * 0.10
)

df_bonus.show()

print("\n========== RENAME COLUMN ==========")
df_rename = df_bonus.withColumnRenamed("Salary", "MonthySalary")
df_rename.show()

print("\n========== DROP COLUMN ==========")
df_drop = df_rename.drop("Experience")
df_drop.show()

print("\n========== ORDERBY ==========")
print("\n========== SORTING ASC ==========")
df_rename.orderBy("MonthySalary").show()

print("\n========== SORTING DESC ==========")
df_rename.orderBy(df_rename.MonthySalary.desc()).show()

print("\n========== DISTINCT ==========")
df.select("Department").distinct().show()

print("\n========== LIMIT ==========")
df_rename.limit(5).show()


print("\n========== DAY 2 CHALLENGE ==========")
df.select("EmployeeID", "Name", "Department").show()

df.filter(df.Age > 30).show()

df.select("Name").filter(df.Department == "Finance").show()

df.orderBy(df.Experience.desc()).show()

df_tax = df.withColumn("Tax", col("Salary") * 0.05)
df_tax.show()