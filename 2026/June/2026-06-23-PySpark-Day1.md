# PySpark Learning Journey - Day 1

## Topics Covered

- Installed PySpark
- Created SparkSession
- Loaded CSV files
- Displayed DataFrames using `show()`
- Explored schema using `printSchema()`

## Transformations

- `select()`
- `filter()`
- `groupBy()`
- `avg()`
- `max()`

## Actions

- `show()`
- `count()`
- `collect()`
- `first()`

## Spark SQL

Created a temporary view:

```python
df.createOrReplaceTempView("employees")
```

Executed SQL queries:

```sql
SELECT name
FROM employees;
```

## Key Learnings

- Spark DataFrames are immutable.
- Transformations are lazily evaluated.
- Spark SQL allows SQL queries on DataFrames.
- Spark can process large datasets efficiently using distributed computing.

## Challenges Faced

- Java compatibility issues during setup.
- Understanding the difference between transformations and actions.
- Understanding how temporary views work.

## Next Steps

- Spark SQL Aggregations
- Joins
- Real-world dataset analysis
- Build a small Employee Analytics project

## Progress Summary

Successfully installed and configured PySpark, worked with DataFrames, performed filtering and aggregations, and executed SQL queries using Spark SQL.
