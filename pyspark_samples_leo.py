# Databricks notebook source
# IMPORTS ON START OF LINE
# IMPORT DATATYPES

# import spark at start of line & create a spark variable
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import *
spark = SparkSession.builder.getOrCreate()

# import datatypes
from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

# LOADING CSV/JSON & OTHER TYPES OF FILES + CREATING DF ON THE LOADED FILES
# OPTIONS OF LOADING THE FILES 

# Import csv
df1=spark.read.option('header', 'true').csv('/FileStore/tables/employees_new.csv',inferSchema=True)
df1.show()

df2=spark.read.option('header', 'true').csv('/FileStore/tables/departments.csv',inferSchema=True)
df2.show()

# Import json
# df1 = spark.read.load("/FileStore/tables/employees.json")
# df1 = spark.read.load("/FileStore/tables/employees.json", format="json")

# Import parquet
# df1 = spark.read.load("/FileStore/tables/employees.parquet")
# df1 = spark.read.load("/FileStore/tables/employees.parquet", format="parquet")


# COMMAND ----------

# CREATING DATAFRAMES MANUALLY

# Create dataframes manually
data = [('James', 'Sales', 3000),
	    ('Michael', 'Sales', 4600),
	    ('Robert', 'Sales', 4100)]
columns = ["Employee", "Department", "Salary"]
df_manual_create = spark.createDataFrame(data, columns)
df_manual_create.show()

# COMMAND ----------

# INSPECTING DATA
# CHECKING DF TYPES
# USING SHOW
# USING PRINTSCHEMA

# Return dataframe's column names and dataypes
df1.dtypes

# Display the content of df1
df1.show()

# Return first n rows
df1.head(10)

# Return first row
df1.first()

# Return the first n rows df1.schema
df1.take(2)

# Compute summary statistics df1.columns Return the columns of df1
df1.describe().show()

# Count the number of rows in df1
df1.count()

# Count the number of distinct rows in df1
df1.distinct().count()

# Print the schema of df1
df1.printSchema()

# Print the (logical and physical) plans
df1.explain()


# COMMAND ----------

# SELECTING COLUMNS - SELECTING ALL & ONLY SELECT COLUMNS

df1.select('*').show()
df1.select('EMPLOYEE_ID', 'FIRST_NAME').show()
df1.select(df1.EMPLOYEE_ID, df1.FIRST_NAME).show()

# COMMAND ----------

# ADDING COLUMNS OF DF
# RENAMING COLUMNS OF DF
# DROPPING COLUMNS OF DF

# Adding columns - filled with null values
df_add_column1 = df1.withColumn('STATUS', lit(None))
df_add_column1.show()
 
# Adding columns - filled with derived values, use concat_ws to separate the column values, the first parameter is the spacer, in this case, an empty space
df_add_column2 = df1.withColumn('FULL_NAME', concat_ws(' ', df1.FIRST_NAME, df1.LAST_NAME))
df_add_column2.show()

# Renaming columns
df_rename_column1 = df1.withColumnRenamed('FIRST_NAME', 'FIRST_NAME_REN')
df_rename_column1.show()
 
# Removing columns
df_drop_column1 = df1.drop('FIRST_NAME', 'LAST_NAME')
df_drop_column1.show()
df_drop_column2 = df1.drop(df1.FIRST_NAME).drop(df1.LAST_NAME)
df_drop_column2.show()

# COMMAND ----------

# HANDLING NULL VAULES
    # - DROP ROWS WITH NULL VALUES WHEN ALL VALUES OF THE ROWS IS NULL
    # - DROP ROWS WITH NULL VALUES WHEN SOME VALUES OF THE ROWS IS NULL & LIMIT IT WITH THRESH()
    # - FILLING NULL VALUES WITH .FILL()
    
# Drop rows with any null value in the row
df_drop_row1 = df1.na.drop(how='any')
df_drop_row1.show()

# Drop rows where all of the values of the row is null
df_drop_row2 = df1.na.drop(how='all')
df_drop_row2.show()

# Drop rows where 2 or more of the values on the row is non-null
df_drop_row3 = df1.na.drop(how='any',thresh=2)
df_drop_row3.show()

# Drop rows if there are any null values on the rows only on the specified column using subset
df_drop_row4 = df1.na.drop(how='any', subset='COMMISSION_PCT')
df_drop_row4.show()

# Filling any null values with a specified value
df_fill_null_values1 = df1.na.fill('Missing Values')
df_fill_null_values1.show()

# Filling null values on specific columns with a value
df_fill_null_values2 = df1.na.fill({'COMMISSION_PCT': 'A', 'MANAGER_ID': 999})
df_fill_null_values2.show()

# COMMAND ----------

# FILTER OPERATIONS
# USING &, |, ==
# USING ~
# USING LIKE
# USING BETWEEN
# USING SUBSTRING

# Filter with Select
df1.filter(df1.SALARY>=5000).select('EMPLOYEE_ID', 'FIRST_NAME', 'SALARY').show()

# Filter with 'AND'
df1.filter((df1.DEPARTMENT_ID == 50) & (df1.MANAGER_ID  == 124)).select('EMPLOYEE_ID', 'FIRST_NAME', 'SALARY', 'DEPARTMENT_ID', 'MANAGER_ID').show()

# Filter with 'OR'
df1.filter((df1.DEPARTMENT_ID == 50) | (df1.MANAGER_ID  == 124)).select('EMPLOYEE_ID', 'FIRST_NAME', 'SALARY', 'DEPARTMENT_ID', 'MANAGER_ID').show()

# Filtering with 'Like'
df1.filter(df1.FIRST_NAME.like('Do%')).show()
 
# Filtering with 'Between'
df1.select("FIRST_NAME","SALARY").where(df1.SALARY.between(3000, 5000)).show()

# Filtering with 'Substring'
df1.select(substring(df1.FIRST_NAME, 1, 2)).show()

# COMMAND ----------

# GROUP BY & AGGREGATES

# Group by & aggregate
df1.groupBy(df1.FIRST_NAME).count().show()
df1.groupBy(df1.FIRST_NAME).avg().show()
df1.groupBy(df1.FIRST_NAME).max().show()
  
# Group by with select & filter & order by
df1.groupBy('DEPARTMENT_ID').agg(sum('SALARY').alias("TOTAL_SALARY_DEPT")).filter(df1.DEPARTMENT_ID != 50).sort(df1.DEPARTMENT_ID.desc()).show() 

# COMMAND ----------

# JOINS

# Inner join
df_inner_join1 = df1.join(df2, df1.DEPARTMENT_ID == df2.DEPARTMENT_ID, 'inner').sort(df1.FIRST_NAME.asc())
df_inner_join1.show()

# Inner join with select, filter, order by
df_inner_join2 = df1.join(df2, df1.DEPARTMENT_ID == df2.DEPARTMENT_ID, 'inner').filter(df1.DEPARTMENT_ID == 50).select(df1.EMPLOYEE_ID, df1.FIRST_NAME, df2.DEPARTMENT_ID).sort(df1.FIRST_NAME.asc())
df_inner_join2.show()

# Left join, the df1 is the 'left' table here
df_left_join1 = df1.join(df2, df1.DEPARTMENT_ID == df2.DEPARTMENT_ID, 'left').sort(df1.FIRST_NAME.asc())
df_left_join1.show()

# Right join, the df2 is the 'right' table here
df_right_join1 = df1.join(df2, df1.DEPARTMENT_ID == df2.DEPARTMENT_ID, 'right').sort(df1.FIRST_NAME.asc())
df_right_join1.show()

# full join
df_full_join1 = df1.join(df2, df1.DEPARTMENT_ID == df2.DEPARTMENT_ID, 'full').sort(df1.FIRST_NAME.asc())
df_full_join1.show()

# COMMAND ----------

# ORDER BY
df1.filter(df1.DEPARTMENT_ID == 50).select('EMPLOYEE_ID', 'FIRST_NAME', 'DEPARTMENT_ID').sort('EMPLOYEE_ID', asc = True).show()

# COMMAND ----------

# INSERT ROWS

# Create columns identical to the table to be inserted
columns = ['EMPLOYEE_ID', 'FIRST_NAME', 'LAST_NAME', 'EMAIL', 'PHONE_NUMBER', 'HIRE_DATE', 'JOB_ID', 'SALARY', 'COMMISSION_PCT', 'MANAGER_ID', 'DEPARTMENT_ID']

# Create data to be inserted
new_row = spark.createDataFrame([(285, 'Mickey', 'Mouse', 'MICKEY', '505.122.333', '02-FEB-09', 'ST_CLERK', 2000, 10 , 103, 50)], columns)
new_row.show()

# Add new rows to existing df using union
df_insert1 = df1.union(new_row)
df_insert1.filter((df1.FIRST_NAME == 'Mickey') | (df1.FIRST_NAME == 'Donald')).show()

# COMMAND ----------

# UPDATE ROWS

# Update rows, when Donald then update Donald2, when Jennifer then update to Jennifer2
df1.show()
df_update1 = df1.withColumn("FIRST_NAME", when(df1.FIRST_NAME == "Donald","Donald2").when(df1.FIRST_NAME == "Jennifer","Jennifer2").otherwise(df1.FIRST_NAME))
df_update1.show()

# COMMAND ----------

# DELETE ROWS

# You can just use filter the df and assign it to a new df since it will not include those rows
df_delete_rows = df1.filter((df1.DEPARTMENT_ID != 60) & (df1.DEPARTMENT_ID != 50) & (df1.DEPARTMENT_ID != 90))
df_delete_rows.show()

# COMMAND ----------

# REPARTITION
    # Returns a new DataFrame partitioned by the given partitioning expressions. The resulting DataFrame is hash partitioned.

# Repartition by number of partitions
df_repartition1 = df1.repartition(5)
df_repartition1.rdd.getNumPartitions()

# Repartition by column name/s
df_repartition2 = df1.repartition('EMPLOYEE_ID', 'FIRST_NAME')
df_repartition2.rdd.getNumPartitions()

# Repartition by column name & specify number of partitions
df_repartition3 = df1.repartition(5, 'EMPLOYEE_ID')
df_repartition3.rdd.getNumPartitions()

# COMMAND ----------

# PARTITIONBY
    # Partitions the output by the given columns on the file system.

# df1.write.partitionBy('FIRST_NAME').mode("overwrite").format("csv")

# COMMAND ----------

# WRITE TO FILES
# OPTIONS WHEN WRITING TO FILES
# WRITE OPTIONS:
    # overwrite – mode is used to overwrite the existing file.
    # append – To add the data to the existing file.
    # ignore – Ignores write operation when the file already exists.
    # error – This is a default option when the file already exists, it returns an error.

df1.write.option('header',True).mode('overwrite').csv('/FileStore/tables/employees_new2.csv')


