
# Import Pyspark Packages
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f


# Import Python Packages
import pandas as pd
import numpy as np

import = pd.read_csv

# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("PySpark_Tutorial_01") \
    .getOrCreate()      
    
    
# Create our SparkDataFrame
df = spark.createDataFrame([('Boaty McBoatface','Female', 25, 692057295, '12-05-2015', '29-10-2017'),
                            ('Parsey McParser','Female', 53, 716492846, '02-06-2016', '11-06-2017'),
                            ('Pickle Rick','Male', 19, 926482943,'16-07-2011', '01-01-2018'),
                            ('Jon Smith','Male',44, 338401754, '12-05-2017', '10-04-2018'),
                            ('Jojo McKilroe','Male', 29 ,295926012, '05-12-2015', '22-07-2018'),
                            ('Ron Parkyns','Male', 60, 105629475, '11-03-2014', '15-02-2018'),
                            ('Shellie Dobbson','Female', 31, 765559271, '29-09-2016', '05-10-2017'),
                            ('Corrie Lidington','Male', 20, 105739925, '24-11-2017', '25-05-2018')],
                           ['Name', 'Gender', 'Age', 'Passport Number', 'Departure_Date', 'Arrival_Date'])


# Show Spark DataFrame
df.show()


# SELECTING COLUMNS- we may only want to look at a subset of the columns
# Columns you want to select are included as strings inside the select() method
# Here, 5 of the 6 original columns have been selected
df.select('Name', 'Gender', 'Age', 'Departure_Date', 'Arrival_Date').show()


# DROPPING COLUMNS
# May want to select all but one/two columns 
# Instead of selecting all the columns required, drop can be used to drop columns not required
# Here, the column 'Gender' has been dropped
df.drop('Gender').show()


# If you want these changes to remain permanently INPLACE, you can either overwrite the original df or create a new df
# Here, we have created a new df: 'df_new', which retains only 3 of the 6 columns
df_new = df.select('Name', 'Gender', 'Age')
df_new.show()

# FILTERING ROWS
# Example: We only want rows where Age is greater than 30 years
# We use filter() to select rows that fit a given criteria
# Again, if you want these changes to remain permanently INPLACE, overwrite the original df or create a new df
df.filter(df.Age > 30).show()


# RENAMING COLUMNS - Method 1 
# selectExpr is an easy way to rename a column 
# selectExpr("Old_Column_Name as New_Column_Name")
# Example below chages the column name  'Gender' to 'Sex' and 'Departure_Date' to 'Dept_Date'
# Issue: This only keeps the renamed columns
df.selectExpr("Gender as Sex", "Departure_Date as Dept_Date").show()


# RENAMING COLUMNS - Method 2
# withColumnRenamed is another easy way to rename a column 
# withColumnRenamed("Old_Column_Name", "New_Column_Name")
# Issue: This only keeps the renamed columns
df.withColumnRenamed("Gender", "Sex").show()


# RENAMING COLUMNS - Method 3 (Recommended)
# This method is more complex but retains all columns, not just the ones renamed
# When the code below is run, all columns are retained, and the single column 'Gender' is renamed to 'Sex'
df.select(*[col(s).alias('Sex') if s == 'Gender' else s for s in df.columns]).show()


# CREATING A NEW COLUMN
# We use withColumn() to create a new column
# withColumn('Name_of_New_Column', condtions)
# Example: We want to create a new column which adds 10 years to every value in the Age Column
# We overwrite the original df so that the column is permenantly added
df = df.withColumn('Age_Plus_Ten', df.Age + 10)
df.show()


# Instead of showing the DataFrame to check if the column is added, we can use printSchema()
df.printSchema()


