


# Import Pyspark Packages
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import col, isnan, when, count, col, countDistinct
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import Imputer


# Import Python Packages
import pandas as pd
import numpy as np


# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("PySpark_Tutorial_03") \
    .getOrCreate()      
    
    
# Create our SparkDataFrame - included null values which are represented as 'None'
df = spark.createDataFrame([(None,None, 25, 692057295, '12-05-2015', None),
                            ('Parsey McParser','Female', 53, 716492846, '02-06-2016', '11-06-2017'),
                            ('Pickle Rick','Male', 19, 926482943, None, '01-01-2018'),
                            ('Jon Smith','Male',44, 338401754, '12-05-2017', '10-04-2018'),
                            ('Jojo McKilroe','Male', None, None, '05-12-2015', '22-07-2018'),
                            ('Ron Parkyns',None, 60, 105629475, '11-03-2014', '15-02-2018'),
                            ('Shellie Dobbson','Female', 31, 765559271, '29-09-2016', '05-10-2017'),
                            ('Corrie Lidington','Male', 20, 105739925, '24-11-2017', '25-05-2018')],
                           ['Name', 'Gender', 'Age', 'Passport Number', 'Departure_Date', 'Arrival_Date'])



# Show Spark DataFrame
df.show()


# CREATE FORENAME AND SURNAME FROM FULL NAME
# split_col takes a column and splits in based on the chosen delimiter
# We then have to manually add the results to our DataFrame using getItem()
# We can then drop the original Name Column
split_col = pyspark.sql.functions.split(df['Name'], ' ')
df = df.withColumn('Forename', split_col.getItem(0))
df = df.withColumn('Surname', split_col.getItem(1))
df = df.drop('Name')
df.show()

# MIN AND MAX VALUES
df.agg(f.min(df.Age)).collect()
df.agg(f.max(df.Age)).collect()


# MISSING VALUES
# Here we count and then show rows of df.Gender that are null
df.where(col("Gender").isNull()).count()
df.where(col("Gender").isNull()).show()


# We can drop the rows where certain columns contain missing values
# Code drops the 2 rows where Gender is null
df.na.drop(subset=["Gender"]).show()


# Alternatively, we can choose to keep only rows that are not null
# Example: Keep only rows where Gender not null (2 methods)
df.filter(df.Gender.isNotNull()).count()
df.filter("Gender is not NULL").show()


# COUNT NULL / NAN VALUES for all columns
# This code counts total null OR (|) nan values for each column
# isnan() is from pysparl.sql.function package, must specify column you want to use as an argument (isnan('Column'))
# isNull() is from pyspark.sql.Column package, must do Column.isNull()
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()


# Here, we impute missing Age values with the mean age
# Firstly, we selected the Age column and convert it from long type to DoubleType
# We then create the column Age_imputed which fills null values with the mean Age
df_2 = df.select(('Age'))
df_2 = df_2.withColumn("Age", df_2["Age"].cast(DoubleType()))
imputer = Imputer(inputCols = df_2.columns, outputCols = ["{}_imputed".format(c) for c in df_2.columns])
df_2 = imputer.fit(df_2).transform(df_2)


# GROUPBY
# We can group records by Gender and then do value counts for these groups
gender_counts = df.groupBy('Gender').count()
gender_counts.show()


# Convert gender_counts to pandas DataFrame - ready for data manipulation in Pandas
pd_gender_counts = gender_counts.toPandas()


# Add new column - proportions (Pandas)
pd_gender_counts["Proportion"] = pd_gender_counts["count"] / pd_gender_counts["count"].sum()


# GroupBy Gender and then count distinct Ages (all ages distinct currently)
# Note: The countDistinct does not appear to consider null values
distinct_values = df.groupBy('Gender').agg(countDistinct("Age"))
distinct_values.show()


