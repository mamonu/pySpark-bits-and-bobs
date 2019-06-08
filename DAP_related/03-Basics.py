


# Import Pyspark Packages
# * means that we import everything from the module
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f


# Import Python Packages
import pandas as pd
import numpy as np


# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("PySpark_Tutorial_00") \
    .getOrCreate()      
    
    
# Firstly, we manually create a Spark DataFrame called "df", containing 5 columns and 8 rows
# The first argument of 'spark.createDataFrame()' is a list (squared brackets) containing the rows of data
# Each row is written inside brackets
# Each row is separated by a comma
# The second argument is a list containing the column headers
# Each header must be a string
df = spark.createDataFrame([('Boaty McBoatface','Female',692057295,'12-05-2015', '29-10-2017'),
                            ('Parsey McParser','Female',716492846,'02-06-2016', '11-06-2017'),
                            ('Pickle Rick','Male',926482943,'16-07-2011', '01-01-2018'),
                            ('Jon Smith','Male',338401754,'12-05-2017', '10-04-2018'),
                            ('Jojo McKilroe','Male',295926012,'05-12-2015', '22-07-2018'),
                            ('Ron Parkyns','Male',105629475,'11-03-2014', '15-02-2018'),
                            ('Shellie Dobbson','Female',765559271,'29-09-2016', '05-10-2017'),
                            ('Corrie Lidington','Male',105739925,'24-11-2017', '25-05-2018')],
                           ['Name', 'Gender', 'Passport Number', 'Departure_Date', 'Arrival_Date'])


# Once we have created the DataFrame we want to show it
df.show(10)


# If we want to just see the first row we can use .first()
# To see x number of rows we can use .take(x)
df.first()
df.take(2)


# Here, we can see our column names
df.columns


# We can now use printSchema() to again show these columns PLUS their corresponding types 
# string, long, integer, date etc.
# Note that our date columns are currently of type 'string' and not of type 'date'
df.printSchema()


# Total row count
df.count()


# When working with big data, we will often want to initally work with a sample of our data
# Here, we take a 50% sample of our data, without replacement
df_sample_data = df.sample(fraction=0.01, withReplacement=False)


# Show this sample
df_sample_data.show()


# End our Spark Session - always do this at the end 
spark.stop()


# ----------------------------------------- #
# -----------End of Tutorial 00 ----------- #
# ----------------------------------------- #
