

# Import Pyspark Packages
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import datediff, dayofmonth, month, year, to_date, date_add, date_sub


# Import Python Packages
import pandas as pd
import numpy as np


# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("PySpark_Tutorial_04") \
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
                           ['Name', 'Gender', 'Age', 'Passport Number', 'Departure_Str', 'Arrival_Str'])


# Show Spark DataFrame
df.show()


# Convert Dates from String Types to Date Types
df = df.withColumn('Departure_Date', to_date(df.Departure_Str, 'dd-MM-yyyy'))
df = df.withColumn('Arrival_Date', to_date(df.Arrival_Str, 'dd-MM-yyyy'))


# Check to see if the new column are date types
df.printSchema()


# MIN AND MAX DATE VALUES
# Appears to work for dates too (once they have been converted to date type)
df.agg(f.min(df.Departure_Date)).collect()
df.agg(f.max(df.Departure_Date)).collect()


# Create Columns Year, Month, Day for Arrival Date
df = df.withColumn("Arrival_Year", year(col("Arrival_Date"))
      ).withColumn("Arrival_Month", month(col("Arrival_Date"))
      ).withColumn("Arrival_Day", dayofmonth(col("Arrival_Date")))


# Calculate Difference Between Arrival Date & Departure Date (Length of Stay)
df = df.withColumn("Length_of_Stay", datediff(df.Arrival_Date, df.Departure_Date))
df.show()


# Add Column with todays date
df = df.withColumn('DateToday',f.current_date())


# Add 10 days to todays date and make this a new column
df = df.withColumn('DateToday_Plus_10', date_add(df.DateToday, 10))
df.show()


# End Session
spark.stop()

