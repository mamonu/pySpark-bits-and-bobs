# Import Pyspark Packages
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *


# Import Python Packages
import pandas as pd
import numpy as np


# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("PySpark_Tutorial_02") \
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


# If our dataset is small enough, then we could convert it to a Pandas DataFrame
df = df.toPandas()


# We can now use standard PANDAS Data Manipulation on our DataFrame
# E.g. Concatanate two copies on top of each other
df_concat = pd.concat([df, df], axis = 0)
# E.g. locate the fifth row from the first column
df_concat.iloc[4,0]
# E.g. Filter Rows by Gender
df_male = df[df.Gender == "Male"]


# End Session
spark.stop()

