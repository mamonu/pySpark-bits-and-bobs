from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,isnan,isnull
import pandas as pd 
import datetime as dt
from faker import Faker




# Create spark session, replace with config appropriate to project
spark = (
    SparkSession.builder.appName('Lesson10')
        .getOrCreate()
)



### create fake data
fake = Faker('en_GB') #Generating things based on location GB
#fake.seed(42)
pdata = {'Name': [fake.name() for x in range(12000)],
         'country':[fake.country() for x in range(12000)],
        'earnings':[float(fake.numerify(text="#####")) for x in range(12000)],
        'smoker': [fake.boolean(chance_of_getting_true = 12) for x in range(12000)],
       }
people_df = pd.DataFrame(data = pdata)
people_df.head()
peopleschema = StructType([
         StructField("Name", StringType(), True),
         StructField("country", StringType(), True),
         StructField("earnings", DoubleType(), True),
         StructField("smoker", BooleanType(), True),    
])

p_df = spark.createDataFrame(people_df,peopleschema)
### end of fake data creation


p_df.show()

#### excersise:
### I would like as a result a dataframe that has:

# name / country /earnings/ aggregate columns for that country . 

#examples of number of people are given
#add some more. ps you can use any function you think its useful
###https://spark.apache.org/docs/2.3.0/api/python/pyspark.sql.html#pyspark.sql.GroupedData

from pyspark.sql import functions as F

g_df=(
  p_df.groupby('country').\
agg(
        F.count("*").alias('peoplecount'),
        ##???? 
    )
)\
.show()


###and then ....???
