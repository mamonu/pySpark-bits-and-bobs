from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.sql.functions import col,isnan,isnull,collect_set,explode,collect_list

# Create spark session, replace with config appropriate to project
spark = (
    SparkSession.builder.appName('Lesson12One_to_many').getOrCreate()
)


import pandas as pd 
import datetime as dt
from faker import Faker
fake = Faker('en_GB') #Generating things based on location GB
#fake.seed(42)




pdata = {'Name': [fake.name() for x in range(2000)],
         'country':[fake.country() for x in range(2000)],
        'smoker': [fake.boolean(chance_of_getting_true = 12) for x in range(2000)],
        
        }
       


## create fake pandas dataframes out of fake data

people_df = pd.DataFrame(data = pdata)
people_df.head()

#########################################################
### create fake spark dataframes based on schema and data  
###
  
peopleschema = StructType([
         
        
         StructField("Name", StringType(), True),
         StructField("country", StringType(), True),
         StructField("smoker", BooleanType(), True),
  
    
    
])

people_df = spark.createDataFrame(people_df,peopleschema)

people_df.show(30)



#############################################################


people_df.groupby('country').count().show()




many_to_one_df = people_df.groupby("country").agg(collect_list("Name"))


many_to_one_df.show()




##rename weird column name


oldColumns = many_to_one_df.schema.names
print(oldColumns)
newColumns = ["country", "names"]


from functools import reduce
many_to_one_df = reduce(lambda data, idx:\
            many_to_one_df.withColumnRenamed(oldColumns[idx], newColumns[idx]), \
            range(len(oldColumns)), many_to_one_df)



many_to_one_df.show()


one_to_many = many_to_one_df.withColumn("name", explode(many_to_one_df.names))

one_to_many.show()

one_to_many=one_to_many.drop('names')

one_to_many.show()
