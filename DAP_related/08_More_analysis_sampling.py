from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.sql.functions import col,isnan,isnull

# Create spark session, replace with config appropriate to project
spark = (
    SparkSession.builder.appName('Lesson10')
        .getOrCreate()
)

### create fake data ####

import pandas as pd 
import datetime as dt
from faker import Faker
fake = Faker('en_GB') #Generating things based on location GB
#fake.seed(42)

### create fake data

cdata = {'country':[fake.country() for x in range(2000)],
        'medianWage':[float(fake.numerify(text="#####")) for x in range(2000)]
       }



pdata = {'Name': [fake.name() for x in range(2000)],
         'country':[fake.country() for x in range(2000)],
        'earnings':[float(fake.numerify(text="#####")) for x in range(2000)],
        'smoker': [fake.boolean(chance_of_getting_true = 12) for x in range(2000)],
       }


## create fake pandas dataframes out of fake data

country_df = pd.DataFrame(data = cdata)
country_df.head()

people_df = pd.DataFrame(data = pdata)
people_df.head()

#########################################################
### create fake spark dataframes based on schema and data
### we create a spark dataframe from a pandas df by describing its schema
### we could ignore it but then spark would infer the types... sometimes we want to be sure 

countryschema = StructType([
  
         StructField("country", StringType(), True),
         StructField("MedianWage", DoubleType(), True),
    
])
         
  
  
####because this dataset comes out of fake data there are
####duplicate country data
c_df = spark.createDataFrame(country_df,countryschema)
c_df.show(30)






c_df.groupby('country').count().show()
#in this way we drop any duplicate country rows

c_df=c_df.drop_duplicates(subset=['country'])

c_df.groupby('country').count().show()
  
  
  
  
### create a spark dataframe from a pandas df by describing its schema
  
peopleschema = StructType([
         
        
         StructField("Name", StringType(), True),
         StructField("country", StringType(), True),
         StructField("earnings", DoubleType(), True),
         StructField("smoker", BooleanType(), True),
    
    
])

p_df = spark.createDataFrame(people_df,peopleschema)

p_df.show(30)
p_df.groupby('country').count().show()


#############################################################


### we now have a spark c_df and a spark p_df
### now time for some joins....


inner_join = p_df.join(c_df, c_df.country == p_df.country)
inner_join.show()



inner_join = p_df.join(c_df, "country")
inner_join.show()





##exc 1 create a column with difference of medianwage with earnings




inner_join.count()

#################################################################

#**********************SAMPLING**********************************

### spark df sampling 
### distributed: but diferent number of samples every time (with equal probability of being chosen)

#In the theory of finite population sampling, Bernoulli sampling is a sampling process 
#where each element of the population is subjected to an independent 
Bernoulli trial which determines whether the element becomes part of the sample. 
#An essential property of Bernoulli sampling is that all elements 
#of the population have equal probability of being included in the sample.
#Bernoulli sampling is therefore a special case of Poisson sampling. 
#In Poisson sampling each element of the population may have a different 
#probability of being included in the sample.
#In Bernoulli sampling, the probability is equal for all the elements.
#Because each element of the population is considered separately 
#for the sample, the sample size is not fixed but rather follows 
#a binomial distribution.



#Basically this means it goes through your DF, 
#and assigns each row a probability of being included. 
#So if you want a 10% sample, each row individually has a 10% 
#chance of being included but it doesn't take into account if 
#it adds up perfectly to the number you want, but it tends to 
#be pretty close for large datasets.



samplefromDF = inner_join.sample(withReplacement=True,fraction=0.01)
samplefromDF.count()
samplefromDF.show()

#spark rdd sampling
#slower non distributed but with exact sample number

samplefromRDD=inner_join.rdd.takeSample(withReplacement=True,num= 20)
type(samplefromRDD)

fromrddtodfsample=spark.createDataFrame(samplefromRDD)
fromrddtodfsample.printSchema()
fromrddtodfsample.count()


fromrddtodfsample.show()


# exc 2
### how many unique countries these 2 samples contain

