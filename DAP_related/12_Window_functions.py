import numpy as np
import pandas as pd
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F

from pyspark.sql.window import Window

from pyspark.sql.functions import rank,col,isnan,isnull,collect_set,explode,collect_list

# Create spark session, replace with config appropriate to project
spark = (
    SparkSession.builder.appName('Lesson13').getOrCreate()
)


spark.version


###some random synthetic data
med = 15.5
dp = 8.2
sDays = np.arange('2001-01', '2016-12', dtype='datetime64[D]')
nDays = len(sDays)
s1 = np.random.gumbel(loc=med,scale=dp,size=nDays)
s1[s1 < 0] =0
company = ['MSFT', 'GOOG', 'APPL', 'FCBK', 'BMW', 'AUDI']
companylist =([ random.choice(company) for c in range(nDays)])
itemlist =([ random.choice(['tablet','laptop','charger']) for c in range(nDays)])
### end of creation of  random synthetic data

###make data into pandas df
dfSint = pd.DataFrame({'price':s1,'sDays':sDays,'company':companylist,'item':itemlist})
compschema = StructType([
         StructField("price", DoubleType(), True),
         StructField("sDays",DateType(), True),
         StructField("company",  StringType(), True),
         StructField("item",  StringType(), True),

])


## now from pandas to pyspark

companies_df = spark.createDataFrame(dfSint,compschema)
companies_df.show() ###availiable in spark



# Add Column with todays date
companies_df = companies_df.withColumn('DateToday',F.current_date())
companies_df = companies_df.withColumn("Diff_in_days",\
                   F.datediff( companies_df.DateToday,companies_df.sDays ))


## orderby
companies_df.orderBy('price').show()

g_df=companies_df.\
groupby('company').\
agg(F.count("*").alias('acount')) 
      
  

##Scenario #1
## No orderBy specified for window object.

window_1 = Window.partitionBy('company')

df_1 = companies_df.withColumn('cmpavg', (F.avg(F.col('price')).over(window_1)))
df_1.show(100)



## Scenario #2 : ranking
##


window_2 = Window.\
              partitionBy('company','item').\
              orderBy(companies_df['price'].desc())


df_2 = companies_df.withColumn("rank_based_on_price",rank().over(window_2))
df_2.show(1000)
    

#https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html

