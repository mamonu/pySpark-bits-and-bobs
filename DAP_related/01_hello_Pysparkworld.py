from pyspark.sql import SparkSession
from pyspark.sql import Window

from pyspark.sql.functions import col,isnan,isnull

# Create spark session, replace with config appropriate to project
spark = (
    SparkSession.builder.appName('Lesson8').getOrCreate()
)


spark.sparkContext.getConf().getAll()


# Create some example data
data = [
    ('Car', 1, 5.0),
    ('Car', 2, 5.0),
    ('Car', 3, 5.0),
    ('Car', 4, 6.0),
    ('Car', 5, None),
    ('Plane', 1, 5.0),
    ('Plane', 2, 8.0),
    ('Plane', 3, None),
    ('Plane', 4, 5.0),
    ('Plane', 5, 5.0),
    ('Plane', 6, 7.0),
    ('Batmobile', 7, 5.0),
    ('Batmobile', 1, None),
    ('Batmobile', 2, 0.0),
    ('Batmobile', 3, 5.0),
    ('Catmobile', 7, 5.0),
    ('Catmobile', 1, None),
    ('Catmobile', 2, 5.0),
    ('Catmobile', 3, 9.0),
    ("",None,None)
  
]

# category and ordering could both be multiple columns
# Any column which has an ordering could be used, for example columns of dates or times
schema = ['category', 'ordering', 'value']

# Create spark DataFrame from the example data
df = spark.createDataFrame(data, schema)

df.printSchema()


df.show()
#####

spark.version



#partitions 
df.rdd.getNumPartitions()

df_repartitioned = df.repartition(4)
df_repartitioned.rdd.getNumPartitions()



# Select only the "Category" column
df.select("Category").show()


# Filter only rows with category Catmobile.
df.filter(col("Category")=='Catmobile').show()


#create new column based on previous column with withColumn
# think it as mutate in DPLYR 
 

df = df.withColumn('orderingsq',col('ordering')**2)
df.show()



###### ex1. create your own column. decide what to do.
### the most ridiculous one wins a sticker



