from pyspark.sql import SparkSession
from pyspark.sql import Window

from pyspark.sql.functions import col,isnan,isnull

# Create spark session, replace with config appropriate to project
spark = (
    SparkSession.builder.appName('HelloHDFS')
        .getOrCreate()
)




# Add the data file to hdfs.
!hdfs dfs -put resources/data/mllib/kmeans_data.txt kmeans.txt



#ls on our home hdfs directory

!hdfs dfs -ls

#ls on tmp hdfs directory

!hdfs dfs -ls /tmp


df=spark.read.csv("kmeans.txt",header=False,sep=" " )

df.show()
