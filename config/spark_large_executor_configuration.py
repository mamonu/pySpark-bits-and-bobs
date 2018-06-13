"""
Setting configuration for the SparkSession in PySpark.

This example sets up Spark executors with a significant amount of memory, useful for processing large datasets.

NOTE: Adjust spark.dynamicAllocation.maxExecutors if you need more executors

This example covers an example use case and should be tweaked according to project needs.

For more details on Spark configuration see here:
https://spark.apache.org/docs/2.2.0/configuration.html 

ONS runs spark on Yarn, that entails another set of configuration options documented here:
https://spark.apache.org/docs/2.2.0/running-on-yarn.html

"""

# SparkSession manages the connection to the SparkCluster
from pyspark.sql import SparkSession

# Config for large Spark Executors (high RAM and multiple cores)
spark = (
  SparkSession.builder.appName('example_config')
    .config('spark.executor.memory', '50g')  # Memory available for the Spark JVM part of the Spark Executor Container
    .config('spark.yarn.executor.memoryOverhead', '10g')  # Memory available for the rest of the Spark Executor Container (including any Python workers that need to run)
    .config('spark.executor.cores', 5)  # Set at 5 to optimise for reading data from HDFS
    .config('spark.dynamicAllocation.maxExecutors', 3)  # Limit the amount of resources requested, value to set depends on current system load and which Yarn queue is being used
    .config('spark.dynamicAllocation.enabled', 'true')  # Spark will request resources as needed, should already be 'true' in CDSW
    .config('spark.shuffle.service.enabled', 'true')  # Part of dynamic allocation set up, should already be 'true' in CDSW
    # .enableHiveSupport()  # Uncomment this line if reading from Hive
    .getOrCreate()
  )
  
# Get all config (including defaults)
spark.sparkContext.getConf().getAll()

