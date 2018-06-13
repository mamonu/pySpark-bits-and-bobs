"""
Setting configuration for the SparkSession in PySpark.

This example sets up Spark executors with a significant amount of memory, useful for processing large datasets.
The configuration uses the memoryOverhead to give space for Python processes on the worker nodes.

NOTE: Adjust spark.dynamicAllocation.maxExecutors if you need more executors

This example covers an example use case and should be tweaked according to project needs.

For more details on Spark configuration see here:
https://spark.apache.org/docs/2.2.0/configuration.html 

ONS runs spark on Yarn, that entails another set of configuration options documented here:
https://spark.apache.org/docs/2.2.0/running-on-yarn.html

created by: Phil Lee
"""

# SparkSession manages the connection to the SparkCluster
from pyspark.sql import SparkSession

# Config for large Spark Executors (high RAM and multiple cores)
# with room for significant Python processing taking place on the worker nodes
spark = (
  SparkSession.builder.appName('example_config')
    .config('spark.executor.memory', '40g')  # Sets the memory available for the Spark JVM part of the Spark Executor Container
    .config('spark.yarn.executor.memoryOverhead', '20g')  # Memory available for the rest of the Spark Executor Container (including any Python workers that need to run)
    .config('spark.executor.cores', 5)  # Set at 5 to optimise for reading data from HDFS
    .config('spark.python.worker.memory', '3g')  # Each core on an executor gets it's own Python worker, this sets out roughly how much memory each will get
    .config('spark.dynamicAllocation.maxExecutors', 3)  # Limit the amount of resources requested, value to set depends on current system load and which Yarn queue is being used
    .config('spark.dynamicAllocation.enabled', 'true')  # Spark will request resources as needed, should already be 'true' in CDSW
    .config('spark.shuffle.service.enabled', 'true')  # Part of dynamic allocation set up, should already be 'true' in CDSW
    # .enableHiveSupport()  # Uncomment this line if reading from Hive
    .getOrCreate()
  )

# Get all config (including defaults)
spark.sparkContext.getConf().getAll()

