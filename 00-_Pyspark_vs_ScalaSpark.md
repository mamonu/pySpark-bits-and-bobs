
# Spark performance for Scala vs Python


(Great answer taken from https://stackoverflow.com/questions/32464122/spark-performance-for-scala-vs-python )


## RDD API 
### (pure Python structures with JVM based orchestration)

This is the component which will be most affected by the performance of the Python code and the details of PySpark implementation. While Python performance is rather unlikely to be a problem, there at least few factors you have to consider:

#### Overhead of JVM communication.

Practically all data that comes to and from Python executor has to be passed through a socket and a JVM worker. While this is a relatively efficient local communication it is still not free.

#### Process-based executors (Python) versus thread based (single JVM multiple threads) executors (Scala). 

Each Python executor runs in its own process. As a side effect, it provides stronger isolation than its JVM counterpart 
and some control over executor lifecycle but potentially significantly higher memory usage:

* interpreter memory footprint
* footprint of the loaded libraries
* less efficient broadcasting (each process requires its own copy of a broadcast)

#### Performance of Python code itself. 

Generally speaking Scala is faster than Python but it will vary on task to task. Moreover you have multiple options including JITs like Numba, C extensions (Cython) or specialized libraries like Theano. Finally, if you don't use ML / MLlib (or simply NumPy stack), consider using PyPy as an alternative interpreter. See SPARK-3094.

PySpark configuration provides the `spark.python.worker.reuse` option which can be used to choose between forking Python process for each task and reusing existing process. The latter option seems to be useful to avoid expensive garbage collection (it is more an impression than a result of systematic tests), while the former one (default) is optimal for in case of expensive broadcasts and imports.

Reference counting, used as the first line garbage collection method in CPython, works pretty well with typical Spark workloads (stream-like processing, no reference cycles) and reduces the risk of long GC pauses.

--------

## MLlib 
### (mixed Python and JVM execution)

Basic considerations are pretty much the same as before with a few additional issues. 
While basic structures used with MLlib are plain Python RDD objects, all algorithms are executed directly using Scala.
It means an additional cost of converting Python objects to Scala objects and the other way around, 
increased memory usage and some additional limitations we'll cover later.

As of now (Spark 2.x), the RDD-based API is in a maintenance mode and is scheduled to be removed in Spark 3.0.


--------

## DataFrame API and Spark ML 
### (JVM execution with Python code limited to the driver)

These are probably the best choice for standard data processing tasks. 

Since Python code is mostly limited to high-level logical operations on the driver, there should be no performance difference between Python and Scala.

A single exception is usage of row-wise Python UDFs which are significantly less efficient than their Scala equivalents. 
While there is some chance for improvements (there has been substantial development in Spark 2.0.0), the biggest limitation is full roundtrip between internal representation (JVM) and Python interpreter.
If possible, you should favor a composition of built-in expressions (example. Python UDF behavior has been improved in Spark 2.0.0, but it is still suboptimal compared to native execution. 

This may improved in the future with introduction of the vectorized UDFs (SPARK-21190).   *See Pandas_UDF*

Also be sure to avoid unnecessary passing data between DataFrames and RDDs. This requires expensive serialization and deserialization, not to mention data transfer to and from Python interpreter.

It is worth noting that Py4J calls have pretty high latency. 
Usually, it shouldn't matter (overhead is constant and doesn't depend on the amount of data) but in the case of soft real-time applications, you may consider caching/reusing Java wrappers.

#### GraphX and Spark DataSets

As for now (Spark 1.6 2.1) neither one provides PySpark API so you can say that PySpark is infinitely worse than Scala.

#### GraphX
In practice, GraphX development stopped almost completely and the project is currently in the maintenance mode with related JIRA tickets closed as won't fix. GraphFrames library provides an alternative graph processing library with Python bindings.

#### Dataset
Subjectively speaking there is not much place for statically typed Datasets in Python and even if there was the current Scala implementation is too simplistic and doesn't provide the same performance benefits as DataFrame.

#### Streaming

From what I've seen so far, I would strongly recommend using Scala over Python. It may change in the future if PySpark gets support for structured streams but right now Scala API seems to be much more robust, comprehensive and efficient. My experience is quite limited.

Structured streaming in Spark 2.x seem to reduce the gap between languages but for now it is still in its early days. Nevertheless, RDD based API is already referenced as "legacy streaming" in the Databricks Documentation (date of access 2017-03-03)) so it reasonable to expect further unification efforts.


------

## Non-performance considerations

#### Feature parity

Not all Spark features are exposed through PySpark API. Be sure to check if the parts you need are already implemented and try to understand possible limitations.

It is particularly important when you use MLlib and similar mixed contexts (see Calling Java/Scala function from a task). To be fair some parts of the PySpark API, like mllib.linalg, provides a more comprehensive set of methods than Scala.

#### API design

The PySpark API closely reflects its Scala counterpart and as such is not exactly Pythonic. It means that it is pretty easy to map between languages but at the same time, Python code can be significantly harder to understand.

#### Complex architecture

PySpark data flow is relatively complex compared to pure JVM execution. It is much harder to reason about PySpark programs or debug. Moreover at least basic understanding of Scala and JVM in general is pretty much a must have.

#### Spark 2.x and beyond

Ongoing shift towards Dataset API, with frozen RDD API brings both opportunities and challenges for Python users. While high level parts of the API are much easier to expose in Python, the more advanced features are pretty much impossible to be used directly.

Moreover native Python functions continue to be second class citizen in the SQL world. Hopefully this will improve in the future with Apache Arrow serialization (current efforts target data collection but UDF serde is a long term goal).

For projects strongly depending on the Python codebase, pure Python alternatives (like Dask or Ray) could be an interesting alternative.

It doesn't have to be one vs. the other

The Spark DataFrame (SQL, Dataset) API provides an elegant way to integrate Scala/Java code in PySpark application. You can use DataFrames to expose data to a native JVM code and read back the results. I've explained some options somewhere else and you can find a working example of Python-Scala roundtrip in How to use a Scala class inside Pyspark.

It can be further augmented by introducing User Defined Types (see How to define schema for custom type in Spark SQL?).
