# PySpark Implementation best practices

------

`Broadcast variables` 

When you have a large variable to be shared across the nodes, use a broadcast variable to reduce the communication cost. 

If you don’t, this same variable will be sent separately for each parallel operation. 
Also, the default variable passing mechanism is optimized for small variables and can be slow when the variable is large. 
Broadcast variables allow the programmer to keep a read-only variable cached, in deserialized form, 
on each machine rather than shipping a copy of it with tasks. 
The broadcast of variable v can be created by bV = sc.broadcast(v). 
Then value of this broadcast variable can be accessed via bV.value.

------


`Parquet and Spark`

It is well-known that columnar storage saves both time and space when it comes to big data processing. In particular, Parquet is shown to boost Spark SQL performance by 10x on average compared to using text. 
Spark SQL provides support for both reading and writing parquet files that automatically capture the schema of the original data, so there is really no reason not to use Parquet when employing Spark SQL. 
Saving the df DataFrame as Parquet files is as easy as writing df.write.parquet(outputDir). 
This creates outputDir directory and stores, under it, all the part files created by the reducers as parquet files.


------


`Overwrite save mode in a cluster` .

When saving a DataFrame to a data source, by default, Spark throws an exception if data already exists. However, It is possible to explicitly specify the behavior of the save operation when data already exists. Among the available options, overwrite plays an important role when running on a cluster.
In fact, it allows to successfully complete a job even when a node fails while storing data into disk, allowing another node to overwrite the partial results saved by the failed one. For instance, the df DataFrame can be saved as Parquet files using the overwrite save mode by df.write.mode('overwrite').parquet(outputDir).


------


`Clean code vs. performance` 

When processing a large amount of data, you may need to trade writing clean code for a performance boost. For instance, I once reported that filtering a specific array by creating a new one 
via list comprehension (one line) before processing it was an order of magnitude slower than writing a longer for loop containing the required conditional statements along with the processing steps. 
This is because a new array was being created and additional time to allocate it is required. 
While this might seem a negligible quantity, when the volume of data is huge, this can make the difference between a feasible and an unfeasible operation.

------


`Process data in batches`

While I was initially required to process our original DataFrame in batches due to the cluster configurations, this actually resulted in a very functional method to process data in later stages too. 

The partial results of each batch can then just be merged together and this approach can be very helpful as 
(i) some nodes might go down and lead your job to fail, forcing you to rerun it on the entire dataset; 
and (ii) this might be the only methodology to crunch your data if your application is memory-bounded. 
Moreover, merging all the partial DataFrame I obtained after each stage was an extremely fast and cheap operation, marginalizing the additional overhead.


------

`Local and cluster Spark modes`


_One of the things that you, as a prospective PySpark user, need to get used to is the inherent parallelism of Spark. Even if you are proficient in Python, executing scripts in PySpark requires shifting your thinking a bit._

Spark can be run in two modes: **Local and cluster**. 

* When you run Spark locally your code might not differ to what you are currently used to with running Python: Changes would most likely be more syntactic than anything else but with an added twist that data and code can be copied between separate worker processes.

* However, taking the same code and deploying it to a cluster might cause a lot of head-scratching if you are not careful. This requires understanding how Spark executes a job on the cluster.

* In the cluster mode, when a job is submitted for execution, the job is sent to the driver (or a master) node. The driver node creates a DAG for a job and decides which executor (or worker) nodes will run specific tasks.

* The driver then instructs the workers to execute their tasks and return the results to the driver when done. Before that happens, however, the driver prepares each task's closure: A set of variables and methods present on the driver for the worker to execute its task on the RDD.

![driver executor](http://spark.apache.org/docs/latest/img/cluster-overview.png)

* This set of variables and methods is inherently static within the executors' context, that is, each executor gets a copy of the variables and methods from the driver. If, when running the task, the executor alters these variables or overwrites the methods, it does so without affecting either other executors' copies or the variables and methods of the driver. This might lead to some unexpected behavior and runtime bugs that can sometimes be really hard to track down.




