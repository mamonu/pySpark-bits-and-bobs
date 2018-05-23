
## Transformations




## Transformation


Eg - map, filter, flatMap ...

Changes data from one format to another

Lazy execution - Delays execution untill finds an 'Action' so that it can prepare optimized lineage ( spark internal code pipeline )




        map(func) Return a new distributed dataset formed by passing each element of the source through a function func.

        filter(func) Return a new dataset formed by selecting those elements of the source on which func returns true.

        flatMap(func) Similar to map, but each input item can be mapped to 0 or more output items (so func should return a Seq rather than a single item).

        mapPartitions(func) Similar to map, but runs separately on each partition (block) of the RDD.

        mapPartitionsWithIndex(func) Similar to mapPartitions, but also provides func with an integer value representing the index of the partition,

        sample(withReplacement, fraction, seed) Sample a fraction fraction of the data, with or without replacement, using a given random number generator seed.

        union(otherDataset) Return a new dataset that contains the union of the elements in the source dataset and the argument.

        intersection(otherDataset) Return a new RDD that contains the intersection of elements in the source dataset and the argument.

        distinct([numTasks])) Return a new dataset that contains the distinct elements of the source dataset.

        groupByKey([numTasks]) When called on a dataset of (K, V) pairs,

        reduceByKey(func, [numTasks]) When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.

        aggregateByKey(zeroValue)(seqOp, combOp, [numTasks]) When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.

        sortByKey([ascending], [numTasks]) When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean ascending argument.

        join(otherDataset, [numTasks]) When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.

        cogroup(otherDataset, [numTasks]) When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable

        cartesian(otherDataset) When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).

        pipe(command, [envVars]) Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.

        coalesce(numPartitions) Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.

        repartition(numPartitions) Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network.

        repartitionAndSortWithinPartitions(partitioner) Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling repartition and then sorting within each partition because it can push the sorting down into the shuffle machinery.



## Actions

Eg - count, collect, reduce ...Trigger execution of pipeline



        reduce(func) Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel.

        collect() Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.

        count() Return the number of elements in the dataset.

        first() Return the first element of the dataset (similar to take(1)).

        take(n) Return an array with the first n elements of the dataset.

        takeSample(withReplacement, num, [seed]) Return an array with a random sample of num elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.

        takeOrdered(n, [ordering]) Return the first n elements of the RDD using either their natural order or a custom comparator.

        saveAsTextFile(path) Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file.

        saveAsSequenceFile(path) (Java and Scala) Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc).

        saveAsObjectFile(path) (Java and Scala) Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using SparkContext.objectFile().

        countByKey() Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key.

        foreach(func) Run a function func on each element of the dataset. This is usually done for side effects such as updating an Accumulator or interacting with external storage systems. Note: modifying variables other than Accumulators outside of the foreach() may result in undefined behavior. See Understanding closures for more details.

        
