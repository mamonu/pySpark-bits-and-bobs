## Mappers and Reducers

Key-Value pairs for some arbitrary pair of types are  mapped and reduced. 
 
- Most of your parallelism comes from the mappers, since they can (ideally) split the data and transform it without any coordination with other processes. 

- By contrast, the amount of parallelism in the reduction phase has an important limitation: although you may have many reducers, any given reducer is guaranteed to receive all the values for some particular key. So if there are a HUGE number of values for some particular key, you're going to have a bottleneck because they're all going to be processed by a single reducer.

However, there is another way! Certain types of data fit into a pattern:
- they can be combined with other values of the same type to form new values. 
- the combining operation is [associative](https://en.wikipedia.org/wiki/Associative_property). For example, integer addition: ((1 + 2) + 3) == (1 + (2 + 3)) - they have an identity value. (for addition its zero because 1 + 0 == 1)

More formally, the conditions above are known as the [monoid laws](https://en.wikipedia.org/wiki/Monoid) and data that obey it is said to "form a monoid". We can exploit monoidal data structures to perform extremely efficient incremental reduction. If you're using Hadoop, you can get in on the fun with [Twitter's Algebird library](http://www.michael-noll.com/blog/2013/12/02/twitter-algebird-monoid-monad-for-large-scala-data-analytics/). If you're using [Apache Spark its even easier](https://github.com/apache/spark/blob/v1.6.1/core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala#L165).

A number of datatypes are known to obey the monoid laws:

- Integers (with addition as the combiner, and zero as the identity)
- Strings (with concatenation as the combiner, and the empty string as the identity)
- Sets (set union as the combiner, empty sets  as the identity)
- HyperLogLog (hyperloglog union, empty hyperloglog as identity)
- BloomFilters (union and empty as identity)


 
The last two are really interesting. They fall under the category of "[probablistic data structures](https://dzone.com/articles/introduction-probabilistic-0)" which allow you to trade some degree of accuracy in order to save alot of space and time. 

- Hyperloglog provides approximate measurements of set cardinality, the number of distinct items in a collection.
- BloomFilters are a data structure that allow you to query for "probable" set membership. You can put items in, and then later query to find out if an item is "probably" already in the set. However, you cannot enumerate the items that have already been "added" to the BloomFilter.

These are extremely useful features because they allow you to summarize vast amounts of data into buckets that consume very limited amounts of storage space. You can then query to see how big they are or find out if particular items are "probably" inside. 

But the best part is that because of the monoid laws, you no longer have the restrictions where all the values for some key must be grouped together to build your data structures. Instead the probablistic data sets can be build up incrementally, because intermediate results can be combined at any point.

Furthermore, because of the associative property, they can be combined in whatever order they naturally occur in your data, since the ordering doesn't matter.

[@bbejeck](https://twitter.com/bbejeck) provides a [nice example of how to use Spark's aggregateByKey to find unique values per key](http://codingjunkie.net/spark-agr-by-key/), which exploits the monoidal structure of sets. Below, I've adapted his example to build HyperLogLog structures instead.

Using Spark's aggregateByKey API to build hyperloglog data structures incrementally looks like this:

```scala
  def aggregateAttribute(records : RDD[MyRecord], getAttribute : MyRecord => Option[String]) : RDD[(String, HyperLogLog)] = {

    def initialSet = new HyperLogLog

    def addToSet = (s:HyperLogLog, value: String) => {
      s.put(value) //the aggregateByKey interface permits destructive update of first argument
    }

    def mergePartitionSets = (p1: HyperLogLog, p2: HyperLoglog) => {
      p1 combine p2  //the aggregateByKey interface permits destructive update of first argument
    }

    val recordsAttributePair : RDD[(String, String)] = records.flatMap {
      record =>
        for (attr <- getAttribute(record)) yield (attr,record.id)   //flatMap ensures that we toss records with no attribute
    }
    recordsAttributePair.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
  }
```

The code above is psuedo code because there is no built-in HyperLogLog datatype. However, various libraries are available. Twitter
published the [Algebird HyperLogLog monoid here](https://github.com/twitter/algebird/wiki/HyperLogLog). AdRoll has made their [Cantor library available, and they blog about it here](http://tech.adroll.com/blog/data/2013/07/10/hll-minhash.html).
