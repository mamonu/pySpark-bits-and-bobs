# Broadcast variables and accumulators in Apache Spark









BROADCAST VARIABLES:

* Broadcast variables are pretty simple in concept. They're variables that we want to share throughout our cluster. 
However there are a couple of caveats that are important to understand. 
Broadcast variables have to be able to fit in memory on one machine. 
That means that they definitely should NOT be anything super large, like a large table or massive vector. 
Secondly, broadcast variables are immutable, meaning that they cannot be changed later on. 
This may seem inconvenient but it truly suits their use case.

* Broadcast variables are:

      Immutable
      Distributed to the cluster
      Fit in memory




ACCUMULATOR VARIABLES:


On the other hand .... Accumulators are global counter variables where each node of the cluster can write values in to.
These are the variables that you want to keep updating as a part of your operation like for example while reading log lines, 
one would like to maintain a real time count of number of certain log type records identified.



      Accumulators are Spark’s answer to Map-Reduce counters but they do much more than that. **** (TBC)
