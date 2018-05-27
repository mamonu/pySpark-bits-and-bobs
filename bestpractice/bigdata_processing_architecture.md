### Goals of Big Data Processing Architecture
* A good real-time data processing architecture needs to be fault-tolerant and scalable.
* It needs to support batch and incremental updates, and must be extensible.

### Solutions 
* Nathan Marz, creator of Apache Storm, describing what we have come to know as the Lambda architecture. The Lambda architecture has proven to be relevant to many use-cases and is indeed used by a lot of companies, for example Yahoo and Netflix. But of course, Lambda is not a silver bullet and has received some fair criticism on the coding overhead it can create.
* Jay Kreps from LinkedIn posted an article describing what he called the Kappa architecture, which addresses some of the pitfalls associated with Lambda. Kappa is not a replacement for Lambda, though, as some use-cases deployed using the Lambda architecture cannot be migrated.

## Lambda Architecture

![Lambda](https://www.ericsson.com/research-blog/wp-content/uploads/2015/11/LambdaKappa1_1.png)

### Layers
* Batch
  1. managing historical data
  2. recomputing results such as machine learning models. S
  3. Specifically, the batch layer receives arriving data, combines it with historical data and recomputes results by iterating over the entire combined data set. 
  4. The batch layer operates on the full data and thus allows the system to produce the most accurate results. 
  5. However, the results come at the cost of high latency due to high computation time.
* Speed
  1. The speed layer is used in order to provide results in a low-latency, near real-time fashion. 
  2. The speed layer receives the arriving data and performs incremental updates to the batch layer results. Thanks to the incremental algorithms implemented at the speed layer, computation cost is significantly reduced. 
* Serving
  1. The serving layer enables various queries of the results sent from the batch and speed layers.
***

## Kappa Architecture

![Kappa](https://www.ericsson.com/research-blog/wp-content/uploads/2015/11/LambdaKappa1_2.png)

  1. One of the important motivations for inventing the Kappa architecture was to avoid maintaining two separate code bases for the batch and speed layers. 
  2. The key idea is to handle both real-time data processing and continuous data reprocessing using a single stream processing engine. 
  3. Data reprocessing is an important requirement for making visible the effects of code changes on the results. As a consequence, the Kappa architecture is composed of only two layers: stream processing and serving. 
  4. The stream processing layer runs the stream processing jobs. Normally, a single stream processing job is run to enable real-time data processing. 
  5. Data reprocessing is only done when some code of the stream processing job needs to be modified. This is achieved by running another modified stream processing job and replying all previous data. 
  6. Finally, similarly to the Lambda architecture, the serving layer is used to query the results.

## Implementation Technologies - Pre Apache Spark 2.x era
  1. Data can be ingested into the Lambda and Kappa architectures using a publish-subscribe messaging system, for example Apache Kafka. 
  2. The data and model storage can be implemented using persistent storage, like HDFS. A high-latency batch system such as Hadoop MapReduce can be used in the batch layer of the Lambda architecture to train models from scratch. 
  3. Low-latency systems, for instance Apache Storm, Apache Samza, and Spark Streaming can be used to implement incremental model updates in the speed layer. The same technologies can be used to implement the stream processing layer in the Kappa architecture.

## Implementation Technologies - Apache Spark 2.x era
  1. Apache Spark can be used as a common platform to develop the batch and speed layers in the Lambda architecture. 
  2. This way, much of the code can be shared between the batch and speed layers. 
  3. The serving layer can be implemented using a NoSQL database, such as Apache HBase, and an SQL query engine like Apache Drill.

## Which one to choose ?

* A very simple case to consider is when the algorithms applied to the real-time data and to the historical data are identical. Then it is clearly very beneficial to use the same code base to process historical and real-time data, and therefore to implement the use-case using the Kappa architecture.

* The algorithms used to process historical data and real-time data are not always identical. In some cases, the batch algorithm can be optimized thanks to the fact that it has access to the complete historical dataset, and then outperform the implementation of the real-time algorithm. Here, choosing between Lambda and Kappa becomes a choice between favoring batch execution performance over code base simplicity.

 * Machine learning application where generation of the batch model requires so much time and resources that the best result achievable in real-time is computing and approximated updates of that model. In such cases, the batch and real-time layers cannot be merged, and the Lambda architecture must be used.
