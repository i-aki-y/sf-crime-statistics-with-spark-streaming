# SF Crime Statistics with Spark Streaming

In this project, an example of spark streaming analysis is presented.
As a data source, real-world dataset, extracted from Kaggle, on San Francisco crime incidents is used.
Apache kafka is used as a data broker. 
A producer service reads each incident from the json file and gives message to the kafka's broker service.
Spark consume the structured streaming data produced by the producer service.

## Development Environment

To examine this project, the following environment is needed.

- Spark 2.4.3
- Scala 2.11.x
- Java 1.8.x
- Kafka build with Scala 2.11.x
- Python 3.6.x or 3.7.x

In a setup, you may need to set the following environment variables.
The detail of the path should differ depending on how you installed the dependencies.

```
export SPARK_HOME=/Users/dev/spark-2.4.3-bin-hadoop2.7
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home
export SCALA_HOME=/usr/local/scala/
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$SCALA_HOME/bin:$PATH
```


## Problem and solution

### step 1

*problem*

Take a screenshot of your kafka-consumer-console output. 

*solution*

![kafka-consumer-console output](./screenshot/step1.png)

### step 2

*problem*

Take a screenshot of your progress reporter after executing a Spark job.

*solution*

![progress reporter](./screenshot/step2-report.png)

*problem*

Take a screenshot of the Spark Streaming UI as the streaming continues. 

*solution*

![spark UI](./screenshot/step2-step2-sparkUI.png)


bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-1 --from-beginning


offsets.topic.replication.factor=1
 
transaction.state.log.replication.factor=1

transaction.state.log.min.isr=1



/usr/bin/zookeeper-server-start ./config/zookeeper.properties
/usr/bin/kafka-server-start ./config/server.properties

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] --conf spark.ui.port=3000 data_stream.py