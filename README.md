# Big Data Projects

### Description
This repository consists of three projects using the Apache Spark and Kafka technologies in Python:
1. Batch processing of data
2. Spark streaming
3. Spark ML (consists of 2 parts; training and classification)

Technologies used in the project:
* Python 3.8.5

* Apache Spark 2.4.3

* Apache Hadoop 2.7.0

* Apache Kafka 2.5.0

* Apache Zookeeper 3.6.1

### Instructions

What is essential for all project is to run the following shell scripts in order:

1. start_spark.sh
2. upload_dataset_to_hdfs.sh

#### Running Project I

The only thing required to run the 1st project is to execute the start_p1.sh script.

#### Running Project II

Since this project is using kafka we need to first activate kafka brokers and then run the consumer and producer in parallel:

1. start_kafka.sh
2. start_p2.sh
3. kafka_producer.py

#### Running Project III

This project is diveded into 2 parts (A: training, B: classification)

##### Running Project III A

To execute the training simply run the script start_p3_training.sh

##### Running Project III B

Since this project is using kafka we need to first activate kafka brokers and then run the consumer and producer in parallel:

1. start_kafka.sh
2. start_p3_classification.sh
3. kafka_producer.py
