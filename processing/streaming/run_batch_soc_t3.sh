#!/bin/bash
#/usr/local/spark/bin/spark-submit 
#--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 
#--py-files config.py 
#--conf spark.cassandra.connection.host=ec2-52-88-251-94.us-west-2.compute.amazonaws.com 
#--executor-memory 4500m 
#--driver-memory 5500m 
#--conf spark.sql.shuffle.partitions=5000

#!/bin/bash
/usr/local/spark/bin/spark-submit --master spark://ec2-52-88-251-94.us-west-2.compute.amazonaws.com:7077 --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.6.0 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,com.datastax.spark:spark-cassandra-connector_2.11:2.3.2 --py-files config.py --conf spark.cassandra.connection.host=ec2-52-88-251-94.us-west-2.compute.amazonaws.com  --executor-memory 4500m --driver-memory 5500m batch_social_t4.py
