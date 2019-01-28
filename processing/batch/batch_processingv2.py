"""
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import json, math, datetime
import csv
import io
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import json, math, datetime
from kafka.consumer import SimpleConsumer
from operator import add
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
"""
import os
# add dependency to use spark with kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell'

import numpy as np
# Spark
from pyspark import SparkContext
# Spark Streaming
from pyspark.streaming import StreamingContext
# Kafka
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import json, math, datetime
from datetime import datetime
from kafka.consumer import SimpleConsumer

from operator import add
"""
# cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from cassandra.query import BatchType
"""
import pyspark_cassandra
from pyspark_cassandra import streaming

# define functions to save rdds to cassandra
def sendCassandra(iter):
    print("send to cassandra")
    cluster = Cluster(['52.32.25.168', '34.210.238.118', '35.166.115.1', '52.26.170.191']) # connect to cassandra
    session = cluster.connect()
    session.execute('USE ' + "playground") # provide keyspace
    insert_statement = session.prepare("INSERT INTO checkin2 (User_id, Venue_id, Time, Latitude, Longitude, Time_org) VALUES (?,?,?,?,?,?)")
    count = 0
  #  batch = BatchStatement( batch_type=BatchType.COUNTER)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for record in iter:

        batch.add(insert_statement,(record[1][0], record[1][1], record[1][2], record[1][2],record[1][2], record[1][2]))

        count += 1
        # if count % 200== 0:
            # session.execute(batch)
            # batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    print "saved"
    session.execute(batch)
    session.shutdown()

def test2(v):
    key=v[0]
    return (key,(int(v[0]),int(v[1]),int(v[2])))	

def main():



    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sc.setLogLevel("WARN")

    # set microbatch interval as 5 seconds, this can be customized according to the project
    ssc = StreamingContext(sc, 1)
    # directly receive the data under a certain topic
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['ctest'], {"metadata.broker.list": 'ec2-52-32-25-168.us-west-2.compute.amazonaws.com:9092'})
    print "before bachtchdata"
    batchdata = kafkaStream.map(lambda x: x[1])
    counts = batchdata.map(lambda line: line.split(',')) # since the input data are csv files, specify the delimeter "," in order to extract the values
    # Processed_data = counts.map(getValue).reduceByKey(lambda a, b: (a[0], a[1], a[2], a[3]+ b[3], a[4]+ b[4], a[5]+ b[5], a[6]+ b[6], a[7]+ b[7], a[8]+ b[8], a[9]+ b[9], a[10]+ b[10], a[11]+ b[11], a[12]+ b[12], a[13]+ b[13], a[14]+ b[14])).map(getAvg)
    time_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
    my_row = counts.map(lambda x: {
          "user_id": int(x[1][1]) })
   # Processed_data = counts.map(test2).map(lambda x: (x[1][0]))

   #Processed_data.pprint()
    #State_data.pprint()
    #Recent_data.pprint()
    #Recent_data.pprint()

    #  Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra)) # save rdd in different format to different tables
    # Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandrastate))
    # Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandrarecent))
    print "before myrow"
    counts.pprint()
    my_row.pprint()
    my_row.saveToCassandra("playground", "checkin3")
    print "after myrow"
    ssc.start()
    ssc.awaitTermination()

    return


if __name__ == '__main__':

    main()
