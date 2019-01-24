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

# define functions to save rdds to cassandra
def sendCassandra(iter):
    print("send to cassandra")
    cluster = Cluster(['54.201.89.215', '52.41.133.58', '52.41.236.79', '34.215.0.219']) # connect to cassandra
    session = cluster.connect()
    session.execute('USE ' + "Checkin") # provide keyspace
    insert_statement = session.prepare("INSERT INTO rawdata (Time, ID, Business, Scale, State, Ecoll, Efacility, Efan, Gfacility, Eheat, Gheat, Einterequip, Ginterequip, Einterlight, Gwater) VALUES (?, ?,?,?,?,?,?,?,?,?,,?,?,?,?)")
    count = 0





def main():
    
    
    
    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sc.setLogLevel("WARN")

    # set microbatch interval as 5 seconds, this can be customized according to the project
    ssc = StreamingContext(sc, 5)
    # directly receive the data under a certain topic
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['EC'], {"metadata.broker.list": 'ec2-18-236-5-16.us-west-2.compute.amazonaws.com:9092'}) 
    batchdata = kafkaStream.map(lambda x: x[1])
    counts = batchdata.map(lambda line: line.split(',')) # since the input data are csv files, specify the delimeter "," in order to extract the values
    Processed_data = counts.map(getValue).reduceByKey(lambda a, b: (a[0], a[1], a[2], a[3]+ b[3], a[4]+ b[4], a[5]+ b[5], a[6]+ b[6], a[7]+ b[7], a[8]+ b[8], a[9]+ b[9], a[10]+ b[10], a[11]+ b[11], a[12]+ b[12], a[13]+ b[13], a[14]+ b[14])).map(getAvg) 
    

    Processed_data.pprint()
    #State_data.pprint()
    #Recent_data.pprint()
    #Recent_data.pprint()

    Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra)) # save rdd in different format to different tables
    Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandrastate))
    Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandrarecent))
    
    ssc.start()
    ssc.awaitTermination()

    return


if __name__ == '__main__':

    main()

