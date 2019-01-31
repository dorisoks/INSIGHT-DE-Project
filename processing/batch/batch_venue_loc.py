
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
import numpy as np

print "start"
def sendCassandra(iter):
    print("send to cassandra")
    cluster = Cluster(['52.88.251.94', '52.36.195.134', '35.155.157.29', '34.210.122.249'], control_connection_timeout=None,  port=9042)
   # cluster = Cluster(['52.88.251.94', '52.36.195.134', '35.155.157.29', '34.210.122.249'],  control_connection_timeout=None,  port=9042) # connect to cassandra
    session = cluster.connect()
    session.execute('USE ' + "playground") # provide keyspace
#    session.execute("TRUNCATE checkin1")
    insert_statement = session.prepare("INSERT INTO venueloc (venue_id, Latitude, Longitude) VALUES (?,?,?)")
    count = 0
  #  batch = BatchStatement( batch_type=BatchType.COUNTER)
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for record in iter:

        batch.add(insert_statement,(record[1][0], record[1][1], record[1][2]))

       # count += 1
       # if count % 200== 0:
       #    session.execute(batch)
       #    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    print "saved"
    session.execute(batch)
    session.shutdown()


def sendCassandraVenueVisit(iter):
    print("send Venue Visit to cassandra")
    cluster = Cluster(['52.88.251.94'], control_connection_timeout=None,  port=9042)
    session = cluster.connect()
    session.execute('USE ' + "playground") # provide keyspace
 #   session.execute("TRUNCATE venueVisit")
    insert_statement = session.prepare("INSERT INTO venueVisit (venue_id, visit) VALUES (?,?)")
    count = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for record in iter:

        batch.add(insert_statement,(record[1][0], record[1][1]))

       # count += 1
       # if count % 200== 0:
       #    session.execute(batch)i
       #    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    print "saved"
    session.execute(batch)
    session.shutdown()

def venueVisitMap(v):
    key = v[2]
    return (key,(int(v[2]), 1)) 

def test2(v):
    key=v[0]	
    a = np.float64(v[1])
    return (key,(int(v[0]),float(a),float(a)))	

def main():



    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sc.setLogLevel("WARN")

    # set microbatch interval as 1 seconds, this can be customized according to the project
    ssc = StreamingContext(sc, 1)
    # directly receive the data under a certain topic
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['ctest'], {"metadata.broker.list": 'ec2-52-88-251-94.us-west-2.compute.amazonaws.com:9092'})
    batchdata = kafkaStream.map(lambda x: x[1])
    	# x: kafkastream0001, (u123,u234,...)
	# x[1]: (u123,u234)
    counts = batchdata.map(lambda line: line.split(','))
	# line: (u123, u234)
	# out: u123, u234
	# since the input data are csv files, specify the delimeter "," in order to extract the values
    # Processed_data = counts.map(getValue).reduceByKey(lambda a, b: (a[0], a[1], a[2], a[3]+ b[3], a[4]+ b[4], a[5]+ b[5], a[6]+ b[6], a[7]+ b[7], a[8]+ b[8], a[9]+ b[9], a[10]+ b[10], a[11]+ b[11], a[12]+ b[12], a[13]+ b[13], a[14]+ b[14])).map(getAvg)
    print "print counts"
    counts.pprint()	# u123, u235, ....
    Processed_data = counts.map(test2).map(lambda x: (x[1][0], (x[1][0], x[1][1], x[1][2]))).reduceByKey(lambda x: (x[0],x[1],x[2]))
   	# counts:       u123, u235, u346, ....
	# after test2: (u123, (u123, u235, u346))
	# lambda:      (u123, (u123, u235, u346)):
	# x: u123, u235, u346; y: u123, u666, u001
        # reducebyKey; (u123, u235, u346 + u001)
   # venue_visit_data = counts.map(venueVisitMap).map(lambda x: (x[1][0], (x[1][0], x[1][1]))).reduceByKey(lambda x,y: (x[0],x[1]+y[1]))

    print "print processed_data"
    Processed_data.pprint
    #State_data.pprint()
    #Recent_data.pprint()
    #Recent_data.pprint()
    print "after print"
    Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra)) # save rdd in different format to different tables
   # venue_visit_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandraVenueVisit))
    # Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandrarecent))

    ssc.start()
    ssc.awaitTermination()

    return


if __name__ == '__main__':

    main()
