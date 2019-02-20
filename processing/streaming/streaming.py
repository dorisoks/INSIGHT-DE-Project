from pyspark.sql.context import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import json, math, datetime
import csv
import io
from pyspark.sql.functions import broadcast
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
print "start"
def sendCassandra(iter):
    print("send to cassandra")
    cluster = Cluster(['52.88.251.94'], control_connection_timeout=None,  port=9042)
   # cluster = Cluster(['52.88.251.94', '52.36.195.134', '35.155.157.29', '34.210.122.249'],  control_connection_timeout=None,  port=9042) # connect to cassandra
    session = cluster.connect()
    session.execute('USE ' + "playground") # provide keyspace
#    session.execute("TRUNCATE checkin1")
    insert_statement = session.prepare("INSERT INTO venuevisitloc (Venue_id, visit, latitude, longitude) VALUES (?,?,?,?)")
    count = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for record in iter:

        batch.add(insert_statement,(record[1][0][0], record[1][0][1], record[1][1][1], record[1][1][2]))
    
    print "saved"
    session.execute(batch)
    session.shutdown()


def sendCassandraVenueVisit(iter):
    print("send Venue Visit to cassandra")
    cluster = Cluster(['52.88.251.94'], control_connection_timeout=None,  port=9042)
    session = cluster.connect()
    session.execute('USE ' + "playground") # provide keyspace
    insert_statement = session.prepare("INSERT INTO venueVisit (venue_id, visit) VALUES (?,?)")
    count = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for record in iter:

        batch.add(insert_statement,(record[1][0], record[1][1]))
    print "saved"
    session.execute(batch)
    session.shutdown()

def venueVisitMap(v):
    key = v[2]
    return (key,(int(v[2]), 1)) 

def test2(v):
    key=v[0]
    return (key,(int(v[0]),int(v[1]),int(v[2])))	

def row2rdd(v):
    key=v[0]
    return (key,(int(v[0]), v[1],v[2]))


def main():

    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sc.setLogLevel("WARN")
    # set microbatch interval as 1 seconds, this can be customized according to the project
    ssc = StreamingContext(sc, 5)
    # directly receive the data under a certain topic
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['ctest'], {"metadata.broker.list": 'ec2-52-88-251-94.us-west-2.compute.amazonaws.com:9092'})

    cluster = Cluster(['52.88.251.94'], control_connection_timeout=None,  port=9042)
    session = cluster.connect('playground')    
    locrows = session.execute('SELECT venue_id, lat, lon FROM venuelocsf')
    spark = SparkSession.builder.getOrCreate()
    print "start to build table"   
    tablea = spark.createDataFrame(locrows,['venue_id', 'latitude', 'longitude'])
    tablea.show(20)
    rdd1 = tablea.rdd
    print "rdd1"
    print rdd1.take(10)
    rdd2 = rdd1.map(row2rdd).map(lambda x: (x[1][0], (x[1][0], x[1][1], x[1][2])))
    print rdd2.take(10)
    print "after rdd"
    batchdata = kafkaStream.map(lambda x: x[1])
    	# x: kafkastream0001, (u123,u234,...)
	# x[1]: (u123,u234)
    counts = batchdata.map(lambda line: line.split('|')) 	      
   # line: (u123, u234)
	# out: u123, u234
	# since the input data are csv files, specify the delimeter "," in order to extract the values
    print "print counts"
   # counts.pprint()	# u123, u235, ....
    Processed_data = counts.map(test2).map(lambda x: (x[1][0], (x[1][0], x[1][1], x[1][2]))).reduceByKey(lambda x,y: (x[0],x[1],x[2]+y[2]))
    #Processed_locdata = countsloc.map(test2).map(lambda x: (x[1][0], (x[1][0], x[1][1], x[1][2])))
    # counts:       u123, u235, u346, ....
	# after test2: (u123, (u123, u235, u346))
	# lambda:      (u123, (u123, u235, u346)):
	# x: u123, u235, u346; y: u123, u666, u001
        # reducebyKey; (u123, u235, u346 + u001)
    
    venue_visit_data = counts.map(venueVisitMap).map(lambda x: (x[1][0], (x[1][0],x[1][1]))).reduceByKey(lambda x,y: (x[0],x[1]+y[1]))
    venue_visit_data.pprint()
    venuejoin = venue_visit_data.transform(lambda rdd: rdd.join(rdd2))
    venuejoin.pprint()
    print "after print"
    venuejoin.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra))

    ssc.start()
    ssc.awaitTermination()

    return


if __name__ == '__main__':

    main()
