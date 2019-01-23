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
    session.execute('USE ' + "Energy_consumption") # provide keyspace
    insert_statement = session.prepare("INSERT INTO rawdata (Time, ID, Business, Scale, State, Ecoll, Efacility, Efan, Gfacility, Eheat, Gheat, Einterequip, Ginterequip, Einterlight, Gwater) VALUES (?, ?,?,?,?,?,?,?,?,?,,?,?,?,?)")
    count = 0
