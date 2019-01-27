
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pyspark_cassandra
from pyspark_cassandra import streaming
from datetime import datetime
import sys

# Create a StreamingContext with batch interval of 3 second
sc = SparkContext("spark://ec2-52-32-25-168.us-west-2.compute.amazonaws.com:7077", "myAppName")
ssc = StreamingContext(sc, 1)

topic = sys.argv[1]
kafkaStream = KafkaUtils.createStream(ssc, "52.32.25.168:2181", "ctest2", {topic: 1})

raw = kafkaStream.flatMap(lambda kafkaS: [kafkaS])
time_now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
clean = raw.map(lambda xs: xs[1].split(","))

# Test timestamp 1 and timestamp 2
# times = clean.map(lambda x: [x[1], time_now])
# times.pprint()

# test subtract new time from old time
# x = clean.map(lambda x:
#             (datetime.strptime(x[1], '%Y-%m-%d %H:%M:%S.%f') -
#             datetime.strptime(time_now, '%Y-%m-%d %H:%M:%S.%f')).seconds)
# x.pprint()


# Match table fields with dictionary keys
# this reads input of format
# partition, timestamp, location, price
my_row = clean.map(lambda x: {
      "user_id": x[1][0] })
# my_row.pprint()

my_row.saveToCassandra("playground", "checkin3")

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
