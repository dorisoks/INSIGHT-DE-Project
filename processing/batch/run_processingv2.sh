/usr/local/spark/bin/spark-submit --master spark://ec2-52-32-25-168.us-west-2.compute.amazonaws.com:7077 \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,TargetHolding/pyspark-cassandra:0.1.5 \
--conf spark.cassandra.connection.host=52-32-25-168 ~/INSIGHT-DE-Project/processing/batch/batch_processingv2.py ctest
