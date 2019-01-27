/usr/local/spark/bin/spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,TargetHolding/pyspark-cassandra:0.3.5 \
--conf spark.cassandra.connection.host=52.32.25.168 test_kafka_spark_cassandra.py ctest2
