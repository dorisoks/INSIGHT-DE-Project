/usr/local/spark/bin/spark-submit --master local[4] \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
--conf spark.default.parallelism=1 \
batch_processing.py
