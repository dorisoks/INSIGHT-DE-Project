/usr/local/spark/bin/spark-submit --master spark://ec2-54-201-89-215.us-west-2.compute.amazonaws.com:7077 \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
--num-executors 4 \
--executor-cores 3 \
--executor-memory 2G \
--conf spark.scheduler.mode=FAIR \
--conf spark.default.parallelism=2 \
~/INSIGHT-DE-Project/processing/batch/batch_processing.py
