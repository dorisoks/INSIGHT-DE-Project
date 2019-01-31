
from pyspark import SparkContext

from pyspark import SparkConf

from pyspark.sql import SQLContext

from pyspark.sql.types import StringType, DoubleType, IntegerType

from pyspark.sql.functions import udf

from pyspark.sql.functions import col

# import cassandra spark connector(datastax)

from cassandra.cluster import Cluster

from cassandra.query import BatchStatement

from cassandra import ConsistencyLevel

#import schema
import config


def reduce_category(cate):

	if cate:
		reduced_cate = config.cateMapping[cate]
	else:
		reduced_cate = cate

	return reduced_cate

def quiet_logs( sc ):
	logger = sc._jvm.org.apache.log4j
	logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
	logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

if __name__ == "__main__":

	# Set Spark context
	sc = SparkContext(appName="News-Users-Analytics")
	sc.addPyFile('config.py')
	# Set Spark SQL context
        sqlContext = SQLContext(sc)
        user_checkin = "s3n://checkin0129/checkins_new.csv"
	#user_bucket = "s3a://userclicklogs/log*.txt
	# Read user clicklogs from S3
	checkin_logs = sc.textFile(user_checkin)

	# Split lines into columns by delimiter '\t'
	record = checkin_logs.map(lambda x: x.split(","))

	# Convert Rdd into DataFram
	df_click = sqlContext.createDataFrame(record,['id', 'user_id','venue_id','latitude', 'longitude', 'time'])

	#df_click.repartition(10000, 'newsid')

	#df_join = df_filtered.join(df_click, on='newsid')

	#df_grouped = df_join.groupby(['userid', 'category']).count()

	#df_grouped.write.format("org.apache.spark.sql.cassandra").mode('append').options(
  	#table='summary', keyspace=config.CASSANDRA_NAMESPACE).save()
	df_click.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='batchlog',keyspace=config.CASSANDRA_NAMESPACE).save()
