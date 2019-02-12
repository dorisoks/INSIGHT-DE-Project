from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

#import schema
#import config


if __name__ == "__main__":

	# Set Spark context
	sc = SparkContext(appName="join_test")
#	sc.addPyFile('config.py')
	# Set Spark SQL context
        sqlContext = SQLContext(sc)
      
        user_rat = "s3n://checkin0129/friendratingavg3.dat"
        venue_loc = "s3n://checkin0129/venues_sf.txt"
#   	user_soc = "/home/ubuntu/bk_socialgraph.dat"
 #       rating_soc = "/home/ubuntu/bk_ratings.dat"
        #user_bucket = "s3a://userclicklogs/log*.txt
	# Read user clicklogs from S3
	rat_logs = sc.textFile(user_rat)
        venue_logs = sc.textFile(venue_loc)

	# Split lines into columns by delimiter '\t'
	
        record_rat = rat_logs.map(lambda x: x.strip().split(",")).map(lambda x: (x[1].strip(), x[0].strip(), x[2].strip()))
        record_venue = venue_logs.map(lambda x: x.strip().split("|")).map(lambda x: (x[0].strip(), x[1].strip(), x[2].strip()))
        print "convert to DF"
       # record.pprint()
	# Convert Rdd into DataFram
        df_rat = sqlContext.createDataFrame(record_rat,['venue_id','user_id', 'rating'])
       # df_soc_id2 = df_soc.where(df_soc.user_id == 2).collect()
        df_venue = sqlContext.createDataFrame(record_venue,['venue_id','lat', 'log'])
 
        df_rat.show(20)
        df_venue.show(20)
       	#sql_sc = SQLContext(sc)
        #re = sql_sc.read\
        #.format("org.apache.spark.sql.cassandra")\
       # .options(table="venue_visit", keyspace="playground")\
        #.load()

       # df_soc.repartition(2000, 'friend_id')
       # df_rating.repartition(2000, 'friend_id')

	df_join = df_venue.join(df_rat, on = 'venue_id')
        df_join.show(20)  
#        sqlContext.registerDataFrameAsTable(df_soc, 'temp')
#	df_filter = sqlContext.sql ("""select * from temp where user_id = '56'""")
       # df_filter = df_join.where("user_id like '%56627%'")
       # df_filter.show(20) 
       # df_filter.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='friendrating3',keyspace='playground').save()    
       # df_group = df_filter.groupby('user_id', 'venue_id').agg({'rating': 'mean'})
       # df_group.show(20)
       # df_group_f = df_group.select(col('user_id').alias('user_id'), col('venue_id').alias('venue_id'), col('avg(rating)').alias('rating'))
       # df_group_f.show(20)
       # df_join_group = df_join.groupby(['user_id','frend_id'])
	#df_grouped = df_join.groupby(['userid', 'category']).count()
        print "save"	
        df_join.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='friendratingloc',keyspace='playground').save()
        print "after save"
