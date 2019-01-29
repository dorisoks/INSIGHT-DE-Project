
import numpy as np
# Spark
import json, math, datetime

from operator import add

# cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from cassandra.query import BatchType



# define functions to save rdds to cassandra
print("send to cassandra")
cluster = Cluster(['52.88.251.94'], control_connection_timeout=None,  port=9042) # connect to cassandra
session = cluster.connect()
session.execute('USE ' + "playground") # provide keyspace
insert_statement = session.prepare("INSERT INTO checkin1 (User_id, Venue_id, Time) VALUES (?,?,?)")
    #insert_statement = session.prepare("INSERT INTO test1 (User_id, Venue_id, Time, Latitude, Longitude, Time_org) VALUES (?,?,?,?,?,?)")
count = 0
#batch = BatchStatement( batch_type=BatchType.COUNTER)
batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

batch.add(insert_statement,(4,5,6))
count += 1
        # if count % 200== 0:
            # session.execute(batch)
            # batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
print "saved"
session.execute(batch)
session.shutdown()

