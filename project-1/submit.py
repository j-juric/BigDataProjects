
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as F
import os

hdfs_url = os.getenv('HDFS_URL')
dataset_loc = os.getenv('DATASET_LOCATION')
spark_master = os.getenv('SPARK_MASTER')

if not hdfs_url:
    print("HDFS URL NOT SET")
if not dataset_loc:
    print("DATASET LOC NOT SET")
if not spark_master:
    print("SPARK MASTER NOT SET")

spark = SparkSession.builder.appName("Darknet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = spark.read.csv(hdfs_url+dataset_loc,header=True)

print(data.count())

print(data.printSchema())

# Preview the column which shows types of traffic for the packets in the dataset
rdd = data.rdd
rdd.map(lambda x:x[-1]).distinct().take(15)

# Preview the column which indicates if the packet flows through the surface web or dark web
rdd.map(lambda x:x[-2]).distinct().take(15)

import time
from datetime import datetime
#Returns a timestamp based on the input date (format must be d/m/y)
def date_to_timestamp(date):
    return time.mktime(datetime.strptime(date,'%d/%m/%Y').timetuple())

#Select columns we want to preview
df1 = data.select(F.col('Src IP'), F.col('Dst IP'), F.col('Timestamp'), F.col('Total Length of Fwd Packet'), F.col('Total Length of Bwd Packet'), F.col('Label1'), F.col('Label2'))

#Filter Data based on type of traffic on the dark net
df1 = df1.where(F.col('Label2') == 'P2P').where((F.col('Label1')=='Tor') | (F.col('Label1')=='VPN'))

start_date = '23/02/2016'
end_date = '24/02/2016'

start_timestamp = date_to_timestamp(start_date)
end_timestamp = date_to_timestamp(end_date)

#Filter data within a selected time frame
df1 = df1.where(F.col('Timestamp').between(start_timestamp,end_timestamp))

print(df1.show(20))

print(df1.summary().show())
