
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as F
import os

hdfs_url = os.getenv('HDFS_URL')
dataset_loc = os.getenv('DATASET_LOCATION')
spark_master = os.getenv('SPARK_MASTER')
ip_address= os.getenv('TARGET_IP_ADDDRESS')


spark = SparkSession.builder.appName("Darknet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = spark.read.csv(hdfs_url+dataset_loc,header=True)


import time
from datetime import datetime
#Returns a timestamp based on the input date (format must be d/m/y)
def date_to_timestamp(date):
    return time.mktime(datetime.strptime(date,'%d/%m/%Y').timetuple())

#Select columns we want to preview
df = data.select(F.col('Src IP'), F.col('Dst IP'), F.col('Timestamp'), F.col('Total Fwd Packet'), F.col('Label1'), F.col('Label2'))

#Filter Data based on type of traffic on the dark net
df = df.where(F.col('Label2') == 'P2P').where(F.col('Src IP')==ip_address) #.where((F.col('Label1')=='Tor') | (F.col('Label1')=='VPN'))
start_date = '23/02/2016'
end_date = '24/02/2016'

start_timestamp = date_to_timestamp(start_date)
end_timestamp = date_to_timestamp(end_date)

#Filter data within a selected time frame
df = df.where(F.col('Timestamp').between(start_timestamp,end_timestamp))

df.show()
rdd1 = df.rdd.map(lambda x: (x['Src IP'], 1)).reduceByKey(lambda x,y : x+y)
print('*'*50)
print(f'Number of P2P darknet packet transactions between {start_date} and {end_date}, ip address : {ip_address}')
print(rdd1.take(1))
print('*'*50)

packet_max = df.rdd.map(lambda x: float(x['Total Fwd Packet'])).max()
packet_min = df.rdd.map(lambda x: float(x['Total Fwd Packet'])).min()
packet_avg = df.rdd.map(lambda x: float(x['Total Fwd Packet'])).mean()
packet_std = df.rdd.map(lambda x: float(x['Total Fwd Packet'])).stdev()

print(f'Total Fwd Packet: max={packet_max} min={packet_min} avg={packet_avg} std={packet_std}')
print('*'*50)