from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import os
'''
spark-submit --master local[*] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.3 /media/jovan/seagate_expansion_drive/Fax/MASTER/BigData/Projekat1/kafka_consumer.py --ip_address 10.152.152.11 --topic test --broker localhost:9092
'''
def process_data(input_data,ip_address):
    data = input_data.map(lambda x:json.loads(x[1]))
    data.cache()

    # Data structure is now in the shape of a dictionary

    ### PART1 ###
    tracking_data = data.filter(lambda x: \
        (x['Src IP'] == ip_address))

    if (tracking_data.isEmpty()):
        print('No traffic on tracked address...')
    else:
        # Measure forward packets
        packet_max = tracking_data.map(lambda x: float(x['Total Fwd Packet'])).max()
        packet_min = tracking_data.map(lambda x: float(x['Total Fwd Packet'])).min()
        packet_avg = tracking_data.map(lambda x: float(x['Total Fwd Packet'])).mean()
        packet_std = tracking_data.map(lambda x: float(x['Total Fwd Packet'])).stdev()
        print(f'Total Fwd Packet: max={packet_max} min={packet_min} avg={packet_avg} std={packet_std}')
        ## Insert data to database
        cassandra_session.execute(f"""
        INSERT INTO test_keyspace.ip_tracker(time, ip_address, max, min, avg, std)
        VALUES (toTimeStamp(now()), '{ip_address}', {packet_max}, {packet_min}, {packet_avg}, {packet_std})
        """)
        # NOTICE: In CQLSH we must put single quotation marks on our ip adresses
        ##########################

    ### PART2 ###
    # Top 3 most source ip addresses
    popularity_data = data\
        .map(lambda x: (x['Src IP'], 1))\
        .reduceByKey(lambda x,y : x+y)\
        .sortBy(lambda x: x[1],ascending=False)\
        .take(3)
    
    N = len(popularity_data)
    if N==0:
        popularity_data = [('',0),('',0),('',0)]
    elif N==1:
        popularity_data.append(('',0))
        popularity_data.append(('',0))
    elif N==2:
        popularity_data.append(('',0))
    
    print(popularity_data)

    cassandra_session.execute(f"""
        INSERT INTO test_keyspace.popularity(time, address1, address2, address3, value1, value2, value3)
        VALUES (toTimeStamp(now()), '{popularity_data[0][0]}', '{popularity_data[1][0]}', '{popularity_data[2][0]}', {popularity_data[0][1]}, {popularity_data[1][1]}, {popularity_data[2][1]})
        """)

def build_database(cassandra_session):
    KEYSPACE = 'test_keyspace'
    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % KEYSPACE)

    cassandra_session.set_keyspace(KEYSPACE)

    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS ip_tracker (
            time TIMESTAMP,
            ip_address text,
            max float,
            min float,
            avg float,
            std float,
            PRIMARY KEY (time)
        )
        """)
    
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS popularity (
            time TIMESTAMP,
            address1 text,
            address2 text,
            address3 text,
            value1 int,
            value2 int,
            value3 int,
            PRIMARY KEY (time)
        )
        """)

if __name__ == '__main__':

    kafka_url = os.getenv('KAFKA_URL')
    topic = os.getenv('KAFKA_TOPIC')
    ip_address= os.getenv('TARGET_IP_ADDDRESS')
    spark_master = os.getenv('SPARK_MASTER')
    cassandra_host = os.getenv('CASSANDRA_HOSTNAME')

    if not kafka_url:
        print("**********KAFKA_URL NOT SET**************")
    if not topic:
        print("TOPIC NOT SET")
    if not ip_address:
        print("IP ADDRESS NOT SET")
    if not spark_master:
        print("SPARK MASTER NOT SET")

    sc = SparkContext(appName='Darknet')
    # sc.getConf().set('spark.jars.package', 'org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.3')
    ssc = StreamingContext(sc,2)
    sc.setLogLevel("ERROR")

    #CASSANDRA SETUP
    cassandra_cluster = Cluster([cassandra_host],port=9042)
    cassandra_session = cassandra_cluster.connect()
    build_database(cassandra_session)

    print('SUCCESSFULLY CONNECTED TO CASSANDRA DATABASE')

    stream = KafkaUtils.createDirectStream(ssc,[topic],{'metadata.broker.list':kafka_url}) #broker
    print('SUCCESSFULLY CONNECTED TO KAFKA BROKER')

    result = stream.foreachRDD(lambda x: process_data(x, ip_address))
    
    ssc.start()
    ssc.awaitTermination()