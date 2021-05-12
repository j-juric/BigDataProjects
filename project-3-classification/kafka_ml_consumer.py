from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
import json
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorIndexer, VectorAssembler, StringIndexer
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os

def process_data(input_data, model):
    data = input_data.map(lambda x:json.loads(x[1]))
    data.cache()

    if(data.isEmpty()):
        print('No input...')
        return
    
    df = data.toDF()
    df = preprocess_data(df)

    pred = model.transform(df)

    pred.select('prediction','label')

    N = pred.count()
    correct = pred.filter(pred['label']==pred['prediction']).count()

    correct=float(correct)

    print(f'{N} predictions made, accuracy = {correct/N*100}%')

    # evaluator = BinaryClassificationEvaluator(labelCol='label',rawPredictionCol='prediction',metricName='areaUnderROC') 
    # accuracy = evaluator.evaluate(pred)


def preprocess_data(df):

    df=df.withColumn('Label1', F.when((df.Label1 == 'VPN') | (df.Label1 == 'Tor'), 1).otherwise(0))

    columns = ['Flow Duration','Total Fwd Packet','Total Bwd packets','Total Length of Fwd Packet','Total Length of Bwd Packet','Fwd Packet Length Max','Fwd Packet Length Min','Fwd Packet Length Mean','Fwd Packet Length Std','Bwd Packet Length Max','Bwd Packet Length Min','Bwd Packet Length Mean','Bwd Packet Length Std','Flow Bytes/s','Flow Packets/s','Flow IAT Mean','Flow IAT Std','Flow IAT Max','Flow IAT Min','Fwd IAT Total','Fwd IAT Mean','Fwd IAT Std','Fwd IAT Max','Fwd IAT Min','Bwd IAT Total','Bwd IAT Mean','Bwd IAT Std','Bwd IAT Max','Bwd IAT Min','Fwd PSH Flags','Bwd PSH Flags','Fwd URG Flags','Bwd URG Flags','Fwd Header Length','Bwd Header Length','Fwd Packets/s','Bwd Packets/s','Packet Length Min','Packet Length Max','Packet Length Mean','Packet Length Std','Packet Length Variance','FIN Flag Count','SYN Flag Count','RST Flag Count','PSH Flag Count','ACK Flag Count','URG Flag Count','CWE Flag Count','ECE Flag Count','Down/Up Ratio','Average Packet Size','Fwd Segment Size Avg','Bwd Segment Size Avg','Fwd Bytes/Bulk Avg','Fwd Packet/Bulk Avg','Fwd Bulk Rate Avg','Bwd Bytes/Bulk Avg','Bwd Packet/Bulk Avg','Bwd Bulk Rate Avg','Subflow Fwd Packets','Subflow Fwd Bytes','Subflow Bwd Packets','Subflow Bwd Bytes','FWD Init Win Bytes','Bwd Init Win Bytes','Fwd Act Data Pkts','Fwd Seg Size Min','Active Mean','Active Std','Active Max','Active Min','Idle Mean','Idle Std','Idle Max','Idle Min','Label1']

    for c in columns:
        df = df.withColumn(c, F.col(c).cast(FloatType()))

    columns.remove('Label1')

    assembler = VectorAssembler().setInputCols(columns).setOutputCol('features').setHandleInvalid('skip')

    featureDf = assembler.transform(df)
    
    indexer = StringIndexer()\
    .setInputCol("Label1")\
    .setOutputCol("label")
    labelDf = indexer.fit(featureDf).transform(featureDf)
    

    return labelDf
    

if __name__ == '__main__':

    topic = os.getenv('KAFKA_TOPIC')
    kafka_url= os.getenv('KAFKA_URL')
    ip_address= os.getenv('TARGET_IP_ADDDRESS')
    hdfs_url = os.getenv('HDFS_URL')
    model_path = os.getenv('MODEL_LOCATION')
    window_duration = os.getenv('WINDOW_DURATION_IN_SECONDS')

    spark = SparkSession.builder.appName('Darknet').getOrCreate()
    
    sc = spark.sparkContext
    ssc = StreamingContext(sc,int(window_duration))
    sc.setLogLevel("ERROR")

    

    model = PipelineModel.load(hdfs_url+model_path)

    stream = KafkaUtils.createDirectStream(ssc,[topic],{'metadata.broker.list':kafka_url})
    result = stream.foreachRDD(lambda x: process_data(x, model))

    ssc.start()
    ssc.awaitTermination()

