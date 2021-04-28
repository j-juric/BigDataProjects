from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, NaiveBayes
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorIndexer, VectorAssembler, StringIndexer
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os

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
    spark = SparkSession.builder.appName('Darknet').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #data_path = '/media/jovan/seagate_expansion_drive/Fax/MASTER/BigData/Projekat1/Darknet2.csv'
    ip_address= os.getenv('TARGET_IP_ADDDRESS')
    hdfs_url = os.getenv('HDFS_URL')
    model_path = os.getenv('MODEL_LOCATION')
    dataset_loc = os.getenv('DATASET_LOCATION')

    df = spark.read.csv(hdfs_url+dataset_loc, header=True)

    

    print('*'*50)
    df.printSchema()
    print('*'*50)
    

    # Drop unnecessary columns

    df = preprocess_data(df)

    print('*'*50)
    df.printSchema()
    print('*'*50)
    print(f'Df column count:{len(df.columns)}')

    #Split data
    train_data, test_data = df.randomSplit([0.8,0.2],1337)

    print('Training Logistic Regression Model...')
    #Build model
    lr = LogisticRegression(maxIter=100, regParam=0.02, elasticNetParam=0.8)
    #Train model
    pipeline = Pipeline(stages=[lr])
    lr_model = pipeline.fit(train_data)
    #Predict
    print('Making predictions...')
    pred = lr_model.transform(test_data)

    # #Evaluate data
    evaluator = BinaryClassificationEvaluator(labelCol='label',rawPredictionCol='prediction',metricName='areaUnderROC')
    accuracy = evaluator.evaluate(pred)

    print(f'Logistic Regression Model accuracy = {accuracy}')

    print('*'*50)

    # print('Training Decision Tree Model...')
    # dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
    # pipeline = Pipeline(stages=[dt])
    # dt_model = pipeline.fit(train_data)
    
    # print('Making predictions...')
    # pred = dt_model.transform(test_data)
    # accuracy = evaluator.evaluate(pred)
    # print(f'Decision Tree accuracy = {accuracy}')

    print('*'*50)

    print('Saving model...')

    lr_model.write().overwrite().save(hdfs_url+model_path)
    print('Model saved')

