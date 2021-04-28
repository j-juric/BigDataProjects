from kafka import KafkaProducer
import pandas as pd
import csv
import json
import time
producer = KafkaProducer(bootstrap_servers=['localhost:50023'], value_serializer = lambda x: x.encode('utf-8'))

print('Reading csv file')
df = pd.read_csv('./data/Darknet2.csv')
print('Loaded csv file')

N = 141530
counter=0

for _,d in df.iterrows():
    counter+=1
    producer.send('test', d.to_json())
    producer.flush()
    if(counter%100 == 0):
        print(f'{counter}/{N}')
    time.sleep(0.01)

print('Producer Finished')


