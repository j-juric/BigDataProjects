FROM bde2020/spark-python-template:2.4.3-hadoop2.7

ENV SPARK_MASTER spark://spark-master:7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/submit.py
ENV TARGET_IP_ADDDRESS 10.152.152.11
ENV HDFS_URL hdfs://namenode:9000
ENV DATASET_LOCATION /data/Darknet2.csv

ADD start.sh /

RUN chmod +x /start.sh

CMD ["/bin/bash", "/start.sh"]