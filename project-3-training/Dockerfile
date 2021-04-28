FROM bde2020/spark-python-template:2.4.3-hadoop2.7

RUN apk add --no-cache py3-numpy

ENV SPARK_MASTER spark://spark-master:7077
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/train_ml_model.py
ENV SPARK_APPLICATION_MAIN_CLASS Project_3
ENV HDFS_URL hdfs://namenode:9000
ENV DATASET_LOCATION /data/Darknet2.csv
ENV MODEL_LOCATION /data/model

ADD start.sh /

RUN chmod +x /start.sh
CMD ["/bin/bash", "/start.sh"]
