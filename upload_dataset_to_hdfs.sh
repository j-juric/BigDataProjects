docker exec -it namenode hdfs dfs -test -e /big-data
if [ $? -eq 1 ]
then
  echo "[INFO]: Creating /data folder on HDFS"
  docker exec -it namenode hdfs dfs -mkdir /data
fi

docker exec -it namenode hdfs dfs -test -e /data/Darknet2.csv
if [ $? -eq 1 ]
then
  echo "[INFO]: Adding Darknet2.csv in the /big-data folder on the HDFS"
  docker exec -it namenode hdfs dfs -put /data/Darknet2.csv /data/Darknet2.csv
fi
