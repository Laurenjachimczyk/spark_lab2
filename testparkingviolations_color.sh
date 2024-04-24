#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /nycpark/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /nycpark/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /nycpark/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/parking_violations.csv /nycpark/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./parkingviolations_location.py hdfs://$SPARK_MASTER:9000/nycpark/input/
–conf spark.default.parallelism=2
