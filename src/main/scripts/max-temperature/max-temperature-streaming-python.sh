#!/bin/bash

hadoop fs -rm /user/marek/max-temperature/output/*
hadoop fs -rmdir /user/marek/max-temperature/output

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming-2.5.0-cdh5.3.0.jar \
    -input   /user/marek/max-temperature/input \
    -output  /user/marek/max-temperature/output \
    -mapper  hdfs:///user/marek/max-temperature/python/map.py \
    -reducer hdfs:///user/marek/max-temperature/python/reduce.py
