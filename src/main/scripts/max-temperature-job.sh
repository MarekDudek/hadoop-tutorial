#!/bin/bash

hadoop fs -rm /user/marek/max-temperature/output/*
hadoop fs -rmdir /user/marek/max-temperature/output

hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar \
    biz.interretis.hadoop_tutorial.max_temperature.MaxTemperature \
    /user/marek/max-temperature/1901 \
    /user/marek/max-temperature/output

hadoop fs -cat /user/marek/max-temperature/output/part-r-00000
