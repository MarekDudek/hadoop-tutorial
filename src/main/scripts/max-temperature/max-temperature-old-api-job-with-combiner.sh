#!/bin/bash

hadoop fs -rm /user/marek/max-temperature/output/*
hadoop fs -rmdir /user/marek/max-temperature/output

hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar \
    biz.interretis.hadoop_tutorial.max_temperature.old_api.MaxTemperatureOldAPIJobWithCombiner \
    /user/marek/max-temperature/input \
    /user/marek/max-temperature/output

hadoop fs -cat /user/marek/max-temperature/output/part-00000
