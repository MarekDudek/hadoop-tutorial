#!/bin/bash

# Checking contents of directory
hadoop fs -ls /user/


# Creating directories
hadoop fs -mkdir /user/marek/
hadoop fs -mkdir /user/marek/max-temperature


# Copying data from local file system to HDSF
hadoop fs -copyFromLocal ./src/main/resources/1901 hdfs:///user/marek/max-temperature
hadoop fs -copyFromLocal ./src/main/resources/1902 hdfs:///user/marek/max-temperature


# Checking data
hadoop fs -ls /user/marek/max-temperature


# Running job
hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar \
    biz.interretis.hadoop_tutorial.max_temperature.MaxTemperature \
    /user/marek/max-temperature/1901 \
    /user/marek/max-temperature/output


# Checking results
hadoop fs -cat /user/marek/max-temperature/output/part-r-00000


# Removing directories
hadoop fs -rm /user/marek/max-temperature/output/*
hadoop fs -rmdir /user/marek/max-temperature/output