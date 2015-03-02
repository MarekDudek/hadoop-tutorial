#!/bin/bash

hadoop fs -mkdir /user/marek/word-count
hadoop fs -mkdir /user/marek/word-count/input
hadoop fs -mkdir /user/marek/word-count/output



hadoop fs -rm /user/marek/word-count/output/*
hadoop fs -rmdir /user/marek/word-count/output

hadoop jar target/hadoop-tutorial-1.0-SNAPSHOT.jar \
    biz.interretis.hadoop_tutorial.word_count.new_api.WordCountNewAPI \
    /user/marek/word-count/input \
    /user/marek/word-count/output

hadoop fs -cat /user/marek/word-count/output/part-r-00000
