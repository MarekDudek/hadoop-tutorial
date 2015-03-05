#!/bin/bash

hadoop fs -rm file:///home/cloudera/Devel/IdeaProjects/hadoop-tutorial/target/output/*
hadoop fs -rmdir file:///home/cloudera/Devel/IdeaProjects/hadoop-tutorial/target/output


hadoop jar ./target/hadoop-tutorial-1.0-SNAPSHOT.jar \
    biz.interretis.hadoop_tutorial.word_count.new_api.WordCountNewAPI \
    file:///home/cloudera/Devel/IdeaProjects/hadoop-tutorial/src/main/resources/word-count/gettysburg-address.txt \
    file:///home/cloudera/Devel/IdeaProjects/hadoop-tutorial/target/output

hadoop fs -cat file:///home/cloudera/Devel/IdeaProjects/hadoop-tutorial/target/output

