#!/usr/bin/env bash
#do not use log compaction under windows which makes kafka crash if you use log cleaning . The only way to recover from
#this error is to manually delete the folder in data/kafka . Long standing bug KAFKA-1194

#create topic with appropriate configs
kafka-topics --zookeeper 127.0.0.1:2181 --create --topic employee-salary --partitions 1 --replication-factor 1 --config cleanup.policy=compact --config min.cleanable.dirty.ration= 0.001 --config segment.ms=5000

#describe topic configs
kafka-topics --zookeeper 127.0.0.1:2181 --describe --topic employee-salary

#in a new tab start a consumer
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic employee-salary --from-beginning --property print.key=true --property key.separator=,

#start pushing data to the topic
kafka-console-producer --broker-list 127.0.0.1:9092 --topic employee-salary --property parse.key=true --property key.separator=,