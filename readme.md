./kafka-console-consumer.sh --topic topicin --bootstrap-server localhost:9092
./kafka-console-consumer.sh --topic topicout1 --bootstrap-server localhost:9092

bin/flume-ng agent --conf /opt/LiveInsights/apache-flume-1.8.0-bin/conf/ --conf-file conf/flume-kstreams.conf --name agent -Dflume.root.logger=INFO,console 

java -cp kafka-streams-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.cognizant.tmg.wfm.kafka_streams.TmgAggregationV1


 bin\windows\kafka-server-start.bat config\server.properties
 
 .\kafka-console-consumer.bat --topic livetemperature --bootstrap-server localhost:9092	
 
 kafka-topics.bat --create --topic livedata --partitions 1 --replication-factor 1 --zookeeper localhost:2181 
 
 
 ./kafka-console-producer.sh --broker-list localhost:2181 --topic 1Kin 

./zookeeper-shell.sh localhost:2181 ls /brokers/ids

 ./zookeeper-shell.sh localhost:2181 <<< "get /brokers/ids/2"
 
 
  ./kafka-topics.sh --zookeeper localhost:2181 --delete --topic kafkain
 
./kafka-topics.sh --list --zookeeper localhost:2181 

./kafka-topics.sh --create --topic livetemperature --partitions 1 --replication-factor 1 --zookeeper 10.142.194.249:2181 


./kafka-configs.sh --alter --entity-name datain --entity-type topics --add-config message.timestamp.type=LogAppendTime --zookeeper localhost:2181 

