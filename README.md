# Kafka Core Concepts
This is a repository where I log my progress in in-depth practice in Kafka.

## Real world project hands-on
1. [**Wikimedia**](https://github.com/Tavet/KafkaCoreConcepts/tree/main/wikimedia) The purpose of this project is to gain experience in Kafka Connect SSE, Kafka Streams, and Kafka Connect ElasticSearch by using the [Stream endpoints of wikimedia](https://stream.wikimedia.org/?doc).
   1. Demo: [https://esjewett.github.io/wm-eventsource-demo/](https://esjewett.github.io/wm-eventsource-demo/)
   2. Demo: [https://codepen.io/Krinkle/pen/BwEKgW?editors=1010](https://codepen.io/Krinkle/pen/BwEKgW?editors=1010)
   3. Create a Kafka topic: ```kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic wikimedia.recentchange --partitions 3 --replication-factor 1```
2. [**Opensearch**](https://github.com/Tavet/KafkaCoreConcepts/tree/main/opensearch) Kafka Consumer to consume the Wikimedia data producer + Opensearch.
   
## Installation on Linux
Download the binaries from the official Apache Kafka webpage & extract wherever it is most convenient.
I extracted it under ```/home/breyner/.libraries/kafka_2.13-3.1.0``` folder

1. Add the binary folder to the SO env variables.
    1. File ```~/.bashrc_profile```
    2. Add to the bottom ```export PATH="${PATH}:/home/breyner/.libraries/kafka_2.13-3.1.0/bin"```


2. Create a separated folder to store Zookeeper and Kafka data
    1. Move to the binaries root folder. ```cd /home/breyner/.libraries/kafka_2.13-3.1.0``` 
    2. Create the folder ```data```. Create the folder ```kafka``` and ```zookeeper``` under the previous folder.
    3. Edit Zookeeper config. In ```config/zookeeper.properties``` edit ```dataDir=/home/breyner/.libraries/kafka_2.13-3.1.0/data/zookeeper```
    4. Edit Kafka config. In ```config/server.properties``` edit ```log.dirs=/home/breyner/.libraries/kafka_2.13-3.1.0/data/kafka```


3. Start Zookeeper & Kafka. Ensure both of them are binding to a port and running.
    1. Move to the binaries root folder. ```cd /home/breyner/.libraries/kafka_2.13-3.1.0``` 
    2. Start Zookeeper ```zookeeper-server-start.sh config/zookeeper.properties```
    2. Start Kafka Broker ```kafka-server-start.sh config/server.properties```

## Useful commands

### Topics

#### List topics in a broker
```kafka-topics.sh --bootstrap-server localhost:9092 --list```

#### Create a topic
You can specify the leader broker, replicas and number of partitions.

```kafka-topics.sh --create --topic first_topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3```

#### Describe a topic
```kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --describe```

#### Delete a topic
```kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --delete```

### Producers

#### Produce messages
It will create a topic if it does not exist. Make sure the topic exists first, as it will be created with defaults like 1 partition, 1 replication factor, so on if it doesn't exist.

```kafka-console-produce --broker-list localhost:9092 --topic first_topic```

#### Producer with keys
```kafka-console-producer.sh --broker-list localhost:9092 --topic first_topic --property parse.key=true --property key.separator=,```

### Consumers

#### Consume messages
It will intercept new messages. It reads from the point you launch it

```kafka-console-consume --broker-list localhost:9092 --topic first_topic```

Add ```--from-beginning``` flag to read all the messages.

Add ```--group group_name``` to read the messages in a group.

#### Describe a group
The LAG column means how many messages haven't been read yet. There's info like the partition, offsets and consumer ID.

```kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-first-app```

#### Reset offsets
```kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-first-app --reset-offsets --to-earliest --execute --topic first_topic```

#### Consumer with keys
```kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --from-beginning --property print.key=true --property key.separator=,```

