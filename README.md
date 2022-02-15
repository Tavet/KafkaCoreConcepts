# Kafka Core Concepts
This is a project where I am practicing the basics of Kafka.

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

### Kafka Topics

#### List topics in a broker
```kafka-topics.sh --bootstrap-server localhost:9092 --list```

#### Create a topic
You can specify the leader broker, replicas and number of partitions.

```kafka-topics.sh --create --topic first_topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4```

#### Describe a topic
```kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --describe```

#### Delete a topic
```kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --delete```