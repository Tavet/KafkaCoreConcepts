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