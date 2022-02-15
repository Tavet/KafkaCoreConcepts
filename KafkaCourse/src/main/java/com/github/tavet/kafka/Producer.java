package com.github.tavet.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(Producer.class);

        // List of properties https://kafka.apache.org/documentation/#producerconfigs
        // Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // async
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("first_topic", "test from Java " + i), (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Metadata: " + metadata.timestamp());
                } else {
                    logger.error("Error: ", exception);
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
