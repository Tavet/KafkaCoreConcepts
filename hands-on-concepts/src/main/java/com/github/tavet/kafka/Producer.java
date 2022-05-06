package com.github.tavet.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private static AtomicInteger messageId = new AtomicInteger(0);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // List of properties https://kafka.apache.org/documentation/#producerconfigs
        // Producer properties
        KafkaProducer<String, String> producer = new KafkaProducer<>(getProperties());

        // async
        for (int i = 0; i < 100; i++) {
            ProducerRecord record = generateRecord("Test message");
            logger.info("Key: ", record.key());
            producer.send(record, (metadata, exception) -> onProduced(metadata, exception))
                    .get();
        }

        producer.flush();
        producer.close();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static ProducerRecord<String, String> generateRecord(String message) {
        final String key = "id_" + messageId.getAndIncrement();
        return new ProducerRecord<>("first_topic", key, message);
    }

    private static void onProduced(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            logger.info("Metadata: " + " | Partition: " + metadata.partition());
        } else {
            logger.error("Error: ", exception);
        }
    }
}
