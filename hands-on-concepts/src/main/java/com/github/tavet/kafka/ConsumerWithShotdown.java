package com.github.tavet.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class ConsumerWithShotdown {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithShotdown.class);

    public static void main(String[] args) {
        String groupId = "test-group-id-spring-two";
        String topic = "first_topic";

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties(groupId));
 
        // get reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // add the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                logger.info("Shotdown detected");
                consumer.wakeup(); // the next time we try to execute consumer.poll it will throw an exception

                // join the main thread to allow the execution there
                try {
                     mainThread.join();
                } catch(InterruptedException e) {

                }
            }
        });

        // subscribe
        try {

            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(records.iterator(), Spliterator.ORDERED), true)
                .forEach(record -> onConsumed(record));
            }
        } catch(WakeupException e) {
            logger.info("Wake up exception");
        } catch(Exception e) {
            
        } finally {
            consumer.close(); // commit the offsets just in case
        }
    }

    private static Properties getProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private static void onConsumed(ConsumerRecord<String, String> record) {
        logger.info("Record: " + record.key() + " | " + record.value() + " | Partition: "
                + record.partition() + " | Offset: " + record.offset());
    }
}
