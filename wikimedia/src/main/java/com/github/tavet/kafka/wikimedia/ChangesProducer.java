package com.github.tavet.kafka.wikimedia;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class ChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        final String bootstrapServers = "127.0.0.1:9092";
        final String topic = "wikimedia.recentchange";
        final String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // High throughput
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // 20 ms
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 kb
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        EventHandler eventHandler = new ChangeHandler(producer, topic);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        // Start the producer in different thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
