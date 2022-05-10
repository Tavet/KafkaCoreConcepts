package com.github.tavet.kafka.opensearch;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {
    final static String indiceName = "wikimedia";

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

        RestHighLevelClient client = OpensearchConnection.getInstance().getClient();

        // Create consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // Create index if not exists
        try (client; consumer) {
            boolean indexExists = client.indices().exists(new GetIndexRequest(indiceName), RequestOptions.DEFAULT);

            if (indexExists) {
                log.info("Wikimedia index already exists");
            } else {
                CreateIndexRequest indexRequest = new CreateIndexRequest(indiceName);
                client.indices().create(indexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia info has been created.");
            }

            // Subscribe to the topic specified in "wikimedia" project
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            final int poolSize = 10;
            ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
            CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executorService);

            while (true) {
                ConsumerRecords<String, String> consumedRecords = consumer.poll(Duration.ofMillis(1000));
                List<ConsumerRecord<String, String>> records = new ArrayList<>();

                // Async poll and commit
                for (ConsumerRecord<String, String> record : consumedRecords) {
                    records.add(record);
                    if (records.size() == poolSize) {
                        int taskCount = poolSize;
                        records.forEach(consumerRecord -> completionService.submit(new Worker(consumerRecord)));

                        while (taskCount > 0) {
                            try {
                                Future<Boolean> futureResult = completionService.poll(1, TimeUnit.SECONDS);
                                if (futureResult != null) {
                                    log.info("Result: " + futureResult.get().booleanValue());
                                    taskCount--;
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        records.clear();
                        Map<TopicPartition, OffsetAndMetadata> commitOffset = Collections.singletonMap(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));
                        consumer.commitSync(commitOffset);
                    }
                }
            }
        }
    }

    static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "wikimedia-opensearch-group";
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getProperties(groupId));
        return consumer;
    }

    static Properties getProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 31000);

        return properties;
    }
}
