package com.github.tavet.kafka.opensearch;

import java.io.IOException;
import java.net.URI;
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

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {
    final static String indiceName = "wikimedia";

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

        RestHighLevelClient client = createOpensearchClient();

        // Create index if not exists
        try (client) {
            boolean indexExists = client.indices().exists(new GetIndexRequest(indiceName), RequestOptions.DEFAULT);

            if (indexExists) {
                log.info("Wikimedia index already exists");
            } else {
                CreateIndexRequest indexRequest = new CreateIndexRequest(indiceName);
                client.indices().create(indexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia info has been created.");
            }
        }

        // Create consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // Subscribe to the topic specified in "wikimedia" project
        consumer.subscribe(Collections.singleton("wikimedia.recentchange"));
        final int poolSize = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
        CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executorService);

        try {

            while (true) {
                ConsumerRecords<String, String> consumedRecords = consumer.poll(Duration.ofMillis(1000));
                List<ConsumerRecord<String, String>> records = new ArrayList<>();

                // Async poll and commit
                for (ConsumerRecord<String, String> record : consumedRecords) {
                    records.add(record);
                    if (records.size() == poolSize) {
                        int taskCount = poolSize;
                        records.forEach(consumerRecord -> completionService.submit(new Worker(consumerRecord, client)));

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
                // StreamSupport.stream(Spliterators.spliteratorUnknownSize(record.iterator(),
                // Spliterator.ORDERED), true)
                // .forEach(r -> onConsumed(r, log, client));
            }
        } finally {
            consumer.close();
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

    private static void onConsumed(ConsumerRecord<String, String> record, Logger log, RestHighLevelClient client) {
        log.info("Record: " + record.key());
        IndexRequest indexRequest = new IndexRequest(indiceName)
                .source(record.value(), XContentType.JSON);
        try {
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            log.info("Document inserted into opensearch. ID: " + response.getId());
        } catch (IOException e) {
            log.error("Error insertir index request: " + e.getMessage());
        }
    }

    public static RestHighLevelClient createOpensearchClient() {
        final String opensearch = "http://localhost:9200";
        RestHighLevelClient client;
        URI opensearchUri = URI.create(opensearch);
        String userInfo = opensearchUri.getUserInfo();

        if (userInfo == null) {
            client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(opensearchUri.getHost(), opensearchUri.getPort(), "http")));
        } else {
            String[] auth = userInfo.split(":");
            CredentialsProvider provider = new BasicCredentialsProvider();
            provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            client = new RestHighLevelClient(RestClient
                    .builder(new HttpHost(opensearchUri.getHost(), opensearchUri.getPort(), opensearchUri.getScheme()))
                    .setHttpClientConfigCallback(
                            asyncBuilder -> asyncBuilder.setDefaultCredentialsProvider(provider)
                                    .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return client;
    }
}
