package com.github.tavet.kafka.opensearch;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Callable<Boolean> {

    Logger log = LoggerFactory.getLogger(Worker.class.getSimpleName());
    ConsumerRecord<String, String> record;
    RestHighLevelClient client;

    public Worker(ConsumerRecord<String, String> record, RestHighLevelClient client) {
        this.record = record;
        this.client = client;
    }

    @Override
    public Boolean call() throws Exception {
        try {
            log.info("Record: " + record.key());
            IndexRequest indexRequest = new IndexRequest("wikimedia")
                    .source(record.value(), XContentType.JSON);
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            Thread.sleep(10000);
            log.info("Document inserted into opensearch. ID: " + response.getId());
            return true;
        } catch (IOException e) {
            log.error("Error inserting index request: " + e.getMessage());
            return false;
        }
    }

}
