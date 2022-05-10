package com.github.tavet.kafka.opensearch;

import java.util.concurrent.Callable;

import com.google.gson.JsonParser;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker implements Callable<Boolean> {

    Logger log = LoggerFactory.getLogger(Worker.class.getSimpleName());
    ConsumerRecord<String, String> record;

    public Worker(ConsumerRecord<String, String> record) {
        this.record = record;
    }

    static String getId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    @Override
    public Boolean call() throws Exception {
        try {
            IndexRequest indexRequest = new IndexRequest("wikimedia")
                    .source(record.value(), XContentType.JSON)
                    .id(getId(record.value())); // Idempotence
                    
            IndexResponse response = OpensearchConnection.getInstance().getClient().index(indexRequest,
                    RequestOptions.DEFAULT);
            log.info("Document inserted into opensearch. ID: " + response.getId());
            return true;
        } catch (Exception e) {
            log.error("Error inserting index request: " + e.getMessage());
            return false;
        }
    }

}
