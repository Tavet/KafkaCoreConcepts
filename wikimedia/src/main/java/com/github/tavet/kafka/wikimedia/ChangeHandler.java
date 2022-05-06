package com.github.tavet.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeHandler implements EventHandler {

    private final Logger log = LoggerFactory.getLogger(ChangeHandler.class.getSimpleName());
    final String topic;
    KafkaProducer<String, String> kafkaProducer;

    public ChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void onOpen() throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        // Async
        kafkaProducer.send(new ProducerRecord<String, String>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in Stream: ", t);
    }

}
