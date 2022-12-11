package com.kafka.java.demo.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class EventStreamingHandler implements EventHandler {

    private static final Logger log = LoggerFactory.getLogger(EventStreamingHandler.class.getClass());
    KafkaProducer<String, String> producer;
    String topic;

    public EventStreamingHandler(KafkaProducer kafkaProducer, String topic) {
        this.producer = kafkaProducer;
        this.topic = topic;

    }

    @Override
    public void onClosed() throws Exception {
        log.info("Going to close producer connection");
        producer.close();

    }

    @Override
    public void onComment(String arg0) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void onError(Throwable arg0) {
        // TODO Auto-generated method stub

        log.error("Exception occurred {}", arg0.fillInStackTrace());

    }

    @Override
    public void onMessage(String arg0, MessageEvent arg1) throws Exception {
        log.info("event message is {}", arg1.getData());
        producer.send(new ProducerRecord<String, String>(topic, arg1.getData()));

    }

    @Override
    public void onOpen() throws Exception {
        // TODO Auto-generated method stub

    }

}
