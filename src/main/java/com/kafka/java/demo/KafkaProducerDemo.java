package com.kafka.java.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        // create kafka producer configuration
        log.info("producer application started");

        final String topic_name = "java_demo";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        // create kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {

            // create producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic_name,
                    "John Doe, Hello world" + i);

            // send message
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {

                    if (e == null) {
                        log.info("=================topic name {} and partition {} and offset {}", topic_name,
                                metadata.partition(),
                                metadata.offset());

                    } else {
                        log.error("Exception occurred while producing the message into kafka");
                    }

                }
            });

            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

            log.info("successfully published the message into kafka topic");
        }
        producer.close();

    }
}
