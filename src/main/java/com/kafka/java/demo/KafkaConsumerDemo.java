package com.kafka.java.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class.getName());

    public static void main(String[] args) {
        // create kafka consumer configs

        final String topic_name = "java_demo";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "first-consumer");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create kafka consumer config

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // consume message from topic
        consumer.subscribe(Arrays.asList(topic_name));

        // adding shutdownhook

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutdown hook called");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        try {
            while (true) {
                log.info("pooling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord record : records) {

                    log.info("Key value is {} : and message is {} :  offset value is {} : partition value is {}",
                            record.key(), record.value(), record.offset(), record.partition());

                }

            }
        } catch (WakeupException wakeupException) {
            log.info("Wakeup exception expected ");
        } catch (Exception e) {
            log.error("unexpected exception ");
        } finally {
            consumer.close();
        }

    }
}
