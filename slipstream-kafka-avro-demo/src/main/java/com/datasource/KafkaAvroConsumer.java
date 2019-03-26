package com.datasource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaAvroConsumer<K, V> extends Thread {
    private static Logger logger = LoggerFactory.getLogger(KafkaAvroConsumer.class);
    private KafkaConsumer<K, V> consumer;
    private LinkedBlockingQueue<String> messageBlockingQueue = new LinkedBlockingQueue<>();

    public KafkaAvroConsumer(Properties kafkaProperty, String topic) {
        try {
            consumer = new KafkaConsumer<K, V>(kafkaProperty);
            consumer.subscribe(Arrays.asList(topic));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        startConsume();
    }

    private void startConsume() {
        logger.warn("start to consume");

        while (true) {
            ConsumerRecords<K, V> records = consumer.poll(1000);
            for (ConsumerRecord<K, V> record : records) {
                logger.info("offset=" + record.offset() + " , key=" + record.key() + " , value=" + record.value());
                try {
                    messageBlockingQueue.put(record.value().toString());
                    logger.info("messageBlockingQueue put: " + record.value().toString());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public String getMessage() {
        String message = null;
        try {
            message = messageBlockingQueue.poll(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return message;
    }
}
