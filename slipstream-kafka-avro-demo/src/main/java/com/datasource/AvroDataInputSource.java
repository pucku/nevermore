package com.datasource;

import io.transwarp.slipstream.api.source.CustomDataSource;
import org.apache.avro.generic.GenericRecord;

import java.util.Properties;

public class AvroDataInputSource extends CustomDataSource {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(AvroDataInputSource.class);

    private Properties kafkaProperty = new Properties();
    private String topic = null;

    private KafkaAvroConsumer<String, GenericRecord> consumer = null;

    public void initKafkaParams(Properties params) {

        kafkaProperty.put("bootstrap.servers", params.getProperty("brokerlist"));
        kafkaProperty.put("group.id", params.getProperty("groupid"));
        kafkaProperty.put("enable.auto.commit", "true");
        kafkaProperty.put("auto.commit.interval.ms", "1000");

        kafkaProperty.put("session.timeout.ms", "30000");

        kafkaProperty.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperty.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        kafkaProperty.put("schema.registry.url", params.getProperty("registryurl"));

        topic = params.getProperty("topic");
    }

    public void init(int id, Properties params) {
        initKafkaParams(params);
        consumer = new KafkaAvroConsumer<>(kafkaProperty, topic);
        consumer.start();
    }

    public byte[] poll() {
        byte[] data = null;
        try {
            String msg = consumer.getMessage();
            if (msg == null) {
                logger.info("messageBlockingQueue poll: null");
            } else {
                logger.info("messageBlockingQueue poll:" + msg.toString());
                data = msg.getBytes();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return data;
    }

    public void close() {

    }

    public static void main(String[] args) throws Exception {
        AvroDataInputSource t = new AvroDataInputSource();
        Properties p = new Properties();
        p.put("brokerlist", "tdh-24:9092");
        p.put("groupid", "group1a");
        p.put("registryurl", "http://tdh-24:18081");
        p.put("topic", "wtest_out");
        t.init(1, p);

        for (int i = 0; i < 100; ++i) {
            Thread.sleep(10 * 1000);
            t.poll();
        }

    }
}
