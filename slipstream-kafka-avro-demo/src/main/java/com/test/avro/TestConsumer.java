package com.test.avro;

import com.landoop.Avro;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class TestConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "tdh-24:9092");
        // 消费者的组id
        props.put("group.id", "group1a");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        // 从poll(拉)的回话处理时长
        props.put("session.timeout.ms", "30000");
        // poll的数量限制
        props.put("max.poll.records", "100");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://tdh-24:18081");

        KafkaConsumer<String, SpecificRecordBase> consumer = new KafkaConsumer<String, SpecificRecordBase>(props);
        // 订阅主题列表topic
        consumer.subscribe(Arrays.asList("wtest"));

        System.out.println("start to consume");
        while (true) {
            ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(1000);
            for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                // 正常这里应该使用线程池处理，不应该在这里处理
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(),
                        record.value() + "\n");
            }

        }
    }
}
