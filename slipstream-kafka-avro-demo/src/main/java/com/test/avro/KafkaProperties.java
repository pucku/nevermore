package com.test.avro;

import java.util.Properties;

public class KafkaProperties {
    private Properties properties = new Properties();

    /**
     * 构造函数
     */
    public KafkaProperties() {

        properties.put("metadata.broker.list", Constant.METADATA_BROKER_LIST);
        properties.put("bootstrap.servers", Constant.METADATA_BROKER_LIST);

        properties.put("key.serializer", Constant.KEY_SERIALIZER_CLASS);
        properties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put("schema.registry.url","http://tdh-24:18081");

        properties.put("request.required.ack", Constant.REQUEST_REQUIRED_ACKS);
        properties.put("batch.size", Constant.BATCH_NUM);

    }

    public Properties properties() {
        return properties;
    }
}
