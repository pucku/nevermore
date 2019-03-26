package com.datasource;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.transwarp.slipstream.api.sink.writer.vendor.custom.CustomWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class AvroDataOutputSource extends CustomWriter {
    private static final Logger logger = LoggerFactory.getLogger(AvroDataOutputSource.class);
    private JobConf conf;
    private boolean autoFlush;
    private long flushSize;

    private KafkaProducer<String, GenericRecord> producer;
    private String topic;
    private Schema schema;
    private String[] fields;

    public AvroDataOutputSource(String jobId, String tableName, JobConf jobConf, long flushSize, long flushInterval, boolean autoFlush) {
        super(jobId, tableName, jobConf, flushSize, flushInterval, autoFlush);
        this.conf = jobConf;
        this.autoFlush = autoFlush;
        this.flushSize = flushSize;

        initKafkaProducer(jobConf);
    }

    public String getSchemaByURL(String url) {
        String targetURL = url;

        String schemaStr = null;
        try {
            URL restServiceURL = new URL(targetURL);

            HttpURLConnection httpConnection = (HttpURLConnection) restServiceURL.openConnection();
            httpConnection.setRequestMethod("GET");
            httpConnection.setRequestProperty("Accept", "application/json");

            if (httpConnection.getResponseCode() != 200) {
                throw new RuntimeException("HTTP GET Request Failed with Error code : "
                        + httpConnection.getResponseCode());
            }

            BufferedReader responseBuffer = new BufferedReader(new InputStreamReader(
                    (httpConnection.getInputStream())));

            String output;
            if ((output = responseBuffer.readLine()) != null) {
                JsonParser parser = new JsonParser();
                JsonObject msgObject = (JsonObject) parser.parse(output);
                schemaStr = msgObject.get("schema").getAsString();
                logger.warn("schema: \n" + schemaStr);
            }


            httpConnection.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return schemaStr;
    }

    public void initKafkaProducer(JobConf jobConf) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("metadata.broker.list", jobConf.get("brokerlist"));
        kafkaProperties.put("bootstrap.servers", jobConf.get("brokerlist"));

        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kafkaProperties.put("schema.registry.url", jobConf.get("registryurl"));

        kafkaProperties.put("request.required.ack", "1");
        kafkaProperties.put("batch.size", "1024");

        producer = new KafkaProducer<>(kafkaProperties);
        this.topic = jobConf.get("topic");

        fields = jobConf.get("fields").split(",");

        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(getSchemaByURL(jobConf.get("schema")));
    }

    public void sendMsg(GenericRecord msg) {
        producer.send(new ProducerRecord<String, GenericRecord>(topic, msg));
    }

    public void testProduce(String row) {
        StringBuilder sb = new StringBuilder();
        if (row instanceof String) {
            String[] columns = row.toString().split(",");
            GenericRecord record = new GenericData.Record(schema);

            for (int i = 0; i < columns.length; ++i) {
                record.put(fields[i], columns[i]);
            }
            sendMsg(record);
        }
    }

    @Override
    public void write(Writable w) {
        if (w instanceof Text) {
            String[] columns = w.toString().split("\001");
            GenericRecord record = new GenericData.Record(schema);

            /***
             * 因为这里columns[i]为string,而为了能够for统一put，
             * 所以使用的schema里面所有的字段都需要统一为string类型
             */
            for (int i = 0; i < columns.length; ++i) {
                record.put(fields[i], columns[i]);
            }
            sendMsg(record);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void close(boolean abort) {

    }

    @Override
    public void flush() {

    }

    @Override
    public boolean shouldFlush() {
        return (autoFlush || (cachedRecordSize().get() >= flushSize));
    }

    public static void main(String[] args) throws Exception {
        JobConf jobconf = new JobConf();
        jobconf.set("brokerlist", "tdh-24:9092");
        jobconf.set("registryurl", "http://tdh-24:18081");
        jobconf.set("topic", "wtest_out");
        jobconf.set("fields", "id,letter");
        jobconf.set("schema", "http://tdh-24:18081/subjects/wtest_out-value/versions/1");
        AvroDataOutputSource t = new AvroDataOutputSource("1", "a", jobconf, 1024, 1024, true);

        int i = 0;
        while (true) {
            t.testProduce(String.valueOf(++i) + ",b");
        }
    }
}
