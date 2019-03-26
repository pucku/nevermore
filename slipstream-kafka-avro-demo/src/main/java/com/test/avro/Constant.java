package com.test.avro;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class Constant {
    //	private static Configuration configuration = new Configuration();
    private static Properties configuration = new Properties();
    // 定义常量
    public static String METADATA_BROKER_LIST;
    public static String ZOOKEEPER_CONNECT_LIST;
    public static String SERIALIZER_CLASS;
    public static String KEY_SERIALIZER_CLASS;
    public static String TOPIC_NAME;
    public static String SEND_INTERVAL;
    public static String BATCH_NUM;
    public static String REQUEST_REQUIRED_ACKS;
    public static String SEND_NUM;
    public static String FILE_FOLDERS;
    public static String THREAD_NUM;
    public static String PARTITIONER_CLASS;
    public static String GENERATOR_CLASS;
    public static String PARTITIONER_COLUMN;

    // 加载配置文件
    static {
        try {

            String confFile = System.getProperty("user.dir") + File.separator + "producer.config";
            configuration.load(new BufferedInputStream(new FileInputStream(confFile)));

            METADATA_BROKER_LIST = configuration.getProperty("metadata.broker.list");
            ZOOKEEPER_CONNECT_LIST = configuration.getProperty("zookeeper.connect.list");
            SERIALIZER_CLASS = configuration.getProperty("serializer.class");
            KEY_SERIALIZER_CLASS = configuration.getProperty("key.serializer.class");
            TOPIC_NAME = configuration.getProperty("topic.name");
            SEND_INTERVAL = configuration.getProperty("send.interval");
            BATCH_NUM = configuration.getProperty("batch.size");
            REQUEST_REQUIRED_ACKS = configuration.getProperty("acks");
            SEND_NUM = configuration.getProperty("send.num");
            FILE_FOLDERS = configuration.getProperty("file.folders");
            THREAD_NUM = configuration.getProperty("thread.num");
            PARTITIONER_CLASS = configuration.getProperty("partitioner.class");
            GENERATOR_CLASS = configuration.getProperty("generator.class");
            PARTITIONER_COLUMN = configuration.getProperty("partitioner.column");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Constant() {

    }
}
