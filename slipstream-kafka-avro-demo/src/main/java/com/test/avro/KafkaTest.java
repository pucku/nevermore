package com.test.avro;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaTest {
    public static void main(String[] args) {

        String[] folders = Constant.FILE_FOLDERS.split(";");
        LinkedBlockingQueue<String> fileList = FileUtil.addFiles(folders);
        int threadNum = Integer.valueOf(Constant.THREAD_NUM);

        ExecutorService es = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; ++i) {
            Producer producer = new Producer(Constant.TOPIC_NAME, fileList);
            es.execute(producer);
        }
    }

}
