package com.test.avro;

import com.landoop.Avro;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.util.Random;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

public class Producer extends Thread {

    private KafkaProducer<String, Avro> producer;
    private final String topic;
    private static String names[] = {"a", "b", "c", "d", "e", "f"};
    private static LinkedBlockingQueue<String> fileList;

    public Producer(String topic, LinkedBlockingQueue<String> fileList) {
        KafkaProperties kafkaProperties = new KafkaProperties();
        producer = new KafkaProducer<String, Avro>(kafkaProperties.properties());
        this.topic = topic;
        this.fileList = fileList;
    }

    public String getTimeStamp() {
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return dateFormat.format(date);
    }

    private String getSendData() {
        Random random = new Random();
        String score = String.valueOf(random.nextInt(100) - 50);
        String id = names[random.nextInt(names.length)];
        String timeStamp = getTimeStamp();
        String sendTime = String.valueOf(System.currentTimeMillis());
        String messageStrTopic = score + "," + id + "," + timeStamp + "," + sendTime;
        messageStrTopic = sendTime;

        return messageStrTopic;
    }

//    private void sendFileData() {
//        try {
//            while (!fileList.isEmpty()) {
//                String fileName = fileList.poll();
//                if (StringUtils.isEmpty(fileName))
//                    return;
//
//                FileInputStream fileInputStream = new FileInputStream(fileName);
//                BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream, "UTF-8"));
//
//                System.out.println("File: " + fileName + " is start reading");
//
//                String strLine;
//                while ((strLine = br.readLine()) != null) {
//                    producer.send(new ProducerRecord<String, String>(topic, strLine));
//                    producer.flush();
//                }
//
//                br.close();
//                System.out.println("File: " + fileName + " is over");
//            }
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//            producer.close();
//        }
//    }

    private void sendRandomData() {
        Integer sendNum = Integer.valueOf(Constant.SEND_NUM);

        if (sendNum == -1)
            sendNum = Integer.MAX_VALUE;

        int total = 0;
        Avro customer = new Avro();
        customer.setId(1);
        customer.setLetter("a");

        while (total < sendNum) {

            String messageStrTopic = getSendData();

            System.out.println(topic + ":" + messageStrTopic);

            producer.send(new ProducerRecord<String, Avro>(topic, customer));

            producer.flush();

            try {
                Thread.sleep(Integer.valueOf(Constant.SEND_INTERVAL));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            total++;
        }
    }

    @Override
    public void run() {
        sendRandomData();
//		sendFileData();
    }
}
