/**
 * Created by wjw on 14-10-19.
 */
package com.wjw.kafka.test;

import kafka.javaapi.producer.*;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import sun.jdbc.odbc.ee.PoolWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class ProducerTest {

    public static long msgId = 0;

    public static String getID() {

        Random rand = new Random(System.currentTimeMillis());
		//if(msgId < 100) {
            int year = rand.nextInt(2000);
            if (year < 1985) {
                int delta = 1985 - year;
                year += (delta + rand.nextInt() % 9);
            }
            int month = rand.nextInt(12);
            String month_str = Integer.toString(month);
            if (month < 10) {
                month_str = ("0" + Integer.toString(month));
            }
            int day = rand.nextInt(28);
            String day_str = Integer.toString(day);
            if (day < 10) {
                day_str = ("0" + Integer.toString(day));
            }
            int flag = rand.nextInt(10);
            int checkCode = 0;
            if (msgId % 5 == 0) {
                checkCode = (year + month + day + flag) % 10;
                if (checkCode < 9) {
                    checkCode += 1;
                } else {
                    checkCode -= 1;
                }
            } else {
                checkCode = (year + month + day + flag) % 10;
            }
            String id = Integer.toString(year) +
                    month_str +
                    (day_str) +
                    Integer.toString(flag) +
                    Integer.toString(checkCode);
            String gender = null;
            if (msgId % 5 == 0) {
                if (flag % 2 == 0) {
                    gender = "M";
                } else {
                    gender = "F";
                }
            } else {
                if (flag % 2 == 0) {
                    gender = "F";
                } else {
                    gender = "M";
                }
            }
            String age = null;
            if (msgId % 3 == 0) {
                age = Integer.toString(2014 - year + 1);
            } else {
                age = Integer.toString(2014 - year);
            }
            String idInfo = id + gender + age;
            msgId++;
            return idInfo;



    }

    public static void sendMsg(String msg) {
        Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","127.0.0.1:9092");
//        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
//        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);
        Random rnd = new Random(System.currentTimeMillis());
        String ip = "192.168.2." + rnd.nextInt(255);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
        producer.send(data);

    }

    public static void sendSentence( ) {
        //long events = Long.parseLong(args[0]);
        long start = System.currentTimeMillis();
        Random rnd = new Random();


        String[] sentences = {
                "the cow jumped over the moon",
                "an apple a day keeps the doctor away",
                "four score and seven years ago",
                "snow white and the seven dwarfs",
                "i am at two with nature"
        };
//        for (long nEvents = 0; nEvents < 10; nEvents++) {
//            long runtime = new Date().getTime();
//            String ip = "192.168.2." + rnd.nextInt(255);
//            String msg = runtime + ",www.example.com," + ip;
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//            producer.send(data);
//        }
//        producer.close();
        int length = sentences.length;
        int i = 300000;
        while(true) {

            long finish = System.currentTimeMillis();
            long delta = finish -start;

            if(delta < (60 * 60 * 1000)) {
                sleep(1000L);

            } else if (delta >= (60 * 60 * 1000) && delta < (120 * 60 * 1000)) {
                sleep(100L);
                //} else if(delta >= ( 180* 60 * 1000) && delta < (240 * 60 * 1000)) {
                //  sleep(5L);
            } else {
                start = System.currentTimeMillis();
            }
            Random random = new Random();
            String msg = sentences[random.nextInt(length)];
            sendMsg(msg);
            //long runtime = new Date().getTime();



           // i--;
        }
        //producer.close();
    }
    public  static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    static class MyRunnable implements Runnable {

        @Override
        public void run() {
            sendSentence();
        }
    }
    public static void main(String[] args) {
//        List<Thread> threads = new ArrayList<Thread>();
//        for(int i = 0; i < 2; i++) {
//            threads.add(new Thread(new ProducerTest.MyRunnable()));
//        }
//        for(int i = 0; i < 2; i++) {
//            threads.get(i).start();
//            sleep(2000);
//        }
       // sendSentence();
        for(int i = 0; i < 100; i++) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            sendMsg(getID());
        }
    }

}

