package com.shaice.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.211.55.19:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            for (int i = 0; i < 10; i++)
                producer.send(new ProducerRecord<String, String>("test", "+"+Integer.toString(i)));
        } catch (Exception e) {
            System.out.println("cause exception:"+e);
        }finally{
            producer.close();
        }

        System.out.println("finish produce message !");
    }
}