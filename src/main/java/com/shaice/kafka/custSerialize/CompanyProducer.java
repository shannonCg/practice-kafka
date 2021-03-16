package com.shaice.kafka.custSerialize;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class CompanyProducer {
    public static void main(String[] args) {
        Properties props = initConfig();
        Producer<String, Company> producer = new KafkaProducer<>(props);
        try {
            for (int i = 0; i < 10; i++){
                Company company = new Company("name"+i, "address");
                ProducerRecord<String, Company> record = new ProducerRecord<>("test", getKey(i), company);
                producer.send(record); //當有broker停止服務，則consumer仍會繼續取得訊息
                System.out.println("send message:"+company);
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            System.out.println("cause exception:"+e);
        }finally{
            producer.close();
        }
        
        System.out.println("finish produce message !");
    }
    
    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.19:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtostuffSerializer.class.getName());
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return props;
    }

    public static String getKey(int num){
        return String.valueOf(num%3);
    }
}
