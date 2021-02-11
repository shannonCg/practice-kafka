package com.shaice.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerTest {

    public static void main(String[] args) {
        
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "10.211.55.19:9092");
    props.setProperty("group.id", "test_consumer4");
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.commit.interval.ms", "1000");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    Consumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("test"));
    
    boolean flag = true;
    while (flag) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records){
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
    consumer.close();
}
}