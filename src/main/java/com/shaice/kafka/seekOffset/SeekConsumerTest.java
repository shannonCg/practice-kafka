package com.shaice.kafka.seekOffset;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SeekConsumerTest {

    public static void main(String[] args) {
        
        Properties props = initConfig();
        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));

        Set<TopicPartition> assignment = new HashSet<>();
        while(assignment.size() == 0){
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }

        /* seek to specific offset */
        // for(TopicPartition tp: assignment){
        //     consumer.seek(tp, 130);
        // }

        /* seek from begin/end offset */
        // Map<TopicPartition, Long> offsets = consumer.beginningOffsets(assignment);
        // // Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        // for(TopicPartition tp: assignment){
        //     consumer.seek(tp, offsets.get(tp)+50);
        //     // consumer.seek(tp, offsets.get(tp)-2);
        // }

        /* seek from time range */
        Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
        for(TopicPartition tp: assignment){
            timestampToSearch.put(tp, System.currentTimeMillis()-1*1*60*1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);
        for(TopicPartition tp: assignment){
            OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
            if(Objects.nonNull(offsetAndTimestamp)){
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        }

        boolean flag = true;
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
        consumer.close();
    }
    public static Properties initConfig() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.211.55.19:9092");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "project1");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        return props;
    }
}