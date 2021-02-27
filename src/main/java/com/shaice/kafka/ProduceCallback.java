package com.shaice.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProduceCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            exception.printStackTrace();
        }else{
            System.out.printf("topic = %s, partition = %d offset = %d%n"
            ,metadata.topic()
            ,metadata.partition()
            ,metadata.offset());
        }
    }
    
}
