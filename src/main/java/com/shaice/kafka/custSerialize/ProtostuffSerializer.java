package com.shaice.kafka.custSerialize;

import java.util.Objects;

import org.apache.kafka.common.serialization.Serializer;

public class ProtostuffSerializer implements Serializer<Company>{

    @Override
    public byte[] serialize(String topic, Company data) {
        if(Objects.isNull(data)){
            return null;
        }

        return ProtostuffUtils.serialize(data);
    }
    
}
