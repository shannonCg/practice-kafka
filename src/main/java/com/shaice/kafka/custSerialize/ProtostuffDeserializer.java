package com.shaice.kafka.custSerialize;

import java.util.Objects;

import org.apache.kafka.common.serialization.Deserializer;

public class ProtostuffDeserializer implements Deserializer<Company>{

    @Override
    public Company deserialize(String topic, byte[] data) {
        if(Objects.isNull(data)){
            return null;
        }

        return ProtostuffUtils.deserialize(data, Company.class);
    }
    
}
