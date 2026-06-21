package com.example.realtimelab.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class JsonSerializationSchema<T> implements SerializationSchema<T> {
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = ObjectMapperFactory.create();
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (Exception e) {
            throw new IllegalStateException("Could not serialize event to JSON", e);
        }
    }
}
