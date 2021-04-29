package com.berezhnyi.kafkaexample.serds;

import com.berezhnyi.kafkaexample.model.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OrderCreatedEventSerializer implements Serializer<OrderCreatedEvent> {

    private ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> map, boolean b) {

    }

    public byte[] serialize(String s, OrderCreatedEvent adViewEvent) {
        try {
            return mapper.writeValueAsBytes(adViewEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}

