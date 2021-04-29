package com.berezhnyi.kafkaexample.serds;

import com.berezhnyi.kafkaexample.model.ShippedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ShippedEventSerializer implements Serializer<ShippedEvent> {
    private ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> map, boolean b) {

    }

    public byte[] serialize(String s, ShippedEvent adViewEvent) {
        try {
            return mapper.writeValueAsBytes(adViewEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}
