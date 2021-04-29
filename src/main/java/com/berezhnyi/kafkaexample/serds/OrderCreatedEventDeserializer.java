package com.berezhnyi.kafkaexample.serds;

import com.berezhnyi.kafkaexample.model.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class OrderCreatedEventDeserializer implements Deserializer<OrderCreatedEvent> {
    private ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> map, boolean b) {

    }

    public OrderCreatedEvent deserialize(String s, byte[] bytes) {

        try {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            return mapper.readValue(bytes, OrderCreatedEvent.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}
