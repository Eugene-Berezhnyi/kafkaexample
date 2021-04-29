package com.berezhnyi.kafkaexample.serds;

import com.berezhnyi.kafkaexample.model.OrderCreatedEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class OrderCreatedEventSerde implements Serde<OrderCreatedEvent> {
    public void configure(Map<String, ?> map, boolean b) {

    }

    public void close() {

    }

    public Serializer<OrderCreatedEvent> serializer() {
        return new OrderCreatedEventSerializer();
    }

    public Deserializer<OrderCreatedEvent> deserializer() {
        return new OrderCreatedEventDeserializer();
    }
}