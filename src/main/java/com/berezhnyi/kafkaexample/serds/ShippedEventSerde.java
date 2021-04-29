package com.berezhnyi.kafkaexample.serds;

import com.berezhnyi.kafkaexample.model.ShippedEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ShippedEventSerde implements Serde<ShippedEvent> {
    public void configure(Map<String, ?> map, boolean b) {

    }

    public void close() {

    }

    public Serializer<ShippedEvent> serializer() {
        return new ShippedEventSerializer();
    }

    public Deserializer<ShippedEvent> deserializer() {
        return new ShippedEventDeserializer();
    }
}