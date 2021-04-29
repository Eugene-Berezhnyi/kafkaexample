package com.berezhnyi.kafkaexample.serds;

import com.berezhnyi.kafkaexample.model.ResultingEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ResultingEventSerde implements Serde<ResultingEvent> {
    public void configure(Map<String, ?> map, boolean b) {

    }

    public void close() {

    }

    public Serializer<ResultingEvent> serializer() {
        return new ResultingEventSerializer();
    }

    public Deserializer<ResultingEvent> deserializer() {
        return new ResultingEventDeserializer();
    }
}