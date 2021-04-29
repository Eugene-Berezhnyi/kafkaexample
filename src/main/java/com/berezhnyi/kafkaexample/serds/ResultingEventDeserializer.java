package com.berezhnyi.kafkaexample.serds;

import com.berezhnyi.kafkaexample.model.ResultingEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class ResultingEventDeserializer implements Deserializer<ResultingEvent> {
    private ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> map, boolean b) {

    }

    public ResultingEvent deserialize(String s, byte[] bytes) {

        try {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            return mapper.readValue(bytes, ResultingEvent.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}