package com.berezhnyi.kafkaexample.serds;

import com.berezhnyi.kafkaexample.model.InvoiceEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class InvoiceEventSerializer implements Serializer<InvoiceEvent> {
    private ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> map, boolean b) {

    }

    public byte[] serialize(String s, InvoiceEvent adViewEvent) {
        try {
            return mapper.writeValueAsBytes(adViewEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}
