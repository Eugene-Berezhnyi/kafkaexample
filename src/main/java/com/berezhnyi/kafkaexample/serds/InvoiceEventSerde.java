package com.berezhnyi.kafkaexample.serds;

import com.berezhnyi.kafkaexample.model.InvoiceEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class InvoiceEventSerde implements Serde<InvoiceEvent> {
    public void configure(Map<String, ?> map, boolean b) {

    }

    public void close() {

    }

    public Serializer<InvoiceEvent> serializer() {
        return new InvoiceEventSerializer();
    }

    public Deserializer<InvoiceEvent> deserializer() {
        return new InvoiceEventDeserializer();
    }
}