package com.berezhnyi.kafkaexample;

import com.berezhnyi.kafkaexample.aggregator.InvoiceAggregator;
import com.berezhnyi.kafkaexample.aggregator.OrderCreatedAggregator;
import com.berezhnyi.kafkaexample.aggregator.ShippedAggregator;
import com.berezhnyi.kafkaexample.model.InvoiceEvent;
import com.berezhnyi.kafkaexample.model.OrderCreatedEvent;
import com.berezhnyi.kafkaexample.model.ResultingEvent;
import com.berezhnyi.kafkaexample.model.ShippedEvent;
import com.berezhnyi.kafkaexample.serds.DagSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;

@Component
public class WithSessionWindow implements CommandLineRunner {

    final private String appId;
    final private int createdPartition;
    final private int shippedPartition;
    final private int invoicePartition;
    final private String createdTopic;
    final private String shippedTopic;
    final private String invoiceTopic;
    final private String outputTopic;

    final Producer<String, OrderCreatedEvent> orderCreatedEventProducer;
    final Producer<String, ShippedEvent> shippedEventProducer;
    final Producer<String, InvoiceEvent> invoiceEventProducer;

    public WithSessionWindow() {
        this.appId = "OuterJoin" + (int) (Math.random() * 1000);
        this.createdPartition = 0;
        this.shippedPartition = 0;
        this.invoicePartition = 0;
        this.createdTopic = "created" + (int) (Math.random() * 1000);
        this.shippedTopic = "shipped" + (int) (Math.random() * 1000);
        this.invoiceTopic = "invoice" + (int) (Math.random() * 1000);
        this.outputTopic = "outputTopic" + (int) (Math.random() * 1000);
        String bootstrapServers = "localhost:9092";

        Properties createdProps = new Properties();
        createdProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        createdProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        createdProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.berezhnyi.kafkaexample.serds.OrderCreatedEventSerializer");

        Properties shippedProps = new Properties();
        shippedProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        shippedProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        shippedProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.berezhnyi.kafkaexample.serds.ShippedEventSerializer");

        Properties invoiceProps = new Properties();
        invoiceProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        invoiceProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        invoiceProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.berezhnyi.kafkaexample.serds.InvoiceEventSerializer");

        orderCreatedEventProducer = new KafkaProducer<>(createdProps);
        shippedEventProducer = new KafkaProducer<>(shippedProps);
        invoiceEventProducer = new KafkaProducer<>(invoiceProps);

    }

    @Override
    public void run(String... args) {
        sendEvents();

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

//        Business Logic Start

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, OrderCreatedEvent> firstSource = builder.stream(createdTopic, Consumed.with(Serdes.String(), DagSerde.CREATE_SERDE));
        KStream<String, ShippedEvent> secondSource = builder.stream(shippedTopic, Consumed.with(Serdes.String(), DagSerde.SHIPPED_SERDE));
        KStream<String, InvoiceEvent> thirdSource = builder.stream(invoiceTopic, Consumed.with(Serdes.String(), DagSerde.INVOICE_SERDE));

        final KGroupedStream<String, OrderCreatedEvent> orderCreatedGrouped = firstSource.groupByKey();
        final KGroupedStream<String, ShippedEvent> shippedGrouped = secondSource.groupByKey();
        final KGroupedStream<String, InvoiceEvent> invoiceGrouped = thirdSource.groupByKey();

        orderCreatedGrouped.cogroup(new OrderCreatedAggregator())
                .cogroup(shippedGrouped, new ShippedAggregator())
                .cogroup(invoiceGrouped, new InvoiceAggregator())
                .windowedBy(SessionWindows.with(Duration.ofMillis(60000)))
                .aggregate(ResultingEvent::new, new StringResultingEventMerger(), Materialized.with(Serdes.String(), DagSerde.RESULTING_SERDE))
                .toStream()
                .foreach((k, v) -> System.out.println("OUTPUT " + k + " |->| " + v));

//        Business Logic End

        final Topology topology = builder.build();
        System.out.println(topology);
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }

    private void sendEvents() {
        sendOrderCreatedEvent(0, "0 field1", 0);
        sendOrderCreatedEvent(1, "1 field1", 0);
        sendOrderCreatedEvent(2, "2 field1", 0);
        sendOrderCreatedEvent(3, "3 field1", 0);
        sendOrderCreatedEvent(4, "4 field1", 0);
        sendOrderCreatedEvent(5, "5 field1", 0);
        sendOrderCreatedEvent(6, "6 field1", 0);
        sendOrderCreatedEvent(7, "7 field1", 0);

        sendOrderShipped(0, "0 field2", 10000);
        sendOrderShipped(1, "1 field2", 20000);
        sendOrderShipped(2, "2 field2", 30000);
        sendOrderShipped(3, "3 field2", 40000);
        sendOrderShipped(4, "4 field2", 50000);
        sendOrderShipped(5, "5 field2", 60000);
        sendOrderShipped(6, "6 field2", 70000);
        sendOrderShipped(7, "7 field2", 80000);

        sendInvoice(7, "7 field3", 10000);
        sendInvoice(6, "6 field3", 20000);
        sendInvoice(5, "5 field3", 30000);
        sendInvoice(4, "4 field3", 40000);
        sendInvoice(3, "3 field3", 50000);
        sendInvoice(2, "2 field3", 60000);
        sendInvoice(1, "1 field3", 70000);
        sendInvoice(0, "0 field3", 80000);
    }

    public void sendOrderCreatedEvent(long correlationId, String adId, long timestamp) {
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent();
        orderCreatedEvent.setCorrelationId(String.valueOf(correlationId));
        orderCreatedEvent.setSomeField1(adId);
        orderCreatedEvent.setTimestamp(timestamp);
        orderCreatedEventProducer.send(new ProducerRecord<String, OrderCreatedEvent>(createdTopic, createdPartition, timestamp, String.valueOf(correlationId), orderCreatedEvent));
    }

    public void sendOrderShipped(long correlationId, String message, long timestamp) {
        ShippedEvent shippedEvent = new ShippedEvent();
        shippedEvent.setCorrelationId(String.valueOf(correlationId));
        shippedEvent.setSomeField2(message);
        shippedEvent.setTimestamp(timestamp);
        shippedEventProducer.send(new ProducerRecord<String, ShippedEvent>(shippedTopic, shippedPartition, timestamp, String.valueOf(correlationId), shippedEvent));
    }

    public void sendInvoice(long correlationId, String message, long timestamp) {
        InvoiceEvent invoiceEvent = new InvoiceEvent();
        invoiceEvent.setCorrelationId(String.valueOf(correlationId));
        invoiceEvent.setSomeField3(message);
        invoiceEvent.setTimestamp(timestamp);
        invoiceEventProducer.send(new ProducerRecord<String, InvoiceEvent>(invoiceTopic, invoicePartition, timestamp, String.valueOf(correlationId), invoiceEvent));
    }

    private static class StringResultingEventMerger implements Merger<String, ResultingEvent> {
        @Override
        public ResultingEvent apply(String s, ResultingEvent event1, ResultingEvent event2) {
            if(event1.getSomeField1() == null) event1.setSomeField1(event2.getSomeField1());
            if(event1.getSomeField2() == null) event1.setSomeField2(event2.getSomeField2());
            if(event1.getSomeField3() == null) event1.setSomeField3(event2.getSomeField3());
            return event1;
        }
    }
}
