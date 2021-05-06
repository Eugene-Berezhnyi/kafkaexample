package com.berezhnyi.kafkaexample;

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

import java.time.Duration;
import java.util.Properties;

public class WithJoints implements CommandLineRunner {

    final private String appId;
    final private int createdPartition;
    final private int shippedPartition;
    final private int invoicePartition;
    final private String createdTopic;
    final private String shippedTopic;
    final private String invoiceTopic;
    final private String outputTopic;

    final Producer<String,OrderCreatedEvent> orderCreatedEventProducer;
    final Producer<String,ShippedEvent> shippedEventProducer;
    final Producer<String,InvoiceEvent> invoiceEventProducer;

    public WithJoints() {
        this.appId = "OuterJoin" + (int)(Math.random()*1000);
        this.createdPartition = 0;
        this.shippedPartition = 0;
        this.invoicePartition = 0;
        this.createdTopic = "created" + (int)(Math.random()*1000);
        this.shippedTopic = "shipped" + (int)(Math.random()*1000);
        this.invoiceTopic = "invoice" + (int)(Math.random()*1000);
        this.outputTopic = "outputTopic" + (int)(Math.random()*1000);
        String bootstrapServers = "localhost:9092";

        Properties createdProps = new Properties();
        createdProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        createdProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        createdProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.berezhnyi.kafkaexample.serds.OrderCreatedEventSerializer");
        createdProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        Properties shippedProps = new Properties();
        shippedProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        shippedProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        shippedProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.berezhnyi.kafkaexample.serds.ShippedEventSerializer");
        shippedProps.put(ProducerConfig.LINGER_MS_CONFIG, 10000);

        Properties invoiceProps = new Properties();
        invoiceProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        invoiceProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        invoiceProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.berezhnyi.kafkaexample.serds.InvoiceEventSerializer");
        invoiceProps.put(ProducerConfig.LINGER_MS_CONFIG, 10000);

        orderCreatedEventProducer = new KafkaProducer<>(createdProps);
        shippedEventProducer = new KafkaProducer<>(shippedProps);
        invoiceEventProducer = new KafkaProducer<>(invoiceProps);

    }

    @Override
    public void run(String... args) {
        sendEvents();

        try {
            Thread.sleep(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, OrderCreatedEvent> firstSource = builder.stream(createdTopic, Consumed.with(Serdes.String(), DagSerde.CREATE_SERDE));
        KStream<String, ShippedEvent> secondSource = builder.stream(shippedTopic, Consumed.with(Serdes.String(), DagSerde.SHIPPED_SERDE));
        KStream<String, InvoiceEvent> thirdSource = builder.stream(invoiceTopic, Consumed.with(Serdes.String(), DagSerde.INVOICE_SERDE));
        KStream<String, ResultingEvent> innerJoin = firstSource
                .join(secondSource, (orderCreatedEvent, shippedEvent) -> new ResultingEvent (
                        orderCreatedEvent !=null ? orderCreatedEvent.getCorrelationId() : null,
                        orderCreatedEvent !=null ? orderCreatedEvent.someField1 : null,
                        shippedEvent !=null ? shippedEvent.someField2 : null,
                        null),
                JoinWindows.of(Duration.ofMillis(60000)),
                StreamJoined.with(Serdes.String(), /* key */
                        DagSerde.CREATE_SERDE, /* left value */
                        DagSerde.SHIPPED_SERDE));

        innerJoin.foreach((k,v)-> System.out.println("MIDDLE " + k +"|->|"+ v));

        innerJoin = innerJoin.join(thirdSource,
                        (ResultingEvent resultingEvent, InvoiceEvent invoiceEvent) ->  new ResultingEvent(
                                resultingEvent !=null ? resultingEvent.getCorrelationId() : null,
                                resultingEvent !=null ? resultingEvent.someField1 : null,
                                resultingEvent !=null ? resultingEvent.someField2 : null,
                                invoiceEvent != null ? invoiceEvent.someField3 : null
                        ),
                        JoinWindows.of(Duration.ofMillis(60000)),
                        StreamJoined.with(Serdes.String(), /* key */
                                DagSerde.RESULTING_SERDE, /* left value */
                                DagSerde.INVOICE_SERDE)); /* right value */


        innerJoin.foreach((k,v)-> System.out.println(outputTopic + " " + k +"|->|"+ v));
        innerJoin.to(outputTopic, Produced.with(Serdes.String(), DagSerde.RESULTING_SERDE));

        final Topology topology = builder.build();
        System.out.println(topology);
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }

    private void sendEvents() {
        sendOrderCreatedEvent(0, "shipped 10000 ms after order", 0);
        sendOrderShipped(0,  "shipped 10000 ms after order", 10000);
        sendInvoice(0,  "invoice 10000 ms after order", 10000);

        sendOrderCreatedEvent(1, "shipped 120,000 ms after order", 0);
        sendOrderShipped(1,  "shipped 120,000 ms after order",120000);
        sendInvoice(1,  "invoice 10000 ms after order", 10000);

        sendOrderCreatedEvent(2, "shipped 50000 ms before order", 0);
        sendOrderShipped(2,  "shipped 50000 ms before order", 50000);
        sendInvoice(2,  "invoice 100000 ms after order", 100000);

        sendOrderCreatedEvent(3, "no shipped", 0);

        sendOrderShipped(4,  "no order", 0);

        sendOrderCreatedEvent(5, "duplicate order event1", 0);
        sendOrderCreatedEvent(5, "duplicate order event2", 1);
        sendOrderShipped(5, "duplicate order event", 10000);


        sendOrderCreatedEvent(6, "duplicate shipped 5000 ms and 8000 ms after order", 0);
        sendOrderShipped(6,  "duplicate shipped 5000 ms and 8000 ms after order1",5000);
        sendOrderShipped(6,  "duplicate shipped 5000 ms and 8000 ms after order2", 8000);

        sendOrderCreatedEvent(7, "shipped 120,000 ms after order", 0);
        sendOrderShipped(7,  "shipped 120,000 ms after order",10000);
        sendInvoice(7,  "invoice 10000 ms after order", 130000);
    }

    public void sendOrderCreatedEvent(long correlationId, String adId, long timestamp){
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent();
        orderCreatedEvent.setCorrelationId(String.valueOf(correlationId));
        orderCreatedEvent.setSomeField1(adId);
        orderCreatedEvent.setTimestamp(timestamp);
        orderCreatedEventProducer.send(new ProducerRecord<String, OrderCreatedEvent>(createdTopic, createdPartition, timestamp, String.valueOf(correlationId), orderCreatedEvent));
    }

    public void sendOrderShipped(long correlationId, String message, long timestamp){
        ShippedEvent shippedEvent = new ShippedEvent();
        shippedEvent.setCorrelationId(String.valueOf(correlationId));
        shippedEvent.setSomeField2(message);
        shippedEvent.setTimestamp(timestamp);
        shippedEventProducer.send(new ProducerRecord<String, ShippedEvent>(shippedTopic, shippedPartition, timestamp, String.valueOf(correlationId), shippedEvent));
    }

    public void sendInvoice(long correlationId, String message, long timestamp){
        InvoiceEvent shippedEvent = new InvoiceEvent();
        shippedEvent.setCorrelationId(String.valueOf(correlationId));
        shippedEvent.setSomeField3(message);
        shippedEvent.setTimestamp(timestamp);
        invoiceEventProducer.send(new ProducerRecord<String, InvoiceEvent>(invoiceTopic, invoicePartition, timestamp, String.valueOf(correlationId), shippedEvent));
    }

}
