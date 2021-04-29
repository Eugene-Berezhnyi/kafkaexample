package com.berezhnyi.kafkaexample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

@Component
public class Pipe implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {
        String bootstrapServers = "localhost:9092";
        String inputTopic = "inputTopic";
        String outputTopic = "outputTopic";
        final Serde<String> stringSerde = Serdes.String();
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-live-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        streamsConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.json.StringSerializer");
        streamsConfiguration.put("schema.registry.url", "http://127.0.0.1:8081");
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory(Paths.get("state"),"MyPrefix").toAbsolutePath().toString());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(inputTopic);

        KTable<String, Long> wordCounts = source
                .flatMapValues(line -> Arrays.asList(line.toLowerCase().split(" ")))
                .groupBy((keyIgnored, word) -> word)
                .count();

        wordCounts.toStream().foreach((k,v)-> System.out.println(k +"|->|"+ v));
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));


        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();
    }
}
