package com.github.fac30ff.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-java");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //we disable the cache to demonstrate all the "steps" involved in in the transformation - not recommended in production
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        KStream<String,String> textLines = kStreamBuilder.stream("favourite-color-input");
        KStream<String,String> usersAndColors = textLines
                .filter((k,v) -> v.contains(","))
                .selectKey((k,v) -> v.split(",")[0].toLowerCase())
                .mapValues(v -> v.split(",")[1].toLowerCase())
                .filter((u,c) -> Arrays.asList("red", "green", "blue").contains(c));

        usersAndColors.to("user-keys-and-colors");

        KTable<String,String> usersAndColorsTable = kStreamBuilder.table("user-keys-and-colors");
        KTable<String, Long> favouriteColors = usersAndColorsTable
                .groupBy((u,c) -> new KeyValue<>(c,c))
                .count("CountsByColours");

        favouriteColors.to(Serdes.String(), Serdes.Long(), "favourite-color-output");

        KafkaStreams streams = new KafkaStreams(kStreamBuilder, properties);

        // do it only in dev not on prod
        streams.cleanUp();

        streams.start();

        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
