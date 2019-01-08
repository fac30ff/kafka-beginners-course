package com.github.fac30ff.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {
    public static void main(String[] args) {
        System.out.println("hello world");
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-counts");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        // 1 stream from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        KTable<String, Long> counts = wordCountInput
                // 2 map values to lowercase
                .mapValues(String::toLowerCase)
                // 3 flatmap values split by space
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                // 4 select key to apply a key (discard the old key)
                .selectKey((key, value) -> value)
                // 5 group by key before aggregation
                .groupByKey()
                //6 count occurrences
                .count("counts");
        // 7 to in order to write results back to kafka
        counts.to(Serdes.String(), Serdes.Long(),"word-count-output");

        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();

        //8 printing topology
        System.out.println(streams.toString());

        //9 add shutdown hook to close the stream application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
