package com.github.fac30ff.kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {

    private final Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());
    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_topic");
        KStream<String, String> filteredTopic = inputTopic.filter((k, jsonTweets) -> extractUserFollowersInTweets(jsonTweets) > 10000
                /*filter for tweets which has a user over 10000 followers*/
        );
        filteredTopic.to("important_tweets");
        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        //start our stream application
        kafkaStreams.start();
    }

    private static int extractUserFollowersInTweets(String value) {
        //gson library
        try {
            return jsonParser.parse(value)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
