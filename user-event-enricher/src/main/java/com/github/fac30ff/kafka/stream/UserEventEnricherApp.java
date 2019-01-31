package com.github.fac30ff.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class UserEventEnricherApp {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        //we get a global table out of Kafka. This table will be replicated on each Kafka Streams application
        //the key of our globalKTable is the user ID
        GlobalKTable<String, String> usersGlobalTable = kStreamBuilder.globalTable("user-table");

        //we get a stream of user purchases
        KStream<String, String> userPurchases = kStreamBuilder.stream("user-purchases");

        //we want to enrich that stream
        KStream<String, String> userPurchasesEnrichedJoin = userPurchases.join(usersGlobalTable, (k, v) -> k,
                /*map from the (key, value) of this stream to the key of the GlobalKTable */(userPurchase, userInfo) -> "Purchase= " + userPurchase + ", UserInfo=[" + userInfo + "]");
        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        //we want to enrich that stream using a Left Join
        KStream<String, String> userPurchasesEnrichedLeftjoin = userPurchases.leftJoin(usersGlobalTable, (k, v) -> k,
                /*map from (key, value) of this stream to the key of the GlobalKTable*/ (userPurchase, userInfo) ->
                {
                    //as this is a left join, userinfo can be null
                    if (userInfo != null) {
                        return "Purchase=" + userPurchase + ", UserInfo=[" + userInfo +"]";
                    } else {
                        return "Purchase=" + userPurchase + ", UserInfo=[null]";
                    }
                });
        userPurchasesEnrichedLeftjoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(kStreamBuilder, properties);
        streams.cleanUp(); //only do this in dev - not in prod
        streams.start();

        //print the topology
        System.out.println(streams.toString());

        //shutdown hook to correctly close the systems application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
