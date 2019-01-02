package com.github.fac30ff.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private static String hostname;
    private static String username;
    private static String password;

    {
        try {
            hostname = PropertiesLoader.getPropertiesLoader().asString("hostname");
            username = PropertiesLoader.getPropertiesLoader().asString("username");
            password = PropertiesLoader.getPropertiesLoader().asString("password");
        } catch (Exception e) {
            logger.error(String.valueOf(e));
        }
    }

    private static RestHighLevelClient createRestHighLevelElasticSearchClient() {
        //don't do if you run local elasticsearch
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        return new RestHighLevelClient(restClientBuilder);
    }

    public static void main(String[] args) throws IOException {
        System.out.println("hello world");

        /**
         * twitter dependency conflicts with elasticsearch dependencies
         */

        RestHighLevelClient restHighLevelClient = createRestHighLevelElasticSearchClient();

        //String jsonString = "{\"foo\":\"bar\"}";


        Consumer<String,String> consumer = createElasticSearchConsumer("twitter_tweets");

        //batching consume for improving performance
        BulkRequest bulkRequest = new BulkRequest();

        //poll new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            int recordsCount = records.count();
            logger.info("Received " + recordsCount + " records");

            for (ConsumerRecord<String,String> record : records) {

                //2 strategies
                //kafka generic id
                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try {
                    //twitter specific id
                    String idFromTwitter = exctractIdFromTweets(record.value());


                    //where we insert data into ElasticSearch
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                            .source(record.value(), XContentType.JSON, idFromTwitter/*this is to make our consumer idempotent*/);
                    //add bulk
                    bulkRequest.add(indexRequest); // we add to bulk request
                } catch (NullPointerException e) {
                    logger.warn("skipping bad data: " + record.value());
                }
                //IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

                //id from elastic search
                //String idFromElasticSearch = indexResponse.getId();

                //logger.info(indexResponse.getId());

                //introduce small delay
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (recordsCount > 0) {
                BulkResponse bulkItemResponses = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets ");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //close client
        //restHighLevelClient.close();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String exctractIdFromTweets(String value) {
        //gson library
        return jsonParser.parse(value)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static Consumer<String, String> createElasticSearchConsumer(String topic) {
        Properties properties = new Properties();

        String bootstrapServer = "127.0.0.1:9092";
        //String topic = "twitter_tweets";

        //create consumer config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        String groupId = "kafka-demo-elasticsearch";
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //manual inserting offsets
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //for readability of number of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        //create a consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

}