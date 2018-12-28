package com.github.fac30ff.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static void main(String[] args) {
        System.out.println("hello world");

    }

}
