package com.github.fac30ff.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        System.out.println("Hello world");

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        //create producer properties
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create producer
        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            //producer record
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("first_topic", "hello world" + i);
            //send data
            producer.send(producerRecord, (metadata, exception) -> {
                //executes record sent successfully or exception is thrown
                if (exception == null) {
                    logger.info("Received new metadata. \n" + "Topic: " + metadata.topic() + "\n" + "Partition: " + metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp" + metadata.timestamp());
                } else {
                    logger.error("Error while producing ", exception);
                }
            });
        }
        //flush data
        producer.flush();
        //producer flush and close
        producer.close();
    }
}
