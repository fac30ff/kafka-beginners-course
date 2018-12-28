package kafka.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());

        Properties properties = new Properties();

        String bootstrapServer = "127.0.0.1:9092";

        //create consumer config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        String groupId = "my-fifth-application";
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create a consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        //subscribe to a topic
        String topic = "first_topic";
        consumer.subscribe(Collections.singletonList(topic));
        //poll new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            for (ConsumerRecord<String,String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
