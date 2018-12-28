package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("Hello world");

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServer = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create producer
        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            String key = "Key_id_" + i;
            String value = "Hello world " + i;
            String topic = "first_topic";
            //producer record
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, key, value);

            logger.info("Key is: " + key);

            //send data
            producer.send(producerRecord, (metadata, exception) -> {
                //executes record sent successfully or exception is thrown
                if (exception == null) {
                    logger.info("Received new metadata. \n" + "Topic: " + metadata.topic() + "\n" + "Partition: " + metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp" + metadata.timestamp());
                } else {
                    logger.error("Error while producing ", exception);
                }
            }).get(); //block the .send() to make it synchronous - don't do this in production
        }
        //flush data
        producer.flush();
        //producer flush and close
        producer.close();
    }
}
