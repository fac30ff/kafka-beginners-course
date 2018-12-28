package kafka.tutorial1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }
    private ConsumerDemoWithThread() {}

    private void run() {
        System.out.println("Hello world!");

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        //subscribe to a topic
        String topic = "first_topic";

        //latch for dealing with multiple thread
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");

        //create consumer runnable
        Runnable myConsumer = new ConsumerThread(latch, topic);

        //create thread for consumer
        Thread myThread = new Thread(myConsumer);
        myThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumer).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application is interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        private Consumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        ConsumerThread(CountDownLatch latch, String topic) {
            this.latch = latch;
            //create a consumer
            consumer = new KafkaConsumer<>(staticProps());
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            //poll new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Shutdown signal received!");
            } finally {
                consumer.close();
                //tell our main code we're done with consumer
                latch.countDown();
            }
        }

        void shutdown() {
            //to interrupt .poll() method of consumer
            consumer.wakeup();
        }

        Properties staticProps() {
            Properties properties = new Properties();
            String bootstrapServer = "127.0.0.1:9092";
            //create consumer config
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            String groupId = "my-sixth-application";
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return properties;
        }
    }
}
