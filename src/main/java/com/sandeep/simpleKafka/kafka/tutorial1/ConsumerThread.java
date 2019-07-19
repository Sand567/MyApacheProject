package com.sandeep.simpleKafka.kafka.tutorial1;

import com.sandeep.simpleKafka.kafka.constants.KafkaConsumerConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerThread.class.getName());

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    ConsumerThread(CountDownLatch latch) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConsumerConstants.BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerConstants.CONSUMER_GROUPS_THREADS_GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConsumerConstants.AUTO_OFFSET_RESET);

        // create consumer
        consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singletonList(KafkaConsumerConstants.TOPIC));

        this.latch = latch;

    }

    @Override
    public void run() {

        try {

            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info("Topic: {}", record.topic());
                    LOGGER.info("Key: {} | Value: {}", record.key(), record.value());
                    LOGGER.info("Partition: {} | Offset: {}", record.partition(), record.offset());
                }
            }

        } catch (WakeupException w) {
            LOGGER.info("Received shut down signal");
        } finally {
            consumer.close();

            // tells the main code that we are done with the consumer
            latch.countDown();
        }

    }

    public void shutDown() {

        LOGGER.info("ShutDown called, Consumer will now shutdown");
        // This is a method to interrupt consumer.poll()
        // This will throw WakeUpException
        consumer.wakeup();
    }

}
