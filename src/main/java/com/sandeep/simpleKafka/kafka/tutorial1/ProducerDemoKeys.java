package com.sandeep.simpleKafka.kafka.tutorial1;

import com.sandeep.simpleKafka.kafka.constants.KafkaProducerConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerDemoKeys.class.getName());

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProducerConstants.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = KafkaProducerConstants.KAFKA_TOPIC;
            String value = KafkaProducerConstants.TEXT_MESSAGE + i;
            String key = KafkaProducerConstants.ID + i;

            // create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            LOGGER.info("Key: {}", key);
            // id       partition
            // ==================
            // id_0     1
            // id_1     0
            // id_2     2
            // id_3     0
            // id_4     2
            // id_5     2
            // id_6     0
            // id_7     2
            // id_8     1
            // id_9     2

            // send data - asynchronous
            producer.send(record, (recordMetadata, e) -> {
                // executes whenever a record is successfully sent or when an exception is thrown

                if (null == e) {
                    LOGGER.info("Received new metadata ==> Topic: {}; Partition: {}; Offsets: {}; Timestamp: {}",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                } else {
                    LOGGER.error("Exception occurred: ", e);
                }

            }).get(); // block the .send() to make it synchronous. But don't do this in production
        }

        // flush producer
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
