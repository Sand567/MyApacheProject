package com.sandeep.simpleKafka.kafka.tutorial1;

import com.sandeep.simpleKafka.kafka.constants.KafkaProducerConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProducerConstants.BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaProducerConstants.KAFKA_TOPIC, KafkaProducerConstants.TEXT_MESSAGE);

        // send data - asynchronous
        producer.send(record);

        // flush producer
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
