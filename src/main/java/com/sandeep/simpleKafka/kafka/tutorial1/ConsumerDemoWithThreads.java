package com.sandeep.simpleKafka.kafka.tutorial1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

    public static void main(String[] args) {

        CountDownLatch latch = new CountDownLatch(1);

        LOGGER.info("Creating the Consumer thread");
        ConsumerThread myConsumerThread = new ConsumerThread(latch);
        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Caught shutdown hook");
            myConsumerThread.shutDown();
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Application interrupted: ", e);
        } finally {
            LOGGER.info("Application is closing");
        }

    }
}
