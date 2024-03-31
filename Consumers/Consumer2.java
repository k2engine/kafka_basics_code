package org.example.demos.kafka;

// Consumer with proper Shutdown

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer2 {

    // We will create a logger
    private static final Logger log = LoggerFactory.getLogger(Consumer2.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I'am a Consumer with Shutdown");

        String topic = "demo_java";

        // 1. Create the Kafka Consumer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // We need to set up the GroupID
        String groupId = "my_consumer_app2";
        properties.setProperty("group.id",groupId);

        // When we consume items from topics we can do in 3 different ways:
        // latest / earliest / none (we need a GroupId created before run the consumer)
        properties.setProperty("auto.offset.reset", "earliest");

        // 2. Instantiate the Consumer
        // Now we need ot instantiate a Kafka Consumer with our "properties"
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties); // remove String,String these are optional

        // 3. Create a Shutdown Hook
        // 3.1. get a reference to a main thread
        final Thread mainThread = Thread.currentThread();

        // 3.2 adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run(){
                log.info("Detected a shutdown. exiting by calling consumer.wakeup()....");

                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
                                             });
        // 4. Subscribe the Consumer to a topic
        try {
            // Subscribe to the Topic - We need to pass a list of topics using the Arrays class
            consumer.subscribe(Arrays.asList(topic));

            // 5. Poll for data
            while (true) {
                log.info("Polling...");
                // 5.1 Consume a batch of records
                ConsumerRecords<String, String> records_coll = consumer.poll(Duration.ofMillis(1000));  // Important this return a collection of records

                // 5.2 Process each record
                for (ConsumerRecord<String, String> record : records_coll) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown...");
        } catch (Exception e) {
            log.error("Unexpected consumer failure", e);
        } finally {
            consumer.close();   // close the consumer and this action will also commit the offsets
            log.info("The consumer is now properly shut downed.");
        }
    }
}
