package org.example.demos.kafka;

// Consumer simple

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer1 {

    // We will create a logger
    private static final Logger log = LoggerFactory.getLogger(Consumer1.class.getSimpleName());

    public static void main(String[] args) {
        log.info("This is the Consumer logger!");

        String topic = "demo_java";

        // 1. Create the Kafka Consumer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // We need to set up the GroupID!!
        String groupId = "my_consumer_app";
        properties.setProperty("group.id",groupId);

        // When we consume items from topics we can do in 3 different ways:
        // latest / earliest / none (we need a GroupId created before run the consumer)
        properties.setProperty("auto.offset.reset", "earliest");

        // 2. Instantiate the Consumer
        // Now we need ot instantiate a Kafka Consumer with our "properties"
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties); // remove String,String these are optional

        // 3. Subscribe the Consumer to a topic
        // Subscribe to the Topic - We need to pass a list of topics using the Arrays class
        consumer.subscribe(Arrays.asList(topic));

        // 4. Poll for data
        while (true){
            log.info("Polling...");

            // 4.1 Consume a batch of records
            ConsumerRecords<String,String> records_coll = consumer.poll(Duration.ofMillis(1000));  // Important this return a collection of records

            // 4.2 Process each record
            for (ConsumerRecord<String, String> record: records_coll){
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }

        }

    }
}
