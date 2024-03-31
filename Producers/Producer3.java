package org.example.demos.kafka;

// Producer with Callback and looping to show the partitioner

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer3 {

    // Create a logger
    private static final Logger log = LoggerFactory.getLogger(Producer3.class.getSimpleName());

    public static void main(String[] args) {
        log.info("This is the Producer logger!");

        // 1. Create the Kafka Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // 2. Instantiate the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        // 3. Create a loop to send multiple records
        // Now we send 10 records -- to demonstrate the StickyPartitioner which is the default partitioner for efficiency
        for (int i = 0; i < 20; i++) {

            // 4. Create some data (record) to send
            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","hello... this is the data value from Producer with Callback " + i);

            // 5.  Send data to the topic wih a callback
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // this callback method will be executed once each record is successfully sent or an exception occurs
                    if (e == null) {
                        // Everything was fine
                        log.info("\n #################################### \n" +
                                "Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n" +
                                "#################################### \n");
                    } else {
                        log.error("Error sending data to the Topic ", e);
                    }
                }
            });
        }


        // 6. Flush and close the producer
        producer.flush(); // tell the Producer to send all data and block until done i.e. synchronous operation
        producer.close();   // close() method also call .flush() before... but I want to make this clear
    }
}
