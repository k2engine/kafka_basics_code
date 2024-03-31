package org.example.demos.kafka;

// Producer with keys

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer4 {

    // We will create a logger
    private static final Logger log = LoggerFactory.getLogger(Producer4.class.getSimpleName());

    public static void main(String[] args) {
        log.info("This is the Producer logger!");

        // 1. Create the Kafka Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // 2. Instantiate the Producer and define the topic
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        String topic = "demo_java";

        // 3. Create a loop to send multiple records with keys
        for (int j=0; j<3; j++){
            log.info("----------------------------------------------------------------------");

            // Now we send 10 records -- to demonstrate the StickyPartitioner which is the default partitioner for efficiency
            for (int i = 0; i < 20; i++) {

                // 4. Create some data (record) to send
                String key = "id_" + i;
                String value = "Hello BMW Team SA - " + i;
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic,key, value);

                // 5.  Send data to the topic wih a callback
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // this callback method will be executed once each record is successfully sent or an exception occurs
                        if (e == null) {
                            // Everything was fine
                            log.info("Key: " + key + " - Partition: " + metadata.partition() + " - ts: " + metadata.timestamp() + "\n" );
                        } else {
                            log.error("Error sending data to the Topic ", e);
                        }
                    }
                });
            }
            // wait for a moment
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // 6. Flush and close the producer
        producer.flush(); // tell the Producer to send all data and block until done i.e. synchronous operation
        producer.close();   // close() method also call .flush() before... but I want to make this clear
    }
}
