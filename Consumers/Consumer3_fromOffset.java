// Grab messages from a specific offset
// -------------------------------------------------------------------
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class SpecificOffsetConsumer {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "demo_java";
        int partitionNumber = 0; // partition number from which to consume
        long offsetToStartFrom = 1500L; // specify the offset to start from

        // Set up properties for the Consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to the topic's specific partition (this is other way to subscribe Consumer)
        TopicPartition topicPartition = new TopicPartition(topic, partitionNumber);
        consumer.assign(Collections.singletonList(topicPartition));

        // Seek to the desired offset
        consumer.seek(topicPartition, offsetToStartFrom);

        // Poll for data from Kafka
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
