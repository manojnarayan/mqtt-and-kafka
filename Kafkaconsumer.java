package com.example.Mqttkafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
 
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
 
public class Kafkaconsumer {
 
    public static void main(String[] args) {
        String kafkaBroker = "10.102.151.79:9092";
        String kafkaTopic = "kafka-topic1";
 
        // Set up Kafka consumer properties
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-group");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
 
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList(kafkaTopic));
 
        // Consume messages from Kafka broker
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Received message from Kafka: " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
