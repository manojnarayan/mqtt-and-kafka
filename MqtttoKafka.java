package com.example.Mqttkafka;




import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.client.mqttv3.*;
 
import java.util.Properties;
 
public class MqtttoKafka {
 
    public static void main(String[] args) {
        String mqttBroker = "tcp://10.86.143.44:1883";
        String kafkaBroker = "10.102.151.79:9092";
        String mqttTopic = "mqtt/topic";
        String kafkaTopic = "kafka-topic1";
 
        try {
            // Set up Kafka producer properties
            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
 
            // Set up MQTT client
            MqttClient mqttClient = new MqttClient(mqttBroker, MqttClient.generateClientId());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
 
            mqttClient.connect(options);
            System.out.println("Connected to MQTT broker: " + mqttBroker);
 
            mqttClient.subscribe(mqttTopic, (topic, message) -> {
                String payload = new String(message.getPayload());
                System.out.println("Received message from MQTT: " + payload);
 
                ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, payload);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Sent message to Kafka: " + payload);
                    } else {
                        exception.printStackTrace();
                    }
                });
            });
 
            // Add a shutdown hook to clean up resources
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    mqttClient.disconnect();
                    producer.close();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }));
 
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
 
