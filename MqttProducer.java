package com.example.Mqttkafka;

import org.eclipse.paho.client.mqttv3.*;

public class MqttProducer {
 
    public static void main(String[] args) {
        String mqttBroker = "tcp://10.86.143.44:1883";
        String mqttTopic = "mqtt/topic";
        String mqttMessage = "Mqtt Data route to kafka";
 
        try {
            // Set up MQTT client
            MqttClient mqttClient = new MqttClient(mqttBroker, MqttClient.generateClientId());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
 
            mqttClient.connect(options);
            System.out.println("Connected to MQTT broker: " + mqttBroker);
 
            // Publish message to MQTT broker
            MqttMessage message = new MqttMessage(mqttMessage.getBytes());
            mqttClient.publish(mqttTopic, message);
            System.out.println("Published message to MQTT: " + mqttMessage);
 
            mqttClient.disconnect();
 
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
