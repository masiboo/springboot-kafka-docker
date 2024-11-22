package com.example.kafka.springbootkafkadocker.service;

import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@AllArgsConstructor
public class DynamicConsumer2 {

    private final ConcurrentKafkaListenerContainerFactory<String, String> factory;
    private final Set<String> messages = new HashSet<>();


    public void startListening(String topicName) {
        ConcurrentMessageListenerContainer<String, String> container =
                factory.createContainer(topicName);

        container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
            String message = record.value();
            messages.add(message);
            System.out.println("Consumed message from " + topicName + ": " + message);
        });

        container.start();
        System.out.println("Started listening to topic: " + topicName);
    }

    public Set<String> getMessages() {
        return messages;
    }
}

