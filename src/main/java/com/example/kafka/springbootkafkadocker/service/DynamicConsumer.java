package com.example.kafka.springbootkafkadocker.service;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class DynamicConsumer {

    private final ConcurrentKafkaListenerContainerFactory<String, String> factory;
    private final Set<String> messages = new HashSet<>();

    public DynamicConsumer(ConcurrentKafkaListenerContainerFactory<String, String> factory) {
        this.factory = factory;
    }

    public void consumeFromTopic(String topicName) {
        // Create a listener container for the given topic
        ConcurrentMessageListenerContainer<String, String> container =
                factory.createContainer(topicName);

        container.getContainerProperties().setMessageListener((MessageListener<String, String>) record -> {
            String msg = record.value(); // Extract the message value
            messages.add(msg);
            System.out.println("Consumed message from " + topicName + ": " + msg);
        });

        container.start(); // Start the listener container
    }

    public Set<String> getMessages() {
        return messages; // Return a copy of the list to avoid modification
    }
}

