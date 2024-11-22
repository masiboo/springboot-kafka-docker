package com.example.kafka.springbootkafkadocker.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class Consumer {

    private final List<String> messages = new ArrayList<>();

    @KafkaListener(topics = "test_topic", groupId = "group_id")
    public void consumeMessage(String message) {
        messages.add(message); // Store the message
        System.out.println("Consumed message: " + message);
    }

    public List<String> getMessages() {
        return new ArrayList<>(messages); // Return a copy of the list to avoid modification
    }
}

