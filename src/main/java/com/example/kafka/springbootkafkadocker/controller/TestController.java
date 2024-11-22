package com.example.kafka.springbootkafkadocker.controller;

import com.example.kafka.springbootkafkadocker.service.*;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;

@RestController
@AllArgsConstructor
public class TestController {

    private final Producer producer;

    private final Consumer consumer;

    private final KafkaTopicService kafkaTopicService;

    private final DynamicConsumer dynamicConsumer;

    private final DynamicConsumer2 dynamicConsumer2;

    @PostMapping("/publish")
        public void messageToTopic(@RequestParam("message") String message, @RequestParam("topicName") String topicName){

        this.producer.sendMessage(message, topicName);
    }

    @GetMapping("/consume")
    public List<String> getConsumedMessages() {
        return consumer.getMessages();
    }

    @PostMapping("/create-topic")
    public String createTopic(
            @RequestParam("name") String topicName,
            @RequestParam(value = "partitions", defaultValue = "1") int partitions,
            @RequestParam(value = "replicationFactor", defaultValue = "1") short replicationFactor) {
        kafkaTopicService.createTopic(topicName, partitions, replicationFactor);
        return "Topic '" + topicName + "' created successfully!";
    }

        @GetMapping("/consumeFromTopic")
    public Set<String> consumeMessagesFromTopic() {
        return dynamicConsumer2.getMessages();
    }


}
