package com.example.kafka.springbootkafkadocker.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

@Service
public class KafkaTopicService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public void createTopic(String topicName, int partitions, short replicationFactor) {
        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            adminClient.createTopics(Collections.singleton(newTopic));
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic: " + topicName, e);
        }
    }
}
