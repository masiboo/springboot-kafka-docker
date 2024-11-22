package com.example.kafka.springbootkafkadocker.service;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

@Service
public class Producer {

  //  private static final String DEFAULT_TOPIC = "test_topic";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private DynamicConsumer2 dynamicConsumer2;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public void sendMessage(String message, String topicName) {
        ensureTopicExists(topicName); // Ensure the topic exists
        this.kafkaTemplate.send(topicName, message);
        dynamicConsumer2.startListening(topicName);
    }

    private void ensureTopicExists(String topicName) {
        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {
            // Fetch existing topics
            Set<String> topicNames = adminClient.listTopics().names().get();
            // Check if the topic exists
            if (!topicNames.contains(topicName)) {
                // Create the topic if it does not exist
                NewTopic newTopic = new NewTopic(topicName, 3, (short) 1); // 3 partitions, replication factor 1
                adminClient.createTopics(Collections.singleton(newTopic));
                System.out.println("Created new topic: " + topicName);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error ensuring topic exists: " + topicName, e);
        }
    }
}
