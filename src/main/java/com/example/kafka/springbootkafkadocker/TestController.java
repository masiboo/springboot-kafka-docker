package com.example.kafka.springbootkafkadocker;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@AllArgsConstructor
public class TestController {

    private final Producer producer;

    private final Consumer consumer;

    @PostMapping("/publish")
        public void messageToTopic(@RequestParam("message") String message){

        this.producer.sendMessage(message);
    }

    @GetMapping("/consume")
    public List<String> getConsumedMessages() {
        return consumer.getMessages();
    }


}
