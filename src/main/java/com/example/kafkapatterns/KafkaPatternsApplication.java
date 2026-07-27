package com.example.kafkapatterns;

import com.example.kafkapatterns.config.MessagingRulesProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(MessagingRulesProperties.class)
public class KafkaPatternsApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaPatternsApplication.class, args);
    }
}
