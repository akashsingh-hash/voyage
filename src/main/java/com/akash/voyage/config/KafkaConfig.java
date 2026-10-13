package com.akash.voyage.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic rideRequestedTopic(){
        return TopicBuilder.name("ride.requested")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic rideRequestedDLT(){
        return TopicBuilder.name("ride.requested-dlt")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String,String> kafkaTemplate){
        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate);
        FixedBackOff backOff = new FixedBackOff(1000L,3); // 3 reties, 1s apart
        return new DefaultErrorHandler(recoverer,backOff);
    }
}
