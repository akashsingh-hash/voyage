package com.akash.voyage.component;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class RideRequestedConsumer {

    @KafkaListener(topics = "ride.requested", groupId = "matching-service")
    public void listen(String rideId){
        System.out.println("Received ride id : " + rideId);
    }
}
