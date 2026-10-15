package com.akash.voyage.component;

import com.akash.voyage.service.RideService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RideRequestedConsumer {

    private final RideService rideService;

    @KafkaListener(topics = "ride.requested", groupId = "matching-service",concurrency = "3")
    public void listen(String rideId){
        rideService.processRide(rideId);
    }

    @KafkaListener(topics = "ride.requested-dlt",groupId = "ride-dlt-handler")
    public void handleFailedRide(String rideId){
        log.warn("DLT received ride ID: {}", rideId);
        rideService.markRideFailed(rideId);
    }

}
