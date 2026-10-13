package com.akash.voyage.component;

import com.akash.voyage.enums.RideStatus;
import com.akash.voyage.model.Ride;
import com.akash.voyage.service.RideService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RideRequestedConsumer {

    private final RideService rideService;

    @KafkaListener(topics = "ride.requested", groupId = "matching-service",concurrency = "3")
    public void listen(String rideId){
        rideService.processRide(rideId);
    }

    @KafkaListener(topics = "ride.requested-dlt")
    public void handleFailedRide(String rideId){
        Ride ride = rideService.getRide(rideId);
        if(ride != null){
            ride.setStatus(RideStatus.FAILED);
            rideService.notifyAndClose(rideId,ride);
        }
    }



}
