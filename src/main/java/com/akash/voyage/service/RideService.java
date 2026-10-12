package com.akash.voyage.service;

import com.akash.voyage.dto.NearestDriverResponse;
import com.akash.voyage.dto.RideRequest;
import com.akash.voyage.enums.RideStatus;
import com.akash.voyage.exception.InvalidRideStateException;
import com.akash.voyage.exception.RideNotFoundException;
import com.akash.voyage.model.Ride;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class RideService {

    private final KafkaTemplate<String,String> kafkaTemplate;

    private final DriverLocationService driverLocationService;
    private final ConcurrentHashMap<String, Ride> rides = new ConcurrentHashMap<>();

    public Ride createRide(RideRequest request){
        String rideId = UUID.randomUUID().toString();
        Ride ride = new Ride();
        ride.setId(rideId);
        ride.setPickupLatitude(request.getLatitude());
        ride.setPickupLongitude(request.getLongitude());
        ride.setStatus(RideStatus.REQUESTED);

        // Find nearby drivers within 5 km
        List<NearestDriverResponse> nearbyDrivers =
                driverLocationService.findNearby(request.getLatitude(), request.getLongitude(), 5.0);

        Integer matchedDriver = null;

        for(NearestDriverResponse driver:nearbyDrivers){
            Integer driverId = driver.getId();
            if(driverLocationService.tryReserve(driverId,rideId)){
                matchedDriver = driverId;
                break; // Stop at the first successfull reservation
            }
        }
        if(matchedDriver != null){
            ride.setDriverId(matchedDriver);
            ride.setStatus(RideStatus.MATCHED);
        }
        else{
            ride.setStatus(RideStatus.NO_DRIVER_FOUND);
        }

        // save rider in memory
        rides.put(rideId,ride);

        // publish to kafka
        System.out.println("Sending to Kafka: " + ride.getId());
        kafkaTemplate.send("ride.requested",ride.getId(),ride.getId());

        return ride;
    }

    public Ride completeRide(String rideId) {
        Ride ride = rides.get(rideId);
        if (ride == null) throw new RideNotFoundException(rideId);

        if (ride.getStatus() != RideStatus.MATCHED) {
            throw new InvalidRideStateException(rideId, ride.getStatus());
        }

        // Mark completed first -> IT's MATCHED -> COMPLETED
        ride.setStatus(RideStatus.COMPLETED);

        // Try to release driver safely
        boolean released = driverLocationService.release(ride.getDriverId(), ride.getId());
        if (!released) {
            log.warn("Release failed for driver {} ride {}", ride.getDriverId(), ride.getId());
        }

        return ride;
    }





    public Ride getRide(String id){
        return rides.get(id);
    }
}
