package com.akash.voyage.service;

import com.akash.voyage.dto.NearestDriverResponse;
import com.akash.voyage.dto.RideRequest;
import com.akash.voyage.enums.RideStatus;
import com.akash.voyage.exception.InvalidRideStateException;
import com.akash.voyage.exception.RideNotFoundException;
import com.akash.voyage.model.Ride;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class RideService {

    private final KafkaTemplate<String,String> kafkaTemplate;

    private final DriverLocationService driverLocationService;
    @Getter
    private final ConcurrentHashMap<String, Ride> rides = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, SseEmitter> emitterConcurrentHashMap = new ConcurrentHashMap<>();

    public Ride createRide(RideRequest request){
        String rideId = UUID.randomUUID().toString();
        Ride ride = new Ride();
        ride.setId(rideId);
        ride.setPickupLatitude(request.getLatitude());
        ride.setPickupLongitude(request.getLongitude());
        ride.setStatus(RideStatus.REQUESTED);

        // save rider in memory
        rides.put(rideId,ride);

        // publish to kafka
        log.info("Publishing ride {} to Kafka for processing",ride.getId());
        kafkaTemplate.send("ride.requested",ride.getId(),ride.getId())
                .whenComplete((result, ex) -> {
                    if(ex != null){
                        log.error("Failed to send ride {} to Kafka", ride.getId(),ex);
                        // optionally mark Ride As Failed
                        ride.setStatus(RideStatus.FAILED);
                        notifyAndClose(rideId,ride);
                    }
                });

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

    public void processRide(String rideId) {
        Ride ride = rides.get(rideId);
        if(ride == null){
            log.warn("Ride with {} id not present.",rideId);
            return;
        }

        if(ride.getStatus() != RideStatus.REQUESTED){
            log.warn("Ride status is not REQUESTED");
            return;
        }

        // Find nearby drivers within 5 km
        List<NearestDriverResponse> nearbyDrivers =
                driverLocationService.findNearby(ride.getPickupLatitude(), ride.getPickupLongitude(), 5.0);

        Integer matchedDriver = null;

        for(NearestDriverResponse driver:nearbyDrivers){
            Integer driverId = driver.getId();
            if(driverLocationService.tryReserve(driverId,rideId)){
                matchedDriver = driverId;
                break; // Stop at the first successful reservation
            }
        }
        if(matchedDriver != null){
            ride.setDriverId(matchedDriver);
            ride.setStatus(RideStatus.MATCHED);
            log.info("Ride {} matched with driver {}",rideId,matchedDriver);
        }
        else{
            ride.setStatus(RideStatus.NO_DRIVER_FOUND);
            log.info("Ride {} could not find any driver",rideId);
        }

        notifyAndClose(rideId,ride);
    }


    public SseEmitter registerEmitter(String rideId) {
        Ride ride = rides.get(rideId);
        if(ride == null){
            throw new RideNotFoundException(rideId);
        }
        SseEmitter emitter = new SseEmitter(30_000L);

        emitter.onCompletion(() -> emitterConcurrentHashMap.remove(rideId,emitter));
        emitter.onTimeout(() -> emitterConcurrentHashMap.remove(rideId,emitter));
        emitter.onError(e -> emitterConcurrentHashMap.remove(rideId,emitter));

        emitterConcurrentHashMap.put(rideId, emitter);

        // If ride already finished, send immediately

        if (ride.getStatus() != RideStatus.REQUESTED) {
            notifyAndClose(rideId, ride);
        }
        return emitter;
    }

    public void notifyAndClose(String rideId,Ride ride){
        SseEmitter emitter = emitterConcurrentHashMap.remove(rideId); // atomic remove
        if(emitter != null){
            try{
                emitter.send(SseEmitter.event()
                        .name("ride-status")
                        .data(ride));
                emitter.complete();
            } catch (Exception e) {
                log.error("Failed to send SSE for ride {}", rideId, e);
            }
        }
    }

    public Ride getRide(String id){
        return rides.get(id);
    }

}
