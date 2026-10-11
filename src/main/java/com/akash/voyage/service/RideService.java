package com.akash.voyage.service;

import com.akash.voyage.dto.NearestDriverResponse;
import com.akash.voyage.dto.RideRequest;
import com.akash.voyage.enums.RideStatus;
import com.akash.voyage.model.Ride;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
public class RideService {

    private final DriverLocationService driverLocationService;
    private final ConcurrentHashMap<String, Ride> rides = new ConcurrentHashMap<>();

    public Ride createRide(RideRequest request){
        String rideId = UUID.randomUUID().toString();
        Ride ride = new Ride();
        ride.setId(rideId);
        ride.setPickupLatitude(request.getLatitude());
        ride.setPickupLongitude(request.getLongitude());
        ride.setStatus(String.valueOf(RideStatus.REQUESTED));

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
            ride.setStatus(String.valueOf(RideStatus.MATCHED));
        }
        else{
            ride.setStatus(String.valueOf(RideStatus.NO_DRIVER_FOUND));
        }

        // save rider in memory
        rides.put(rideId,ride);
        return ride;
    }

    public Ride getRide(String id){
        Ride ride = rides.get(id);
        return ride;
    }
}
