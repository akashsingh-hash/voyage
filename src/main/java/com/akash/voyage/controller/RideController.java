package com.akash.voyage.controller;

import com.akash.voyage.dto.RideRequest;
import com.akash.voyage.model.Ride;
import com.akash.voyage.service.RideService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;


@RestController
@RequestMapping("/rides")
@RequiredArgsConstructor
public class RideController {

    private final RideService rideService;

    @PostMapping
    public ResponseEntity<Ride> createRide(@RequestBody RideRequest request){
        if(!isValid(request)) return ResponseEntity.status(400).body(null);
        Ride ride = rideService.createRide(request);
        // Process the ride request
        return ResponseEntity.status(202).body(ride);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Ride> getRide(@PathVariable String id) {
        Ride ride = rideService.getRide(id);
        if (ride == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(ride);
    }

    @PostMapping("/{id}/complete")
    public ResponseEntity<Ride> completeRide(@PathVariable String id) {
        return ResponseEntity.ok(rideService.completeRide(id));
    }

    @GetMapping("/{id}/events")
    public SseEmitter streamRideEvents(@PathVariable String id){
        return rideService.registerEmitter(id);
    }


    private boolean isValid(RideRequest request){
        return request.getLatitude() != null && request.getLongitude() != null
                && !(request.getLatitude() < -85.05) && !(request.getLatitude() > 85.05)
                && request.getLongitude() >= -180 && request.getLongitude() <= 180;
    }
}
