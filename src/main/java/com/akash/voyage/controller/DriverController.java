package com.akash.voyage.controller;

import com.akash.voyage.model.Location;
import com.akash.voyage.model.NearestDriverResponse;
import com.akash.voyage.utils.GeoUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@RequestMapping("/drivers")
public class DriverController {

    private final Map<Integer, Location> driverLocations = new ConcurrentHashMap<>();

    @PostMapping("/{id}/location")
    public ResponseEntity<String> updateLocation(@PathVariable Integer id,@RequestBody Location location) {

        if(location.getLatitude() == null || location.getLongitude() == null || location.getLatitude() < -90 || location.getLatitude() > 90 || location.getLongitude() < -180 || location.getLongitude() > 180) return ResponseEntity.status(400).body("Invalid location data");
        driverLocations.put(id, location);
        return ResponseEntity.ok("Location updated for driver ID: " + id);
    }

    @GetMapping("/{id}/location")
    public ResponseEntity<Location> getLocation(@PathVariable Integer id){
        Location location = driverLocations.get(id);
        if (location != null) {
            return ResponseEntity.ok(location);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/nearby")
    public ResponseEntity<List<NearestDriverResponse>> getNearestDrivers(@RequestParam Double lat, @RequestParam Double lon, @RequestParam Double radiusKm){
        List<NearestDriverResponse> nearestDrivers = new ArrayList<>();

        if(radiusKm <= 0 || lat < -90 || lat > 90 || lon < -180 || lon > 180) return ResponseEntity.status(400).body(nearestDrivers);

        for(Map.Entry<Integer, Location> entry : driverLocations.entrySet()){
            Location location = entry.getValue();
            double distance = GeoUtils.distanceKm(lat,lon,location.getLatitude(),location.getLongitude());

            if(distance <= radiusKm){
                NearestDriverResponse response = new NearestDriverResponse();
                response.setId(entry.getKey());
                response.setDistance(Math.round(distance * 100.0) / 100.0);
                nearestDrivers.add(response);
            }
        }
        nearestDrivers.sort(Comparator.comparingDouble(NearestDriverResponse::getDistance));

        return ResponseEntity.status(200).body(nearestDrivers);
    }

}
