package com.akash.voyage.controller;

import com.akash.voyage.model.Location;
import com.akash.voyage.model.NearestDriverResponse;
import com.akash.voyage.service.DriverLocationService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/drivers")
@RequiredArgsConstructor
public class DriverController {

    private final DriverLocationService driverLocationService;

    @PostMapping("/{id}/location")
    public ResponseEntity<String> updateLocation(@PathVariable Integer id,@RequestBody Location location) {

        if(location.getLatitude() == null || location.getLongitude() == null || location.getLatitude() < -85.05 || location.getLatitude() > 85.05 || location.getLongitude() < -180 || location.getLongitude() > 180) return ResponseEntity.status(400).body("Invalid location data");
        driverLocationService.saveLocation(id,location.getLatitude(),location.getLongitude());
        return ResponseEntity.ok("Location updated for driver ID: " + id);
    }

    @GetMapping("/{id}/location")
    public ResponseEntity<Location> getLocation(@PathVariable Integer id){
        Point p = driverLocationService.getLocation(id);
        if(p == null) return ResponseEntity.notFound().build();
        Location location = new Location(p.getY(),p.getX());
        return ResponseEntity.ok(location);

    }

    @GetMapping("/nearby")
    public ResponseEntity<List<NearestDriverResponse>> getNearestDrivers(@RequestParam Double lat, @RequestParam Double lon, @RequestParam Double radiusKm){
        List<NearestDriverResponse> nearestDrivers = new ArrayList<>();

        if(radiusKm <= 0 || lat < -85.05 || lat > 85.05 || lon < -180 || lon > 180) return ResponseEntity.status(400).body(nearestDrivers);
        GeoResults<RedisGeoCommands.GeoLocation<String>> nearbyDrivers = driverLocationService.findNearby(lat, lon, radiusKm);

        nearbyDrivers.getContent().forEach(driver -> {
            NearestDriverResponse response = new NearestDriverResponse();
            response.setId(Integer.parseInt(driver.getContent().getName()));
            response.setDistance(Math.round(driver.getDistance().getValue() * 100.0) / 100.0);
            nearestDrivers.add(response);
        });

        return ResponseEntity.status(200).body(nearestDrivers);
    }

}
