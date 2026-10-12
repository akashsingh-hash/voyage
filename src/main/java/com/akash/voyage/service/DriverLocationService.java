package com.akash.voyage.service;

import com.akash.voyage.dto.NearestDriverResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Service
@RequiredArgsConstructor
public class DriverLocationService {

    private final StringRedisTemplate redisTemplate;
    private static final String KEY = "drivers";


    // Save driver location
    public void saveLocation(Integer id, double latitude, double longitude) {
        Point point = new Point(longitude, latitude); // longitude first
        redisTemplate.opsForGeo().add(KEY, point, id.toString());
        redisTemplate.opsForValue().set(aliveKey(id),"1",java.time.Duration.ofMinutes(2));
    }

    // Get Location
    public Point getLocation(Integer id) {
        List<Point> points = redisTemplate.opsForGeo().position(KEY, id.toString());
        if(points.isEmpty() || points.getFirst() == null){
            return null;
        }
        return points.getFirst();
    }

    // Find nearby drivers
    public List<NearestDriverResponse> findNearby(double latitude, double longitude, double radiusKm) {
        Point center = new Point(longitude, latitude);
        Distance distance = new Distance(radiusKm, Metrics.KILOMETERS);
        Circle circle = new Circle(center, distance);

        RedisGeoCommands.GeoRadiusCommandArgs args = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                .includeDistance()
                .sortAscending();

        GeoResults<RedisGeoCommands.GeoLocation<String>> results = redisTemplate.opsForGeo().radius(KEY, circle, args);

        if(results == null || results.getContent().isEmpty()){
            return Collections.emptyList();
        }

        // Filter only alive drivers
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> aliveDrivers = new ArrayList<>();
        results.getContent().forEach(
                result -> {
                    String driverId = result.getContent().getName();
                    if (Boolean.TRUE.equals(redisTemplate.hasKey(aliveKey(Integer.parseInt(driverId))))) {
                        aliveDrivers.add(result);
                    }
                }
        );

        List<NearestDriverResponse> nearestDrivers = new ArrayList<>();
        aliveDrivers.forEach(driver -> {
            NearestDriverResponse response = new NearestDriverResponse();
            response.setId(Integer.parseInt(driver.getContent().getName()));
            response.setDistance(Math.round(driver.getDistance().getValue() * 100.0) / 100.0);
            nearestDrivers.add(response);
        });
        return nearestDrivers;
    }

    public boolean tryReserve(Integer driverId, String rideId){
        Boolean success = redisTemplate.opsForValue().setIfAbsent(busyKey(driverId),rideId, Duration.ofMinutes(5));
        return Boolean.TRUE.equals(success);
    }

    public boolean release(Integer driverId,String rideId){
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText("if redis.call('Get',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end");
        script.setResultType(Long.class);
        Long result = redisTemplate.execute(script,List.of(busyKey(driverId)),rideId);
        return result != null && result == 1L;
    }

    private String aliveKey(Integer id){
        return "driver:" + id + ":alive";
    }

    private String busyKey(Integer id){
        return "driver:" + id + ":busy";
    }

}
