package com.akash.voyage.service;

import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DriverLocationService {

    private final StringRedisTemplate redisTemplate;
    private static final String KEY = "drivers";

    public DriverLocationService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // Save driver location
    public void saveLocation(Integer id, double latitude, double longitude) {
        Point point = new Point(longitude, latitude); // longitude first
        redisTemplate.opsForGeo().add(KEY, point, id.toString());
    }

    // Get Location
    public Point getLocation(Integer id) {
        List<Point> points = redisTemplate.opsForGeo().position(KEY, id.toString());
        return points.get(0);
    }

    // Find nearby drivers
    public GeoResults<RedisGeoCommands.GeoLocation<String>> findNearby(double latitude, double longitude, double radiusKm) {
        Point center = new Point(longitude, latitude);
        Distance distance = new Distance(radiusKm, Metrics.KILOMETERS);
        Circle circle = new Circle(center, distance);

        RedisGeoCommands.GeoRadiusCommandArgs args = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                .includeDistance()
                .sortAscending();

        return redisTemplate.opsForGeo().radius(KEY, circle, args);
    }
}
