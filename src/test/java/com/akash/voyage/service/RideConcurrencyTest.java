package com.akash.voyage.service;

import com.akash.voyage.dto.RideRequest;
import com.akash.voyage.enums.RideStatus;
import com.akash.voyage.model.Ride;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class RideConcurrencyTest {

    @Autowired
    private DriverLocationService driverLocationService;

    @Autowired
    private RideService rideService;

    @Autowired
    private StringRedisTemplate redisTemplate;


    @BeforeEach
    void setup() {
        // Clean up any leftover keys
        redisTemplate.delete("driver:9001:busy");
        redisTemplate.delete("driver:9001:alive");
        redisTemplate.opsForGeo().remove("drivers", "9001");

        // Save one driver at a test location
        driverLocationService.saveLocation(9001, 40.0, -74.0);
    }

    @AfterEach
    void destroy(){
        redisTemplate.delete("driver:9001:busy");
        redisTemplate.delete("driver:9001:alive");
        redisTemplate.opsForGeo().remove("drivers", "9001");
    }

    @Test
    void onlyOneThreadShouldReserveDriver() throws InterruptedException {

        String rideId = UUID.randomUUID().toString();
        int threads = 100;

        ExecutorService executor =
                Executors.newFixedThreadPool(threads);

        // Starting gate: all threads wait here
        CountDownLatch startLatch = new CountDownLatch(1);

        List<Future<Boolean>> results = new ArrayList<>();

        try {

            for (int i = 0; i < threads; i++) {

                Future<Boolean> future = executor.submit(() -> {

                    // Wait until all threads are ready
                    startLatch.await();

                    // Race for the same driver
                    return driverLocationService.tryReserve(
                            9001,
                            rideId
                    );
                });

                results.add(future);
            }

            // Open the gate — all waiting threads can now race
            startLatch.countDown();

            // Collect results
            long winners = 0;

            for (Future<Boolean> future : results) {
                if (future.get()) {
                    winners++;
                }
            }

            System.out.println("Number of winners: " + winners);

            // Exactly one thread should reserve the driver
            assertEquals(
                    1,
                    winners,
                    "Exactly one thread should win reservation"
            );

        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {

            executor.shutdown();

            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    @Test
    void onlyOneThreadWithDifferentRideIdShouldReservceDriver() throws Exception{
        int threads = 100;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);

        List<Future<Ride>> results = new ArrayList<>();

        try{
            for(int i = 0;i<threads;i++){
                results.add(executor.submit(
                        () -> {
                            startLatch.await();
                            RideRequest request = new RideRequest();
                            request.setLatitude(40.0);
                            request.setLongitude(-74.0);
                            return rideService.createRide(request);
                        }
                ));
            }

            startLatch.countDown();

            long matched = 0;
            long noDriver = 0;
            for(Future<Ride> f : results){

                Ride ride = f.get();
                if (String.valueOf(RideStatus.MATCHED).equals(ride.getStatus())) matched++;
                if (String.valueOf(RideStatus.NO_DRIVER_FOUND).equals(ride.getStatus())) noDriver++;

            }
            System.out.println("Matched rides: " + matched);
            System.out.println("No driver rides: " + noDriver);

            assertEquals(1, matched, "Exactly one ride should be matched");
            assertEquals(99, noDriver, "The rest should fail");
        } finally {
            executor.shutdown();
        }
    }


}
