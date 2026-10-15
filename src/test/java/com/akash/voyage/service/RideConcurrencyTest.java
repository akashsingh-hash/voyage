package com.akash.voyage.service;

import com.akash.voyage.dto.RideRequest;
import com.akash.voyage.enums.RideStatus;
import com.akash.voyage.model.Ride;
import com.akash.voyage.repository.RideRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.*;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class RideConcurrencyTest {

    static {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Kolkata"));
    }

    @Autowired
    private DriverLocationService driverLocationService;

    @Autowired
    private RideService rideService;

    @Autowired
    private RideRepository rideRepository;

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
    void destroy() {
        redisTemplate.delete("driver:9001:busy");
        redisTemplate.delete("driver:9001:alive");
        redisTemplate.opsForGeo().remove("drivers", "9001");
    }

    @Test
    void onlyOneThreadShouldReserveDriver() throws InterruptedException {

        String rideId = UUID.randomUUID().toString();
        int threads = 50;

        ExecutorService executor = Executors.newFixedThreadPool(threads);

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
                            rideId);
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
                    "Exactly one thread should win reservation");

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
    void onlyOneThreadWithDifferentRideIdShouldReserveDriver() throws Exception {

        int requests = 50;
        int workers = 20;

        ExecutorService executor = Executors.newFixedThreadPool(workers);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<Ride>> results = new ArrayList<>();

        try {
            for (int i = 0; i < requests; i++) {
                results.add(executor.submit(() -> {
                    startLatch.await();

                    RideRequest request = new RideRequest();
                    request.setLatitude(40.0);
                    request.setLongitude(-74.0);

                    return rideService.createRide(request);
                }));
            }

            // Release all workers to submit requests concurrently.
            startLatch.countDown();
            System.out.println(
                    "Busy key before collecting results: "
                            + redisTemplate.opsForValue().get("driver:9001:busy"));

            List<String> rideIds = new ArrayList<>();

            for (Future<Ride> future : results) {
                Ride ride = future.get(15, TimeUnit.SECONDS);
                rideIds.add(ride.getId());
            }

            // Read the persisted statuses, not the stale objects
            // returned immediately by createRide().
            await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .until(() -> rideIds.stream().allMatch(id -> rideRepository.findById(id)
                            .map(ride -> ride.getStatus() != RideStatus.REQUESTED)
                            .orElse(false)));

            List<Ride> processedRides = rideIds.stream()
                    .map(id -> rideRepository.findById(id).orElseThrow())
                    .toList();

            long matched = processedRides.stream()
                    .filter(ride -> ride.getStatus() == RideStatus.MATCHED)
                    .count();

            long noDriver = processedRides.stream()
                    .filter(ride -> ride.getStatus() == RideStatus.NO_DRIVER_FOUND)
                    .count();

            System.out.println("Total requests: " + requests);
            System.out.println("Matched rides: " + matched);
            System.out.println("No-driver rides: " + noDriver);

            assertEquals(requests, matched + noDriver,
                    "Every ride should finish processing");

            assertEquals(1, matched,
                    "Only one ride should reserve the single test driver");

        } finally {
            executor.shutdownNow();

            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                System.err.println("Executor did not terminate in time");
            }
        }
    }

    @Test
    void testSafeRelease() {
        // Arrange driver9001 busy with ride-A
        redisTemplate.opsForValue().set("driver:9001:busy", "ride-A", 600, TimeUnit.SECONDS);

        // Act + Assert
        assertFalse(driverLocationService.release(9001, "ride-B")); // wrong ride, key not deleted
        assertEquals("ride-A", redisTemplate.opsForValue().get("driver:9001:busy"));

        assertTrue(driverLocationService.release(9001, "ride-A")); // correct ride, key deleted
        assertNull(redisTemplate.opsForValue().get("driver:9001:busy"));

        assertFalse(driverLocationService.release(9001, "ride-A")); // Repeat call

    }

    // @Test
    // void testIdempotentTryReserve() {
    // String rideId = UUID.randomUUID().toString();
    //
    // // First reservation attempt
    // boolean first = driverLocationService.tryReserve(9001, rideId);
    // assertTrue(first, "First reservation should succeed");
    //
    // // Simulate Kafka redelivery: processRide runs again
    // Ride ride = new Ride();
    // ride.setId(rideId);
    // ride.setPickupLatitude(40.0);
    // ride.setPickupLongitude(-74.0);
    // ride.setStatus(RideStatus.REQUESTED);
    //
    // // Save ride before processing
    // rideService.getRides().put(rideId, ride);
    //
    // // Second reservation attempt (same rideId, same driver)
    // rideService.processRide(rideId);
    //
    // // Assert idempotency: ride is still matched with driver 9001
    // Ride processed = rideService.getRide(rideId);
    // assertEquals(RideStatus.MATCHED, processed.getStatus(), "Ride should remain
    // MATCHED");
    // assertEquals(9001, processed.getDriverId(), "Ride should stay with driver
    // 9001");
    // }

}
