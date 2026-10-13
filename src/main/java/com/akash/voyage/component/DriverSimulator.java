package com.akash.voyage.component;

import com.akash.voyage.service.DriverLocationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.stream.IntStream;


@Service
@RequiredArgsConstructor
@Slf4j
public class DriverSimulator {

    private final DriverLocationService driverLocationService;

    @Scheduled(fixedRate = 5000)
    public void updateDrivers() {
        int totalDrivers = 5000;

        IntStream.rangeClosed(1, totalDrivers).parallel().forEach(i -> {
            double lat = 12.9 + Math.random() * 0.1;
            double lon = 80.2 + Math.random() * 0.1;
            driverLocationService.saveLocation(i, lat, lon);
        });

        log.info("Updated locations for {} drivers", totalDrivers);
    }

}
