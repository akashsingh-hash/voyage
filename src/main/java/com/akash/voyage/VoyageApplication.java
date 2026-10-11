package com.akash.voyage;

import com.akash.voyage.service.DriverLocationService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class VoyageApplication {

	public static void main(String[] args) {
		SpringApplication.run(VoyageApplication.class, args);
	}

}
