package com.akash.voyage.exception;

import com.akash.voyage.enums.RideStatus;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class InvalidRideStateException extends RuntimeException {
    public InvalidRideStateException(String id, RideStatus status) {
        super("Ride " + id + " is not in a state to be completed. Current status: " + status);
    }
}