package com.akash.voyage.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Ride {
    private String id;
    private Integer driverId; // null until matched
    private Double pickupLatitude;
    private Double pickupLongitude;
    private String status;
}

