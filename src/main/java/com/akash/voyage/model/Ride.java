package com.akash.voyage.model;

import com.akash.voyage.enums.RideStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "rides")
public class Ride {

    @Id
    private String id;

    @Column(nullable = false)
    private Double pickupLatitude;

    @Column(nullable = false)
    private Double pickupLongitude;

    @Column
    private Integer driverId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private RideStatus status;

    @Column(nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    @Column
    private Instant updatedAt = Instant.now();

    @PreUpdate
    public void preUpdate() {
        updatedAt = Instant.now();
    }
}

