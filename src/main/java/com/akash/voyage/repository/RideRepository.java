package com.akash.voyage.repository;

import com.akash.voyage.enums.RideStatus;
import com.akash.voyage.model.Ride;
import org.springframework.data.repository.query.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

public interface RideRepository extends JpaRepository<Ride, String> {

    @Modifying
    @Query("""
    UPDATE Ride r
    SET r.status = :newStatus
    WHERE r.id = :rideId
      AND r.status = :expectedStatus
    """)
    int updateStatus(
            @Param("rideId") String rideId,
            @Param("expectedStatus") RideStatus expectedStatus,
            @Param("newStatus") RideStatus newStatus
    );

    @Modifying
    @Query("""
    UPDATE Ride r
    SET r.status = :requested
    WHERE r.id = :rideId
      AND r.status = :failed
    """)
    int recoverFailedRide(
            @Param("rideId") String rideId,
            @Param("failed") RideStatus failed,
            @Param("requested") RideStatus requested
    );


}
