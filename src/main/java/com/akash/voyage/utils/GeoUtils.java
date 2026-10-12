package com.akash.voyage.utils;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class GeoUtils {
    public static double Radius = 6371;

    public static double distanceKm(double lat1, double lon1, double lat2, double lon2){
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLng / 2) * Math.sin(dLng / 2);

        double c = 2 * Math.asin(Math.sqrt(a));
        return Radius * c;
    }

    public static void main(String[] args) {

    }

}
