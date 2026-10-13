package com.akash.voyage.service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadTest {

    public static void main(String[] args) throws Exception {
        int totalRequests = 1000;       // number of ride requests
        int threadPoolSize = 100;       // concurrent threads

        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        HttpClient client = HttpClient.newHttpClient();

        for (int i = 0; i < totalRequests; i++) {
            int requestId = i;
            executor.submit(() -> {
                try {
                    String body = "{\"latitude\":12.9,\"longitude\":80.2}";
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:8080/rides"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();

                    long start = System.nanoTime();
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    long latency = (System.nanoTime() - start) / 1_000_000;

                    System.out.printf("Request %d -> Status: %d, Latency: %d ms%n",
                            requestId, response.statusCode(), latency);
                } catch (Exception e) {
                    System.err.println("Request " + requestId + " failed: " + e.getMessage());
                }
            });
        }

        executor.shutdown();
    }
}
