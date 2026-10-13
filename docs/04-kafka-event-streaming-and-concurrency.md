# 04 — Kafka Event Streaming & Concurrency

---

## 🌪️ 1. Why Apache Kafka in Voyage?

Consider what happens if you handle ride matching **synchronously** inside the HTTP request thread:

```
Rider App ---POST /rides---> Tomcat Thread #1 (Wait 800ms for Redis Geo + Locks)
Rider App ---POST /rides---> Tomcat Thread #2 (Wait 600ms)
...
Rider App ---POST /rides---> Tomcat Thread #200 (Tomcat Thread Pool Exhausted!)
Rider App ---POST /rides---> 💥 HTTP 504 GATEWAY TIMEOUT / CONNECTION REFUSED
```

By default, an embedded Tomcat server has a fixed thread pool (typically 200 worker threads). If spatial searches or database transactions take 500 ms under peak load, **just 200 concurrent users will bring down the entire web server**.

### The Kafka Solution:
1. When a rider requests a ride, the HTTP controller creates the database record and pushes the `rideId` to Kafka in **under 10 milliseconds**.
2. The controller immediately responds with `HTTP 202 Accepted`.
3. The HTTP connection is freed instantly.
4. Dedicated Kafka consumer worker threads pull messages from Kafka partitions and execute matching in the background at their own controlled pace.
5. If 10,000 requests arrive in 1 second, Kafka buffers them safely on disk. Zero dropped requests, zero thread starvation!

---

## ⚙️ 2. Topic Configuration & Partitioning (`KafkaConfig.java`)

In [`KafkaConfig.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/config/KafkaConfig.java):

```java
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic rideRequestedTopic() {
        return TopicBuilder.name("ride.requested")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic rideRequestedDLT() {
        return TopicBuilder.name("ride.requested-dlt")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
```

### Why 3 Partitions?
In Kafka, **the partition is the fundamental unit of parallelism**.
- A topic with 1 partition can only ever be consumed by **1 consumer thread** at a time within a consumer group. Even if you have 10 servers, 9 servers will sit idle!
- By creating **3 partitions**, up to **3 consumer threads** can process ride requests in true parallel execution.

---

## 🧵 3. Multi-Threaded Consumer Concurrency (`RideRequestedConsumer.java`)

In [`RideRequestedConsumer.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/component/RideRequestedConsumer.java):

```java
@Component
@RequiredArgsConstructor
public class RideRequestedConsumer {

    private final RideService rideService;

    @KafkaListener(topics = "ride.requested", groupId = "matching-service", concurrency = "3")
    public void listen(String rideId) {
        rideService.processRide(rideId);
    }
}
```

### How `concurrency = "3"` works under the hood:
Spring Kafka instantiates a `ConcurrentMessageListenerContainer` with 3 separate internal worker threads.
- Thread 1 listens exclusively to **Partition 0**.
- Thread 2 listens exclusively to **Partition 1**.
- Thread 3 listens exclusively to **Partition 2**.

Each thread independently pulls batches of ride requests and executes `rideService.processRide(rideId)`, tripling our background matching throughput!

---

## 🛟 4. Error Handling, Retries & Dead Letter Topic (DLT)

Distributed systems experience transient errors: temporary database deadlocks, network timeouts, or intermittent Redis disconnections.

In [`KafkaConfig.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/config/KafkaConfig.java#L31-L37):

```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
    FixedBackOff backOff = new FixedBackOff(1000L, 3); // 3 retries, 1s apart
    return new DefaultErrorHandler(recoverer, backOff);
}
```

### The Retry & Recovery Lifecycle:
1. If `rideService.processRide(rideId)` throws an unhandled exception, `DefaultErrorHandler` intercepts it.
2. It waits **1,000 ms** (1 second) and retries processing the message.
3. It repeats this up to **3 attempts**.
4. If all 3 attempts fail (poison pill message or permanent infrastructure failure), `DeadLetterPublishingRecoverer` automatically publishes the message to the Dead Letter Topic: `ride.requested-dlt`.

### Handling the Dead Letter Event:
In [`RideRequestedConsumer.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/component/RideRequestedConsumer.java#L21-L28):

```java
@KafkaListener(topics = "ride.requested-dlt")
public void handleFailedRide(String rideId) {
    Ride ride = rideService.getRide(rideId);
    if (ride != null) {
        ride.setStatus(RideStatus.FAILED);
        rideService.notifyAndClose(rideId, ride);
    }
}
```
**Why this matters**:
Without DLT handling, a failed ride would leave the rider's mobile phone hanging on a loading screen indefinitely! 
With Voyage's DLT listener, the system catches the failure, updates the database status to `FAILED`, and pushes a notification to the client via SSE so their screen displays an alert: *"Matching failed, please retry"*.

---

## 📡 5. Real-Time Streaming: Server-Sent Events (SSE) vs WebSockets

Once matching completes asynchronously in Kafka, how does the rider find out?
Clients connect to `GET /rides/{id}/events`.

### In [`RideService.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/service/RideService.java#L125-L156):
```java
private final ConcurrentHashMap<String, SseEmitter> emitterConcurrentHashMap = new ConcurrentHashMap<>();

public SseEmitter registerEmitter(String rideId) {
    Ride ride = rideRepository.findById(rideId).orElseThrow(() -> new RideNotFoundException(rideId));
    SseEmitter emitter = new SseEmitter(30_000L); // 30 second timeout

    emitter.onCompletion(() -> emitterConcurrentHashMap.remove(rideId, emitter));
    emitter.onTimeout(() -> emitterConcurrentHashMap.remove(rideId, emitter));
    emitter.onError(e -> emitterConcurrentHashMap.remove(rideId, emitter));

    emitterConcurrentHashMap.put(rideId, emitter);

    // If ride already matched or completed before listener connected, send immediately!
    if (ride.getStatus() != RideStatus.REQUESTED) {
        notifyAndClose(rideId, ride);
    }
    return emitter;
}
```

### Why Server-Sent Events (SSE) instead of WebSockets?
| Feature | WebSockets | Server-Sent Events (SSE) | Why Voyage Chose SSE |
| :--- | :--- | :--- | :--- |
| **Communication Direction** | Bi-directional (Client <-> Server) | Unidirectional (Server -> Client) | Ride matching is 100% server-to-client push (the client just needs updates). |
| **Protocol** | Custom `ws://` protocol (Requires handshake upgrade) | Standard HTTP (`text/event-stream`) | Works through corporate proxies, firewalls, and standard HTTP/2 multiplexing. |
| **Complexity & Overhead** | Heavy: Requires heartbeat ping/pongs, binary framing | Lightweight: Built into standard browser/mobile HTTP clients natively. |
| **Reconnection** | Requires custom client code | Built-in native browser automatic reconnection |

---

## 🎤 6. How to Explain Kafka to an Interviewer

> **Interviewer**: *"Why did you integrate Kafka? Wouldn't a simple thread pool or synchronous controller be enough?"*

> **Your Answer**:
> *"In a ride-hailing architecture, coupling the HTTP request lifecycle to spatial searches and driver locking causes severe thread pool starvation during traffic spikes.
>
> We introduced Apache Kafka to decouple the write ingestion from matching execution. The `/rides` endpoint publishes a ride event and returns `HTTP 202 Accepted` within 10 milliseconds.
>
> We configured the `ride.requested` topic with 3 partitions and configured the consumer container with concurrency = 3 so that 3 parallel worker threads process events across partitions without idling.
>
> For reliability, we implemented Spring Kafka's `DefaultErrorHandler` with a 3-retry FixedBackOff and a DeadLetterPublishingRecoverer. If a poisoned message repeatedly fails, it routes to `ride.requested-dlt`, where a dedicated listener marks the ride as FAILED and notifies the waiting client via Server-Sent Events (SSE). This guarantees zero hung connections."*
