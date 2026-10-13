# 07 — Complete File & Module Reference

This document provides a comprehensive, file-by-file and module-by-module walkthrough of every single source file in the **Voyage** repository.

---

## 🌳 1. Complete Repository Directory Tree

```
voyage/
├── pom.xml                                              # Project dependencies and build configuration
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/akash/voyage/
│   │   │       ├── VoyageApplication.java              # Main Spring Boot entrypoint
│   │   │       ├── component/
│   │   │       │   ├── DriverSimulator.java            # Background GPS updater (5,000 virtual drivers)
│   │   │       │   └── RideRequestedConsumer.java      # Kafka listener for matching & dead-letter topics
│   │   │       ├── config/
│   │   │       │   └── KafkaConfig.java                 # Kafka topic beans, retry policy, error handler
│   │   │       ├── controller/
│   │   │       │   ├── DriverController.java            # REST endpoints for driver coordinates & nearby lookup
│   │   │       │   └── RideController.java              # REST endpoints for ride creation, status, completion & SSE
│   │   │       ├── dto/
│   │   │       │   ├── NearestDriverResponse.java       # DTO containing driver ID and distance in km
│   │   │       │   └── RideRequest.java                 # DTO capturing ride pickup latitude and longitude
│   │   │       ├── enums/
│   │   │       │   └── RideStatus.java                  # State machine enum (REQUESTED, MATCHED, etc.)
│   │   │       ├── exception/
│   │   │       │   ├── InvalidRideStateException.java   # HTTP 400 when completing an un-matched ride
│   │   │       │   └── RideNotFoundException.java       # HTTP 404 when requested ride ID does not exist
│   │   │       ├── model/
│   │   │       │   ├── Location.java                    # POJO representing latitude and longitude
│   │   │       │   └── Ride.java                        # JPA Entity representing the 'rides' database table
│   │   │       ├── repository/
│   │   │       │   └── RideRepository.java              # Spring Data JPA repository with atomic CAS updates
│   │   │       ├── service/
│   │   │       │   ├── DriverLocationService.java       # Redis Geo, liveness heartbeats, SETNX lock & Lua release
│   │   │       │   └── RideService.java                 # Business orchestration, Kafka publishing, SSE management
│   │   │       └── utils/
│   │   │           └── GeoUtils.java                    # Haversine distance formula for Great-Circle distance
│   │   └── resources/
│   │       └── application.yaml                         # Configuration for Postgres, Redis, Kafka, and Hibernate
│   └── test/
│       └── java/
│           └── com/akash/voyage/
│               ├── VoyageApplicationTests.java          # Spring Context verification test
│               └── service/
│                   ├── LoadTest.java                    # 100-thread, 1,000-request HTTP load benchmark
│                   └── RideConcurrencyTest.java         # 1,000-thread race condition & integration test suite
```

---

## 📦 2. Build & Infrastructure Configuration

### 📄 [`pom.xml`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/pom.xml)
- **Purpose**: Defines project metadata, Java 21 toolchain, and Maven dependencies.
- **Key Dependencies**:
  - `spring-boot-starter-webmvc`: Provides Spring MVC REST controllers and embedded Tomcat.
  - `spring-boot-starter-data-redis`: Client integration (Lettuce) for Redis strings, geospatial commands, and Lua scripts.
  - `spring-boot-starter-data-jpa`: Spring Data JPA and Hibernate for relational persistence.
  - `postgresql`: PostgreSQL JDBC driver for database connectivity.
  - `spring-boot-starter-kafka`: Spring Kafka integration for producer templates and consumer containers.
  - `lombok`: Compile-time code generator for getters, setters, constructors, and SLF4J logging.
  - `spring-boot-starter-webmvc-test`: Unit testing and mock MVC support.

### 📄 [`src/main/resources/application.yaml`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/resources/application.yaml)
- **Purpose**: Central configuration file for all external infrastructure.
- **Configured Services**:
  - `spring.datasource`: Connects to PostgreSQL on `localhost:5433` with database `voyage`.
  - `spring.jpa.hibernate.ddl-auto: update`: Automatically manages table schema migrations.
  - `spring.data.redis`: Connects to Redis cache on `localhost:6379`.
  - `spring.kafka.bootstrap-servers`: Points to Kafka broker on `localhost:9092`.
  - `spring.kafka.consumer.group-id`: Consumer group `matching-service`.

---

## 🚀 3. Application Entrypoint & Background Components

### 📄 [`VoyageApplication.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/VoyageApplication.java)
- **Package**: `com.akash.voyage`
- **Annotations**: `@SpringBootApplication`
- **Responsibilities**:
  - Main Java method bootstrapping the Spring application context.
  - Sets the JVM default time zone explicitly to `Asia/Kolkata`.

### 📄 [`component/DriverSimulator.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/component/DriverSimulator.java)
- **Package**: `com.akash.voyage.component`
- **Annotations**: `@Service`, `@RequiredArgsConstructor`, `@Slf4j`
- **Key Method**:
  - `updateDrivers()`: Annotated with `@Scheduled(fixedRate = 2000)`. Loops through 5,000 drivers, calculates randomized coordinates around Chennai (`12.9 + rand*0.1`, `80.2 + rand*0.1`), and calls `driverLocationService.saveLocation(...)` to update Redis geospatial sets and refresh heartbeats every 2 seconds.

### 📄 [`component/RideRequestedConsumer.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/component/RideRequestedConsumer.java)
- **Package**: `com.akash.voyage.component`
- **Annotations**: `@Component`, `@RequiredArgsConstructor`
- **Key Methods**:
  - `listen(String rideId)`: Annotated with `@KafkaListener(topics = "ride.requested", groupId = "matching-service", concurrency = "3")`. Pulls ride IDs from the queue and triggers `rideService.processRide(rideId)`.
  - `handleFailedRide(String rideId)`: Annotated with `@KafkaListener(topics = "ride.requested-dlt")`. Catches unrecoverable errors sent to the Dead Letter Topic, sets ride status to `FAILED`, and notifies the waiting client via SSE.

---

## ⚙️ 4. Configuration

### 📄 [`config/KafkaConfig.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/config/KafkaConfig.java)
- **Package**: `com.akash.voyage.config`
- **Annotations**: `@Configuration`
- **Key Beans**:
  - `rideRequestedTopic()`: Creates topic `ride.requested` with 3 partitions and 1 replica.
  - `rideRequestedDLT()`: Creates Dead Letter Topic `ride.requested-dlt` with 3 partitions and 1 replica.
  - `errorHandler(...)`: Creates `DefaultErrorHandler` pairing `DeadLetterPublishingRecoverer` with `FixedBackOff(1000L, 3)` for 3 retries spaced 1 second apart.

---

## 🌐 5. REST Controllers

### 📄 [`controller/DriverController.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/controller/DriverController.java)
- **Package**: `com.akash.voyage.controller`
- **Base Path**: `/drivers`
- **Endpoints**:
  - `POST /{id}/location`: Receives `{latitude, longitude}`, validates ranges (-85.05 to +85.05 lat, -180 to +180 lon), and saves to Redis. Returns `200 OK` or `400 Bad Request`.
  - `GET /{id}/location`: Returns current cached `Location` from Redis or `404 Not Found`.
  - `GET /nearby?lat=..&lon=..&radiusKm=..`: Queries alive drivers within radius and returns a list of `NearestDriverResponse` sorted closest-first.

### 📄 [`controller/RideController.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/controller/RideController.java)
- **Package**: `com.akash.voyage.controller`
- **Base Path**: `/rides`
- **Endpoints**:
  - `POST /`: Validates coordinates, calls `rideService.createRide(request)`, and returns `HTTP 202 Accepted` with initial `Ride` entity.
  - `GET /{id}`: Returns current state of ride from PostgreSQL or `404 Not Found`.
  - `POST /{id}/complete`: Completes ride, sets status to `COMPLETED`, releases driver lock via Lua script, and returns updated `Ride`.
  - `GET /{id}/events`: Registers an `SseEmitter` streaming live matching events to the rider's client.

---

## 💼 6. Services & Business Logic

### 📄 [`service/DriverLocationService.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/service/DriverLocationService.java)
- **Package**: `com.akash.voyage.service`
- **Key Methods**:
  - `saveLocation(id, lat, lon)`: Adds coordinate to Redis Geo sorted set `drivers` and sets key `driver:{id}:alive = "1"` with 2-minute TTL.
  - `getLocation(id)`: Reads driver's point from Redis Geo index.
  - `findNearby(lat, lon, radiusKm)`: Executes `opsForGeo().radius(...)` with `sortAscending()`, filters results against `driver:{id}:alive`, and maps to `NearestDriverResponse`.
  - `tryReserve(driverId, rideId)`: Calls Redis `setIfAbsent("driver:{id}:busy", rideId, 5 minutes)`. Includes idempotency check returning `true` if current lock owner already matches `rideId`.
  - `release(driverId, rideId)`: Executes atomic Lua script to delete `driver:{id}:busy` only if current value equals `rideId`.

### 📄 [`service/RideService.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/service/RideService.java)
- **Package**: `com.akash.voyage.service`
- **Key Methods**:
  - `createRide(request)`: Generates UUID, saves `Ride` to PostgreSQL as `REQUESTED`, publishes event to Kafka topic `ride.requested`, and returns the ride.
  - `processRide(rideId)`: Background matching logic invoked by Kafka consumer. Queries nearby alive drivers from Redis, loops through candidates trying to reserve via `tryReserve`, performs atomic CAS state update in PostgreSQL (`RideRepository.updateStatus`), and notifies client via SSE.
  - `completeRide(rideId)`: Validates that ride status is `MATCHED`, updates status to `COMPLETED` in PostgreSQL, and releases Redis driver lock.
  - `registerEmitter(rideId)`: Creates and registers `SseEmitter` with 30s timeout; pushes status immediately if ride already finished.
  - `notifyAndClose(rideId, ride)`: Safely removes `SseEmitter`, sends SSE event `"ride-status"`, and completes the connection.
  - `getRide(id)`: Fetches `Ride` by ID from PostgreSQL.

---

## 🗄️ 7. Persistence & Data Models

### 📄 [`model/Ride.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/model/Ride.java)
- **Package**: `com.akash.voyage.model`
- **Annotations**: `@Entity`, `@Table(name = "rides")`, `@Getter`, `@Setter`, `@AllArgsConstructor`, `@NoArgsConstructor`
- **Fields**:
  - `id` (String): UUID primary key (`@Id`).
  - `pickupLatitude`, `pickupLongitude` (Double): Mandatory pickup coordinates.
  - `driverId` (Integer): ID of reserved driver.
  - `status` (RideStatus): Enum stored as string.
  - `createdAt`, `updatedAt` (Instant): Auditing timestamps; `@PreUpdate` hook automatically updates `updatedAt`.

### 📄 [`repository/RideRepository.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/repository/RideRepository.java)
- **Package**: `com.akash.voyage.repository`
- **Inheritance**: `extends JpaRepository<Ride, String>`
- **Key Query**:
  ```java
  @Modifying
  @Query("UPDATE Ride r SET r.status = :newStatus WHERE r.id = :rideId AND r.status = :expectedStatus")
  int updateStatus(@Param("rideId") String rideId,
                   @Param("expectedStatus") RideStatus expectedStatus,
                   @Param("newStatus") RideStatus newStatus);
  ```
  Provides atomic Compare-And-Set (CAS) state transition at the database level.

### 📄 [`model/Location.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/model/Location.java)
- **Package**: `com.akash.voyage.model`
- **Fields**: `Double latitude`, `Double longitude`. Simple POJO for coordinate transfer.

---

## 📬 8. DTOs, Enums & Exceptions

### 📄 [`dto/RideRequest.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/dto/RideRequest.java)
- Captures `Double latitude` and `Double longitude` sent in `POST /rides`.

### 📄 [`dto/NearestDriverResponse.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/dto/NearestDriverResponse.java)
- Carries `Integer id` (driver ID) and `Double distance` (in kilometers rounded to 2 decimal places).

### 📄 [`enums/RideStatus.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/enums/RideStatus.java)
- Enum constants:
  - `REQUESTED`: Initial state after creation.
  - `MATCHED`: Driver found, locked, and assigned.
  - `NO_DRIVER_FOUND`: No alive driver available within 5 km.
  - `COMPLETED`: Ride finished; driver unlocked.
  - `FAILED`: Unrecoverable error routed through Kafka DLT.

### 📄 [`exception/InvalidRideStateException.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/exception/InvalidRideStateException.java)
- Annotated with `@ResponseStatus(HttpStatus.BAD_REQUEST)`. Thrown when attempting to complete a ride that is not in `MATCHED` state.

### 📄 [`exception/RideNotFoundException.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/exception/RideNotFoundException.java)
- Annotated with `@ResponseStatus(HttpStatus.NOT_FOUND)`. Thrown when an endpoint requests a non-existent ride ID.

---

## 🧮 9. Utilities

### 📄 [`utils/GeoUtils.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/utils/GeoUtils.java)
- **Method**: `distanceKm(lat1, lon1, lat2, lon2)`
- **Mathematical Formula**: Implements the **Haversine formula** to calculate the Great-Circle distance over Earth's curvature (Earth radius = 6,371 km). Useful for independent distance verification and fallback calculations.

---

## 🧪 10. Test Suite

### 📄 [`src/test/java/com/akash/voyage/service/RideConcurrencyTest.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/test/java/com/akash/voyage/service/RideConcurrencyTest.java)
- **Annotations**: `@SpringBootTest`
- **Key Tests**:
  - `onlyOneThreadShouldReserveDriver()`: Spawns 1,000 threads blocked on a `CountDownLatch`. Releases all 1,000 threads simultaneously to race for Driver 9001. Asserts that **exactly 1 thread** wins and 999 threads fail.
  - `onlyOneThreadWithDifferentRideIdShouldReserveDriver()`: Fires 1,000 concurrent ride requests through `rideService.createRide()`, waits via `Awaitility` for Kafka consumption, and asserts that **exactly 1 ride is MATCHED** and **999 rides become NO_DRIVER_FOUND**.
  - `testSafeRelease()`: Verifies that releasing a lock with the wrong `rideId` fails and preserves the lock, while releasing with the correct `rideId` deletes the key.

### 📄 [`src/test/java/com/akash/voyage/service/LoadTest.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/test/java/com/akash/voyage/service/LoadTest.java)
- Standalone load-testing benchmark using `ExecutorService` (100 threads) and `HttpClient` to fire 1,000 HTTP POST requests to `http://localhost:8080/rides`, recording status codes and millisecond latencies.

### 📄 [`src/test/java/com/akash/voyage/VoyageApplicationTests.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/test/java/com/akash/voyage/VoyageApplicationTests.java)
- Verifies that the Spring application context loads without dependency injection or configuration errors.
