# 02 — Database & PostgreSQL Integration

---

## 💡 1. The Story: Why Did We Add PostgreSQL?

When Voyage was initially prototyped, ride state was stored in an in-memory Java structure:
```java
// The old approach (Before PostgreSQL):
private final ConcurrentHashMap<String, Ride> rides = new ConcurrentHashMap<>();
```

While `ConcurrentHashMap` was fast for local development, it suffered from **three critical production flaws**:

### Flaw 1: The Crash Disaster (Zero Durability)
If the server machine restarts, crashes, or deploys a new release, all data in JVM RAM vanishes. Riders would lose ongoing trips, drivers wouldn't get paid, and trip history would be permanently wiped out.

### Flaw 2: The Multi-Instance Nightmare (No Distributed State)
In production, a ride-hailing service runs on multiple pods/servers behind a load balancer (e.g., Server A, Server B, Server C).
- Rider creates a ride on **Server A** -> stored in Server A's RAM.
- Rider later checks ride status on **Server B** -> Server B checks its own RAM, finds nothing, and throws `404 Not Found`!
- Memory is isolated per process.

### Flaw 3: Inability to Perform Auditing and Complex Analytics
You cannot run SQL queries like:
- *"How many rides completed in the last hour?"*
- *"What is the average distance and matching time per driver?"*
- *"Generate daily driver earnings for tax and payouts."*

### The Solution:
We introduced **PostgreSQL** via **Spring Data JPA / Hibernate** to act as the durable, transactional source of truth for all ride records.

---

## ⚖️ 2. Polyglot Persistence: Why PostgreSQL AND Redis?

Interviewers love asking:
> *"Why did you use both PostgreSQL and Redis? Why didn't you just use PostgreSQL for everything, or Redis for everything?"*

Here is the exact comparison to explain:

| Database | What We Use It For in Voyage | Why It Excels Here | Why Not Use the Other? |
| :--- | :--- | :--- | :--- |
| **PostgreSQL** (Relational / Disk) | • Ride details & history<br>• Ride status state transitions<br>• Auditing & timestamps | • ACID transaction guarantees<br>• Data durability across crashes<br>• Complex relational querying | Redis RAM is expensive. Storing millions of historical completed trips in RAM is wasteful and risky. |
| **Redis** (In-Memory Key-Value) | • Driver GPS locations (`GEOADD`)<br>• Radial lookups (`GEORADIUS`)<br>• Heartbeats (TTL: 120s)<br>• Mutex locks (`SETNX` TTL: 300s) | • Sub-millisecond read/write<br>• Built-in geospatial algorithms<br>• Native key expiry (TTLs) | PostgreSQL geospatial extensions (PostGIS) require disk I/O and index recalculations. 5,000 drivers updating GPS every 2s would saturate disk I/O. |

**The Architecture Principle**:
- **Redis** handles transient, high-velocity, ephemeral data (GPS pings, live locks).
- **PostgreSQL** handles long-term, critical business records (rides, billing, state transitions).

---

## 🗄️ 3. Database Schema & The `Ride` Entity

The `Ride` model ([`Ride.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/model/Ride.java)) is mapped to the PostgreSQL table `rides`:

```java
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "rides")
public class Ride {

    @Id
    private String id;                                // UUID String primary key

    @Column(nullable = false)
    private Double pickupLatitude;                    // Rider's pickup latitude

    @Column(nullable = false)
    private Double pickupLongitude;                   // Rider's pickup longitude

    @Column
    private Integer driverId;                         // ID of matched driver (null until MATCHED)

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private RideStatus status;                        // REQUESTED, MATCHED, etc.

    @Column(nullable = false, updatable = false)
    private Instant createdAt = Instant.now();        // Audit timestamp of creation

    @Column
    private Instant updatedAt = Instant.now();        // Timestamp of last update

    @PreUpdate
    public void preUpdate() {
        updatedAt = Instant.now();                    // Automatically refreshes before DB update
    }
}
```

### Corresponding PostgreSQL DDL:
```sql
CREATE TABLE rides (
    id VARCHAR(255) PRIMARY KEY,
    pickup_latitude DOUBLE PRECISION NOT NULL,
    pickup_longitude DOUBLE PRECISION NOT NULL,
    driver_id INTEGER,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE
);
```

---

## 🛡️ 4. The Compare-And-Set (CAS) Update in `RideRepository`

Look at [`RideRepository.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/repository/RideRepository.java):

```java
public interface RideRepository extends JpaRepository<Ride, String> {

    @Modifying
    @Query("UPDATE Ride r SET r.status = :newStatus WHERE r.id = :rideId AND r.status = :expectedStatus")
    int updateStatus(@Param("rideId") String rideId,
                     @Param("expectedStatus") RideStatus expectedStatus,
                     @Param("newStatus") RideStatus newStatus);
}
```

### Why is this query so critical? (The Double-Update Bug)

Imagine Kafka accidentally delivers a ride message twice to two consumer worker threads (Thread 1 and Thread 2) simultaneously:

#### Without CAS (Naïve approach):
```java
// DANGEROUS CODE:
Ride ride = rideRepository.findById(rideId).get();
ride.setStatus(RideStatus.MATCHED);
rideRepository.save(ride); // Both threads blindly overwrite!
```
If both threads execute this, Thread 2 could overwrite a ride that was already cancelled or matched with another driver.

#### With CAS (`updateStatus`):
We pass the `expectedStatus`:
```sql
UPDATE rides 
SET status = 'MATCHED' 
WHERE id = 'ride-123' 
  AND status = 'REQUESTED';
```

1. **Thread 1** executes the SQL. In PostgreSQL, `status` is currently `'REQUESTED'`. The `WHERE` condition matches. The row is updated. PostgreSQL returns **`1`** (1 row modified).
2. **Thread 2** executes the exact same query 2 milliseconds later. In PostgreSQL, the row's `status` is now already `'MATCHED'`. The condition `status = 'REQUESTED'` evaluates to **FALSE**.
3. PostgreSQL updates **0 rows** and returns **`0`**.

In [`RideService.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/service/RideService.java#L103-L113):
```java
int updated = rideRepository.updateStatus(rideId, RideStatus.REQUESTED, RideStatus.MATCHED);
if (updated == 1) {
    // Only one thread wins the update
    ride.setDriverId(matchedDriver);
    rideRepository.save(ride);
    log.info("Ride {} matched with driver {}", rideId, matchedDriver);
    notifyAndClose(rideId, ride);
} else {
    // Another thread already updated this ride
    log.warn("Ride {} already updated by another thread", rideId);
}
```

**Interview Value**: This is **Database-Level Optimistic Concurrency Control (Compare-And-Set)**. It eliminates race conditions at the database layer without expensive pessimistic table locks!

---

## ⚙️ 5. PostgreSQL Configuration in `application.yaml`

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5433/voyage
    username: voyage
    password: voyage123
  jpa:
    hibernate:
      ddl-auto: update
    show-sql: true
    properties:
      hibernate:
        format_sql: true
```

### Explaining the settings:
- **`port: 5433`**: Uses custom port 5433 (often used when Docker maps `5433:5432` to avoid conflicts with a default local Postgres service on 5432).
- **`ddl-auto: update`**: Hibernate automatically creates or modifies the table schema based on the `@Entity` annotations when the application boots up.
- **`show-sql: true` & `format_sql: true`**: Prints formatted SQL statements to the console log, making it easy to observe queries during testing and debugging.

---

## 🎤 6. How to Explain PostgreSQL to the Interviewer

> **Interviewer**: *"Tell me about the persistence layer. How do you store ride data?"*

> **Your Answer**:
> *"Originally, the prototype used an in-memory ConcurrentHashMap, but that lacked durability across restarts, couldn't scale horizontally across multiple application pods, and didn't support transactional queries.*
>
> *We introduced PostgreSQL as our durable source of truth using Spring Data JPA. We designed a clean `Ride` entity with immutable UUID identifiers, spatial pickup coordinates, and timestamps.*
>
> *To protect against race conditions where multiple Kafka consumer threads process duplicate messages, we implemented an atomic Compare-And-Set (CAS) update in `RideRepository`: `UPDATE rides SET status = :newStatus WHERE id = :id AND status = :expectedStatus`. Only the thread that successfully transitions the row from `REQUESTED` to `MATCHED` receives an update count of 1 and assigns the driver. The other thread receives 0 and safely backs off.*
>
> *We use a polyglot persistence strategy: Redis handles high-frequency ephemeral data like driver GPS coordinates and distributed locks, while PostgreSQL handles durable business entities and transactional consistency."*
