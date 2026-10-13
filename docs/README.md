# 📚 Voyage Documentation Suite

Welcome to the complete, developer-friendly and interview-ready documentation suite for **Voyage — Distributed Real-Time Ride Matching Service**.

Whether you are preparing to explain this architecture to an interviewer, onboarding a fellow engineer, or reviewing how each component interacts under high concurrency, this documentation breaks down every concept from first principles into plain, accessible language with production-grade depth.

---

## 🗂️ Table of Contents

| Document | Topic & Focus Area | Key Concepts Covered |
| :--- | :--- | :--- |
| **[01. System Architecture & Flows](./01-architecture-and-system-design.md)** | End-to-end design & data lifecycles | Request-response flow, Async Kafka queueing, Redis Geo search, SSE live streaming, End-to-end Mermaid sequence diagrams. |
| **[02. Database & PostgreSQL Integration](./02-database-and-postgresql.md)** | Durable storage & ACID persistence | Why PostgreSQL replaced in-memory storage, JPA `@Entity`, Compare-And-Set (CAS) conditional SQL update, Polyglot persistence. |
| **[03. Redis Geospatial & Distributed Locking](./03-redis-geospatial-and-distributed-locking.md)** | In-memory indexing & race-condition safety | Redis `GEOADD`/`GEORADIUS`, Heartbeat liveness (`driver:{id}:alive`), Distributed lock (`SETNX`), Atomic Lua lock release. |
| **[04. Kafka Event Streaming & Concurrency](./04-kafka-event-streaming-and-concurrency.md)** | Asynchronous ingestion & message delivery | Topic partitioning (3 partitions), Consumer concurrency (3 worker threads), Retries with FixedBackOff, Dead Letter Topic (DLT). |
| **[05. Driver GPS Simulation Guide](./05-driver-simulation-guide.md)** | Background traffic generation | `DriverSimulator.java` breakdown, `@Scheduled` periodic updates, 5,000 drivers in Chennai, Redis Geo updates, `@EnableScheduling`. |
| **[06. Load Testing & Benchmarking](./06-load-testing-and-benchmarking.md)** | Stress-testing under heavy concurrency | `LoadTest.java` breakdown, 1,000 requests across 100 concurrent threads, HTTP 202 latency metrics, Why async architecture wins. |
| **[07. Complete File & Module Reference](./07-complete-file-and-module-reference.md)** | Codebase directory walkthrough | Line-by-line and file-by-file analysis of every controller, service, repository, model, DTO, enum, exception, and config. |
| **[08. Interview Cheat Sheet & Q&A](./08-interview-cheat-sheet-and-qa.md)** | Interview masterclass | 2-minute elevator pitch, Deep-dive Q&A (Double booking, Split-brain, Redis vs Postgres, System bottlenecks, Tradeoffs). |

---

## 💡 Quick Overview: What is Voyage?

**Voyage** is a distributed, event-driven backend service modeled after real-world ride-hailing platforms like **Uber, Lyft, and Grab**. 

### The Core Problem Voyage Solves:
1. **High Volume of Requests**: Thousands of riders request rides at the exact same second.
2. **Dynamic Supply**: Thousands of drivers are constantly moving across a city, sending GPS coordinates every few seconds.
3. **The Race Condition (Double-Booking)**: If 50 riders request a ride near the same intersection, only **one** rider can get the nearest driver. Two riders must never be assigned to the same driver.
4. **Resilience & Responsiveness**: The rider's mobile app must never hang or time out waiting for spatial search queries. The API responds instantly (`HTTP 202 Accepted`), matches drivers asynchronously in the background, and streams the status in real time via **Server-Sent Events (SSE)**.

---

## 🧩 Architectural Topology

```
+-----------------------------------------------------------------------------------------+
|                                    CLIENT APPLICATIONS                                  |
|   Rider App (Booking & SSE Stream)                    Driver App / Simulation           |
+---------------------------+---------------------------------------+---------------------+
                            |                                       |
                   HTTP POST /rides                        HTTP POST /drivers/{id}/location
                   GET /rides/{id}/events (SSE)                             |
                            v                                       v
+-----------------------------------------------------------------------------------------+
|                                  VOYAGE SPRING BOOT SERVICE                             |
|                                                                                         |
|   [RideController]                                       [DriverController]             |
|          |                                                        |                     |
|          v                                                        v                     |
|   [RideService]                                          [DriverLocationService]        |
|     1. Save to DB (REQUESTED)                                     |                     |
|     2. Publish to Kafka                                           |                     |
|          |                                                        |                     |
+----------|--------------------------------------------------------|---------------------+
           |                                                        |
           v                                                        v
+-----------------------+                         +---------------------------------------+
|     APACHE KAFKA      |                         |             REDIS CACHE               |
|                       |                         |                                       |
| Topic: ride.requested |                         | 1. Geo Set: "drivers" (lon, lat, id)  |
| (3 Partitions)        |                         | 2. Heartbeat: "driver:{id}:alive"     |
|                       |                         |    (TTL: 120s)                        |
| Topic: ride.requested |                         | 3. Distributed Lock:                  |
|        -dlt           |                         |    "driver:{id}:busy" (TTL: 300s)     |
+-----------+-----------+                         +-------------------+-------------------+
            |                                                         ^
            | (Consumer Concurrency: 3)                               |
            v                                                         |
+-------------------------------------------------------------+       |
| [RideRequestedConsumer]                                     |       |
|   -> Calls RideService.processRide()                        |       |
|   -> Queries nearby alive drivers --------------------------+       |
|   -> Locks driver via Redis SETNX (driver:{id}:busy) -------+       |
|   -> Conditional DB State Update (REQUESTED -> MATCHED)             |
|   -> Pushes SSE event to Rider ("ride-status")                      |
+-----------------------------+---------------------------------------+
                              |
                              v
+-----------------------------------------------------------------------------------------+
|                                  POSTGRESQL DATABASE                                    |
|                                                                                         |
|   Table: rides                                                                          |
|   Columns: id (UUID), pickup_latitude, pickup_longitude, driver_id, status, timestamps  |
|   Optimistic CAS Transition: UPDATE ... WHERE id = :id AND status = 'REQUESTED'        |
+-----------------------------------------------------------------------------------------+
```

---

## 🧭 Suggested Reading Order

1. **If you are new to the project or preparing for an interview**:
   - Start with **[01. System Architecture & Flows](./01-architecture-and-system-design.md)** to grasp the big picture.
   - Read **[02. Database & PostgreSQL Integration](./02-database-and-postgresql.md)**, **[05. Driver Simulation Guide](./05-driver-simulation-guide.md)**, and **[06. Load Testing & Benchmarking](./06-load-testing-and-benchmarking.md)** to master the three latest additions you asked about.
   - Finish with **[08. Interview Cheat Sheet & Q&A](./08-interview-cheat-sheet-and-qa.md)** to practice answering questions aloud.

2. **If you are writing or reviewing code**:
   - Check **[07. Complete File & Module Reference](./07-complete-file-and-module-reference.md)** for a file-by-file breakdown of methods, imports, and variables.
   - Consult **[03. Redis Geospatial & Distributed Locking](./03-redis-geospatial-and-distributed-locking.md)** and **[04. Kafka Event Streaming & Concurrency](./04-kafka-event-streaming-and-concurrency.md)** for distributed systems guarantees.
