# 03 — Redis Geospatial & Distributed Locking

---

## ⚡ 1. Why Redis in Voyage?

In a ride-matching system, drivers move constantly and riders demand immediate matching. Redis serves three critical roles in Voyage:
1. **Spatial Index**: Fast retrieval of drivers within a radius (sub-millisecond).
2. **Ephemeral Heartbeat Sentinel**: Automatic detection of offline drivers via key TTLs.
3. **Distributed Mutex Lock**: Guaranteeing that two riders never book the same driver simultaneously.

---

## 🗺️ 2. Key Architecture & TTL Lifecycle

Voyage uses three distinct key naming patterns in Redis:

| Key Pattern | Redis Type | Expiration (TTL) | Value / Payload | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `drivers` | `GEO / ZSET` | None (Permanent index) | Sorted set member: `driverId` | Stores geographical coordinates (Longitude, Latitude) encoded into 52-bit geohashes. |
| `driver:{id}:alive` | `String` | **2 minutes** (`Duration.ofMinutes(2)`) | `"1"` | Heartbeat flag. Driver must ping every 2 minutes or they are marked offline. |
| `driver:{id}:busy` | `String` | **5 minutes** (`Duration.ofMinutes(5)`) | `rideId` (UUID string) | Distributed lock holding the ID of the reserving ride. |

---

## 📍 3. Geospatial Indexing (`GEOADD` & `GEORADIUS`)

### Step 1: Storing Driver Coordinates
In [`DriverLocationService.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/service/DriverLocationService.java#L25-L29):
```java
public void saveLocation(Integer id, double latitude, double longitude) {
    Point point = new Point(longitude, latitude); // Note: Longitude first, Latitude second!
    redisTemplate.opsForGeo().add(KEY, point, id.toString());
    redisTemplate.opsForValue().set(aliveKey(id), "1", Duration.ofMinutes(2));
}
```

> [!IMPORTANT]
> **Spatial GIS Standard (X, Y)**:
> In everyday speech, people say *"latitude, longitude"*. However, in cartography and Redis, coordinates follow Cartesian `(X, Y)`. Longitude is the **X-axis** (east-west: -180 to +180), and Latitude is the **Y-axis** (north-south: -85 to +85). Redis `Point` takes `new Point(longitude, latitude)`. Swapping these will place your driver in Antarctica or the middle of the ocean!

### Step 2: Radial Querying
When a rider requests a pickup at `(latitude, longitude)`, we query for all drivers within a 5 km circle:
```java
Point center = new Point(longitude, latitude);
Distance distance = new Distance(radiusKm, Metrics.KILOMETERS);
Circle circle = new Circle(center, distance);

RedisGeoCommands.GeoRadiusCommandArgs args = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
        .includeDistance()
        .sortAscending();

GeoResults<RedisGeoCommands.GeoLocation<String>> results = 
        redisTemplate.opsForGeo().radius(KEY, circle, args);
```
- `includeDistance()`: Computes the precise distance from rider to driver.
- `sortAscending()`: Returns the closest driver first (Driver at 0.4 km appears before Driver at 2.1 km).

---

## 💓 4. The Driver Heartbeat Pattern (Detecting Dead Apps)

### The Problem:
What happens if a driver drives into a tunnel, runs out of battery, or force-closes their phone app?
The driver's coordinate remains inside the Redis `drivers` Geoset forever. If a rider requests a ride nearby, the system would keep trying to match a driver who is no longer active!

### The Solution:
Whenever a driver reports location, we set an ephemeral key:
```java
redisTemplate.opsForValue().set("driver:" + id + ":alive", "1", Duration.ofMinutes(2));
```

When searching for nearby drivers, we perform a **liveness check**:
```java
List<GeoResult<RedisGeoCommands.GeoLocation<String>>> aliveDrivers = new ArrayList<>();
results.getContent().forEach(result -> {
    String driverId = result.getContent().getName();
    if (Boolean.TRUE.equals(redisTemplate.hasKey(aliveKey(Integer.parseInt(driverId)))))) {
        aliveDrivers.add(result);
    }
});
```
If a driver hasn't sent a GPS ping in the last 2 minutes, the key automatically disappears. The matching engine silently skips them.

---

## 🔒 5. Distributed Locking via `SETNX` (`tryReserve`)

### The Race Condition (Double-Booking):
Suppose Driver 101 is located at the center of a busy train station.
- Rider Alice submits a ride request at 10:00:00.001.
- Rider Bob submits a ride request at 10:00:00.002.
Both requests query Redis and see Driver 101 as the closest driver (0.2 km).
Without synchronization, **both Alice and Bob would be matched with Driver 101**.

### The Mutex Acquisition:
In [`DriverLocationService.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/service/DriverLocationService.java#L77-L87):
```java
public boolean tryReserve(Integer driverId, String rideId) {
    Boolean success = redisTemplate.opsForValue()
            .setIfAbsent(busyKey(driverId), rideId, Duration.ofMinutes(5));
    if (Boolean.TRUE.equals(success)) {
        return true;
    } else {
        // Idempotency check: is the driver already reserved for the same rideId?
        String current = redisTemplate.opsForValue().get(busyKey(driverId));
        return rideId.equals(current);
    }
}
```

### How `SETNX` (`setIfAbsent`) works:
- Translates to Redis command: `SET driver:101:busy "ride-alice-uuid" NX EX 300`
- `NX` = **Only set if Not eXists**.
- Since Redis executes commands sequentially on its single-threaded event loop, Alice's command arrives first. The key is created, returning `true`.
- When Bob's command arrives 1 millisecond later, the key already exists. Redis rejects Bob's write and returns `false`.
- Bob's thread catches `false` and immediately evaluates the next nearest driver (e.g., Driver 102).

### Idempotency Protection:
What if Kafka experiences network jitter and delivers Alice's ride message twice?
1. The first delivery locks Driver 101 for `ride-alice-uuid`.
2. The retry arrives a moment later. `setIfAbsent` returns `false` because the key already exists.
3. Our fallback check executes: `return rideId.equals(current);`.
4. Since `current == "ride-alice-uuid"`, it returns `true`! The system knows Alice *already* owns this lock and does not mark the retry as a failure.

---

## 💣 6. Atomic Lock Release via Lua Script (The Danger of Plain `DEL`)

When a ride completes, we must release `driver:{id}:busy` so the driver can take new rides.
Why can't we simply call `redisTemplate.delete("driver:101:busy")`?

### The "Stolen Lock" Disaster (Why plain `DEL` breaks):
1. Rider A reserves Driver 101 for 5 minutes (`TTL = 300s`).
2. Due to heavy traffic or network delays, the trip or matching takes 5 minutes and 1 second.
3. Redis automatically expires `driver:101:busy` because the 5-minute TTL elapsed.
4. Rider B requests a ride. Driver 101 is now free, so Rider B locks Driver 101 (`driver:101:busy = "ride-B"`).
5. Suddenly, Rider A's server thread wakes up and blindly calls `DEL driver:101:busy`.
6. **Disaster**: Rider A just deleted **Rider B's lock**! Driver 101 is now unlocked while driving Rider B, allowing Rider C to book Driver 101!

### The Atomic Lua Solution:
In [`DriverLocationService.java`](file:///c:/6th%20sem/Microservice%20and%20Other/voyage/src/main/java/com/akash/voyage/service/DriverLocationService.java#L89-L95):
```java
public boolean release(Integer driverId, String rideId) {
    DefaultRedisScript<Long> script = new DefaultRedisScript<>();
    script.setScriptText(
        "if redis.call('GET', KEYS[1]) == ARGV[1] then " +
        "    return redis.call('DEL', KEYS[1]) " +
        "else " +
        "    return 0 " +
        "end"
    );
    script.setResultType(Long.class);
    Long result = redisTemplate.execute(script, List.of(busyKey(driverId)), rideId);
    return result != null && result == 1L;
}
```

### Why Lua guarantees safety:
1. **Atomicity**: Redis executes Lua scripts as a single atomic unit. No other command can execute while the script is running.
2. **Value Check**: It reads the key (`GET KEYS[1]`) and compares it against `ARGV[1]` (`rideId`).
3. If Rider A attempts to delete Rider B's lock, the comparison fails (`"ride-B" != "ride-A"`). The script returns `0` and **does not delete the key**!

---

## 🎤 7. How to Explain Redis to an Interviewer

> **Interviewer**: *"How do you prevent two riders from booking the same driver at the same time?"*

> **Your Answer**:
> *"We implement a distributed lock pattern using Redis `SETNX` (`setIfAbsent`) with an automatic 5-minute safety TTL.*
>
> *When a rider needs a driver, we iterate through candidate drivers found via Redis Geospatial radius query. For each candidate, we attempt: `SET driver:{id}:busy rideId NX EX 300`.*
>
> *Because Redis handles commands atomically on its single-threaded core, exactly one concurrent thread succeeds in creating the key and acquiring the lock. All other threads receive false and seamlessly fall back to the next closest candidate.*
>
> *When releasing the driver upon trip completion, we execute an atomic Redis Lua script that checks whether the key's value matches the completing `rideId` before issuing a `DEL`. This eliminates the classic distributed locking bug where a thread inadvertently deletes a newly reassigned lock belonging to another ride."*
