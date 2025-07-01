# Redis Multi-Datacenter Client Library

A comprehensive Java Gradle-based Redis client library using Lettuce that supports synchronous, asynchronous, and reactive programming models for multi-datacenter deployments.

## Features

### 🌐 Multi-Datacenter Support
- **Data Locality**: Configurable read/write operations targeting local or remote datacenters
- **Intelligent Routing**: Based on datacenter proximity, latency, and availability
- **Cross-Datacenter Lookup**: Support for distributed data discovery across multiple datacenters

### 🔄 Programming Models
- **Synchronous Operations**: Traditional blocking API for simple use cases
- **Asynchronous Operations**: CompletableFuture-based non-blocking operations
- **Reactive Operations**: Reactive Streams (Project Reactor) for high-throughput scenarios

### 🛡️ Enterprise-Grade Reliability
- **Circuit Breaker Pattern**: Built-in fault tolerance with configurable thresholds
- **Health Monitoring**: Continuous datacenter health checks and automatic failover
- **Retry Logic**: Configurable retry mechanisms with exponential backoff
- **Production-Ready Connection Pooling**: Enterprise-grade connection pools per datacenter with comprehensive metrics
- **Resilience4j Integration**: Complete fault tolerance patterns (circuit breaker, retry, rate limiter, bulkhead, time limiter)

### 📊 Observability
- **Metrics Collection**: Comprehensive metrics using Micrometer
- **Health Events**: Real-time health change notifications
- **Performance Monitoring**: Latency, throughput, and error rate tracking
- **Distributed Tracing**: Request tracing across datacenters

### 🔑 Advanced Key Management
- **Tombstone Keys**: Support for soft deletion, cache invalidation, and distributed locks
- **Data Lifecycle**: Automated data expiration and cleanup
- **Conflict Resolution**: CRDB-aware conflict resolution for multi-master scenarios

## Quick Start

### Dependencies

Add the following dependencies to your `build.gradle`:

```gradle
dependencies {
    implementation 'com.redis.multidc:redis-multidc-client:1.0.0'
    implementation 'io.lettuce:lettuce-core:6.3.2.RELEASE'
    implementation 'io.projectreactor:reactor-core:3.6.0'
    implementation 'io.micrometer:micrometer-core:1.12.0'
    
    // Resilience4j for fault tolerance patterns
    implementation 'io.github.resilience4j:resilience4j-all:2.1.0'
    
    // Optional: Spring Boot integration
    implementation 'org.springframework.boot:spring-boot-starter:3.2.0'
}
```

For Maven projects:

```xml
<dependencies>
    <dependency>
        <groupId>com.redis.multidc</groupId>
        <artifactId>redis-multidc-client</artifactId>
        <version>1.0.0</version>
    </dependency>
    <dependency>
        <groupId>io.lettuce</groupId>
        <artifactId>lettuce-core</artifactId>
        <version>6.3.2.RELEASE</version>
    </dependency>
    <dependency>
        <groupId>io.projectreactor</groupId>
        <artifactId>reactor-core</artifactId>
        <version>3.6.0</version>
    </dependency>
    <dependency>
        <groupId>io.micrometer</groupId>
        <artifactId>micrometer-core</artifactId>
        <version>1.12.0</version>
    </dependency>
    <dependency>
        <groupId>io.github.resilience4j</groupId>
        <artifactId>resilience4j-all</artifactId>
        <version>2.1.0</version>
    </dependency>
</dependencies>
```

### Basic Usage

```java
import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.ResilienceConfig;
import com.redis.multidc.config.RoutingStrategy;
import com.redis.multidc.model.DatacenterPreference;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import reactor.core.publisher.Mono;

// Configure multiple datacenters
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .datacenters(List.of(
        DatacenterEndpoint.builder()
            .id("us-east-1")
            .region("us-east")
            .host("redis-us-east.example.com")
            .port(6379)
            .priority(1)
            .build(),
        DatacenterEndpoint.builder()
            .id("us-west-1")
            .region("us-west")
            .host("redis-us-west.example.com")
            .port(6379)
            .priority(2)
            .build()
    ))
    .localDatacenter("us-east-1")
    .routingStrategy(RoutingStrategy.LATENCY_BASED)
    .healthCheckInterval(Duration.ofSeconds(30))
    .resilienceConfig(ResilienceConfig.defaultConfig()) // Modern resilience configuration
    .build();

// Create client
try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
    
    // Synchronous operations
    client.sync().set("user:123", "John Doe", DatacenterPreference.LOCAL_PREFERRED);
    String value = client.sync().get("user:123", DatacenterPreference.ANY_AVAILABLE);
    
    // Asynchronous operations
    CompletableFuture<String> future = client.async().get("user:123");
    
    // Reactive operations
    Mono<String> mono = client.reactive().get("user:123");
}
```

### Advanced Resilience Configuration

For production environments, you can configure comprehensive resilience patterns:

```java
import com.redis.multidc.config.ResilienceConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.RetryConfig;

// Production-ready resilience configuration
ResilienceConfig resilienceConfig = ResilienceConfig.builder()
    .circuitBreaker(CircuitBreakerConfig.custom()
        .failureRateThreshold(50.0f)                    // 50% failure rate threshold
        .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30s in open state
        .slidingWindowSize(10)                          // Sliding window size
        .minimumNumberOfCalls(5)                        // Minimum calls to evaluate
        .build())
    .retry(RetryConfig.custom()
        .maxAttempts(3)                                 // Max 3 retry attempts
        .waitDuration(Duration.ofMillis(500))           // 500ms between retries
        .build())
    .enableBasicPatterns()                              // Enable circuit breaker + retry
    .build();

// Use in configuration
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .datacenters(/* ... */)
    .resilienceConfig(resilienceConfig)                 // Apply resilience config
    .build();
```

### Quick Configuration Presets

For common scenarios, use predefined configurations:

```java
// Development/Testing - relaxed thresholds
ResilienceConfig relaxed = ResilienceConfig.relaxedConfig();

// High-throughput production - optimized for performance
ResilienceConfig highThroughput = ResilienceConfig.highThroughputConfig();

// Default production - balanced reliability and performance
ResilienceConfig defaultConfig = ResilienceConfig.defaultConfig();
```

## Configuration

### Connection Pooling

The library provides enterprise-grade connection pooling with comprehensive monitoring and management capabilities.

#### Basic Pool Configuration

```java
// Configure connection pools per datacenter
DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
    .id("us-east-1")
    .host("redis.us-east.example.com")
    .port(6379)
    .connectionPoolSize(20)                    // Maximum pool size
    .minPoolSize(5)                           // Minimum connections to maintain
    .acquisitionTimeout(Duration.ofSeconds(2)) // Max time to wait for connection
    .idleTimeout(Duration.ofMinutes(10))      // Idle connection timeout
    .maxConnectionAge(Duration.ofHours(1))    // Maximum connection lifetime
    .validateOnAcquire(true)                  // Validate connections before use
    .build();
```

#### Pool Presets for Common Scenarios

```java
// High throughput configuration
ConnectionPoolConfig highThroughput = ConnectionPoolConfig.builder()
    .highThroughput()                         // Optimized for high volume
    .maxPoolSize(100)
    .minPoolSize(25)
    .acquisitionTimeout(Duration.ofSeconds(1))
    .build();

// Low latency configuration  
ConnectionPoolConfig lowLatency = ConnectionPoolConfig.builder()
    .lowLatency()                             // Optimized for speed
    .maxPoolSize(50)
    .minPoolSize(20)
    .acquisitionTimeout(Duration.ofMillis(500))
    .validateOnAcquire(true)
    .build();

// Balanced configuration
ConnectionPoolConfig balanced = ConnectionPoolConfig.builder()
    .balanced()                               // Balanced throughput/latency
    .maxPoolSize(30)
    .minPoolSize(10)
    .build();

// Apply configuration to endpoint
DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
    .id("production-dc")
    .host("redis.prod.example.com")
    .poolConfig(highThroughput)               // Use preset configuration
    .build();
```

#### Advanced Pool Configuration

```java
ConnectionPoolConfig advancedConfig = ConnectionPoolConfig.builder()
    .maxPoolSize(50)
    .minPoolSize(10)
    .acquisitionTimeout(Duration.ofSeconds(2))
    .idleTimeout(Duration.ofMinutes(5))
    .maxConnectionAge(Duration.ofMinutes(30))
    .validateOnAcquire(true)
    .validateOnReturn(false)
    .validatePeriodically(true)
    .validationInterval(Duration.ofMinutes(1))
    .evictionInterval(Duration.ofMinutes(2))
    .build();
```

#### Pool Metrics and Monitoring

```java
// Access pool metrics for monitoring
ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics("us-east-1");

System.out.printf("Pool Status for %s:%n", metrics.getDatacenterId());
System.out.printf("  Active Connections: %d/%d%n", 
    metrics.getActiveConnections(), metrics.getMaxPoolSize());
System.out.printf("  Idle Connections: %d%n", metrics.getIdleConnections());
System.out.printf("  Pool Utilization: %.1f%%%n", metrics.getUtilizationPercentage());
System.out.printf("  Efficiency Ratio: %.3f%n", metrics.getEfficiencyRatio());
System.out.printf("  Average Acquisition Time: %.2f ms%n", metrics.getAverageAcquisitionTime());
System.out.printf("  Total Connections Created: %d%n", metrics.getTotalConnectionsCreated());
System.out.printf("  Total Acquisition Timeouts: %d%n", metrics.getTotalAcquisitionTimeouts());

// Get aggregated metrics across all datacenters
AggregatedPoolMetrics aggregated = client.getAggregatedPoolMetrics();
System.out.printf("Total Active Connections: %d%n", aggregated.getTotalActiveConnections());
System.out.printf("Overall Pool Utilization: %.1f%%%n", aggregated.getOverallUtilization());

// Pool health monitoring
boolean isHealthy = client.isConnectionPoolHealthy("us-east-1");
Map<String, Boolean> allPoolHealth = client.getConnectionPoolHealth();
```

#### Pool Management Operations

```java
// Drain connections from a pool (useful for maintenance)
client.drainConnectionPool("us-east-1");

// Reset pool metrics
client.resetConnectionPoolMetrics("us-east-1");

// Get detailed pool status
Map<String, ConnectionPoolMetrics> allMetrics = client.getAllConnectionPoolMetrics();
for (Map.Entry<String, ConnectionPoolMetrics> entry : allMetrics.entrySet()) {
    System.out.printf("Datacenter %s: %s%n", entry.getKey(), entry.getValue());
}

// Monitor pool events (connection creation, destruction, acquisition timeouts)
client.subscribeToPoolEvents((datacenterId, event, details) -> {
    switch (event) {
        case CONNECTION_CREATED:
            logger.info("New connection created for datacenter: {}", datacenterId);
            break;
        case ACQUISITION_TIMEOUT:
            logger.warn("Connection acquisition timeout for datacenter: {}", datacenterId);
            // Consider increasing pool size or acquisition timeout
            break;
        case POOL_EXHAUSTED:
            logger.error("Connection pool exhausted for datacenter: {}", datacenterId);
            // Alert operations team
            break;
    }
});
```

#### Integration with Resilience Patterns

```java
// Connection pooling works seamlessly with resilience patterns
ResilienceConfig resilienceConfig = ResilienceConfig.builder()
    .enableAllPatterns()
    .circuitBreakerConfig(
        30.0f,                                // 30% failure rate threshold
        Duration.ofSeconds(60),               // Open state duration
        100,                                  // Sliding window size
        10                                    // Minimum calls before evaluation
    )
    .retryConfig(
        3,                                    // Max retry attempts
        Duration.ofMillis(200)                // Delay between retries
    )
    .rateLimiterConfig(
        1000,                                 // Permits per period
        Duration.ofSeconds(1),                // Period duration
        Duration.ofMillis(100)                // Timeout for permit acquisition
    )
    .bulkheadConfig(
        50,                                   // Max concurrent calls
        Duration.ofMillis(100)                // Max wait duration
    )
    .timeLimiterConfig(
        Duration.ofSeconds(10),               // Operation timeout
        true                                  // Cancel running futures on timeout
    )
    .build();

DatacenterConfiguration config = DatacenterConfiguration.builder()
    .datacenters(List.of(
        DatacenterEndpoint.builder()
            .id("resilient-dc")
            .host("redis.example.com")
            .port(6379)
            .poolConfig(ConnectionPoolConfig.builder()
                .highThroughput()
                .maxPoolSize(100)
                .build())
            .resilienceConfig(resilienceConfig)
            .build()
    ))
    .build();
```

### Datacenter Configuration

```java
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .datacenters(List.of(/* endpoints */))
    .localDatacenter("primary-dc")
    .routingStrategy(RoutingStrategy.LATENCY_BASED)
    .healthCheckInterval(Duration.ofSeconds(30))
    .connectionTimeout(Duration.ofSeconds(5))
    .requestTimeout(Duration.ofSeconds(10))
    .maxRetries(3)
    .retryDelay(Duration.ofMillis(100))
    .enableCircuitBreaker(true)
    .circuitBreakerConfig(CircuitBreakerConfig.defaultConfig())
    .build();
```

### Datacenter Endpoints

```java
DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
    .id("us-east-1")
    .region("us-east")
    .host("redis.us-east-1.amazonaws.com")
    .port(6379)
    .ssl(true)
    .password("your-password")
    .database(0)
    .priority(1)
    .weight(1.0)
    .readOnly(false)
    .build();
```

## Routing Strategies

### Available Strategies

- **`LOCAL_ONLY`**: Always use the local datacenter
- **`LATENCY_BASED`**: Route to the datacenter with lowest latency
- **`PRIORITY_BASED`**: Use datacenter priority configuration
- **`WEIGHTED_ROUND_ROBIN`**: Distribute load based on weights
- **`LOCAL_WRITE_ANY_READ`**: Local writes, any datacenter for reads
- **`CUSTOM`**: Pluggable custom routing logic

### Datacenter Preferences

Control routing on a per-operation basis:

```java
// Use local datacenter only
client.sync().get("key", DatacenterPreference.LOCAL_ONLY);

// Prefer local, fallback to remote
client.sync().get("key", DatacenterPreference.LOCAL_PREFERRED);

// Use any available datacenter
client.sync().get("key", DatacenterPreference.ANY_AVAILABLE);

// Use lowest latency datacenter
client.sync().get("key", DatacenterPreference.LOWEST_LATENCY);

// Use remote datacenters only
client.sync().get("key", DatacenterPreference.REMOTE_ONLY);
```

## Operations

### String Operations

All string operations support datacenter routing and are available in sync, async, and reactive modes.

```java
// Basic operations
client.sync().set("key", "value");
String value = client.sync().get("key");

// With TTL
client.sync().set("session:123", "active", Duration.ofMinutes(30));

// Conditional set
boolean created = client.sync().setIfNotExists("lock:resource", "owner-id");
boolean createdWithTtl = client.sync().setIfNotExists("temp:resource", "value", Duration.ofMinutes(5));

// Delete operations
boolean deleted = client.sync().delete("key");
long deletedCount = client.sync().delete("key1", "key2", "key3");

// Key existence and metadata
boolean exists = client.sync().exists("key");
Duration ttl = client.sync().ttl("key");
boolean expired = client.sync().expire("key", Duration.ofMinutes(10));

// Async variants
CompletableFuture<String> futureValue = client.async().get("key");
CompletableFuture<Void> futureSet = client.async().set("key", "value");

// Reactive variants
Mono<String> monoValue = client.reactive().get("key");
Mono<Void> monoSet = client.reactive().set("key", "value");
```

### Hash Operations

Hash operations provide efficient storage for structured data across datacenters.

```java
// Single field operations
client.sync().hset("user:123", "name", "John Doe");
client.sync().hset("user:123", "email", "john@example.com");

String name = client.sync().hget("user:123", "name");
boolean fieldExists = client.sync().hexists("user:123", "email");

// Bulk operations
Map<String, String> fields = Map.of(
    "name", "John Doe",
    "email", "john@example.com",
    "age", "30",
    "city", "New York"
);
client.sync().hset("user:123", fields);

// Retrieve all fields
Map<String, String> user = client.sync().hgetAll("user:123");
Set<String> fieldNames = client.sync().hkeys("user:123");

// Delete fields
boolean deleted = client.sync().hdel("user:123", "temporary_field");
long deletedCount = client.sync().hdel("user:123", "field1", "field2", "field3");

// Async hash operations
CompletableFuture<String> futureName = client.async().hget("user:123", "name");
CompletableFuture<Map<String, String>> futureUser = client.async().hgetAll("user:123");
```

### List Operations

Redis lists with multi-datacenter awareness for queue and stack operations.

```java
// Push operations
client.sync().lpush("queue:tasks", "task1", "task2", "task3");
client.sync().rpush("queue:notifications", "notification1", "notification2");

// Pop operations
String leftItem = client.sync().lpop("queue:tasks");
String rightItem = client.sync().rpop("queue:notifications");

// Range operations
List<String> items = client.sync().lrange("queue:tasks", 0, 10);
long length = client.sync().llen("queue:tasks");

// Cross-datacenter list operations
client.sync().lpush("global:events", DatacenterPreference.ANY_AVAILABLE, "event1", "event2");

// Async list operations
CompletableFuture<String> futureItem = client.async().lpop("queue:priority");
CompletableFuture<List<String>> futureRange = client.async().lrange("queue:all", 0, -1);
```

### Set Operations

Unordered collections with membership testing and set operations.

```java
// Add and remove members
boolean added = client.sync().sadd("tags:article:123", "redis", "database", "nosql");
boolean removed = client.sync().srem("tags:article:123", "deprecated_tag");

// Membership and cardinality
boolean isMember = client.sync().sismember("tags:article:123", "redis");
long memberCount = client.sync().scard("tags:article:123");

// Retrieve all members
Set<String> allTags = client.sync().smembers("tags:article:123");

// Async set operations
CompletableFuture<Boolean> futureAdded = client.async().sadd("online:users", "user123", "user456");
CompletableFuture<Set<String>> futureMembers = client.async().smembers("online:users");
```

### Sorted Set Operations

Score-based ordered collections for rankings and leaderboards.

```java
// Add with scores
boolean added = client.sync().zadd("leaderboard:game", 1500.0, "player1");
client.sync().zadd("leaderboard:game", 2000.0, "player2");

// Bulk add with scores
Map<String, Double> scores = Map.of(
    "player3", 1750.0,
    "player4", 1250.0,
    "player5", 1900.0
);
boolean bulkAdded = client.sync().zadd("leaderboard:game", scores);

// Retrieve by rank
Set<String> topPlayers = client.sync().zrange("leaderboard:game", 0, 9); // Top 10

// Get score for member
Double playerScore = client.sync().zscore("leaderboard:game", "player1");

// Remove members
boolean removed = client.sync().zrem("leaderboard:game", "inactive_player");

// Get cardinality
long playerCount = client.sync().zcard("leaderboard:game");

// Async sorted set operations
CompletableFuture<Boolean> futureAdded = client.async().zadd("realtime:scores", 500.0, "user123");
CompletableFuture<Set<String>> futureTop = client.async().zrange("realtime:scores", 0, 4);
```

### Batch Operations

Efficient bulk operations to reduce network round trips.

```java
// Multi-get operations
List<String> values = client.sync().mget("key1", "key2", "key3");
List<String> preferredValues = client.sync().mget(DatacenterPreference.LOCAL_PREFERRED, 
    "user:123", "user:456", "user:789");

// Multi-set operations
Map<String, String> keyValues = Map.of(
    "cache:user:123", "John Doe",
    "cache:user:456", "Jane Smith",
    "cache:user:789", "Bob Johnson"
);
client.sync().mset(keyValues);
client.sync().mset(keyValues, DatacenterPreference.LOCAL_ONLY);

// Async batch operations
CompletableFuture<List<String>> futureValues = client.async().mget("batch:1", "batch:2", "batch:3");
CompletableFuture<Void> futureSet = client.async().mset(keyValues);
```

## Cross-Datacenter Operations

Advanced operations for multi-datacenter data access and management.

```java
// Direct datacenter access
String remoteValue = client.sync().crossDatacenterGet("user:123", "us-west-1");

// Multi-key cross-datacenter lookup
List<String> keys = List.of("user:123", "user:456", "user:789");
Map<String, String> remoteData = client.sync().crossDatacenterMultiGet(keys, "eu-central-1");

// Async cross-datacenter operations
CompletableFuture<String> futureRemote = client.async().crossDatacenterGet("config:global", "backup-dc");
CompletableFuture<Map<String, String>> futureMulti = client.async().crossDatacenterMultiGet(keys, "dr-datacenter");
```

## Tombstone Keys

Tombstone keys provide advanced data lifecycle management with support for soft deletion, cache invalidation, and cleanup automation.

### Basic Tombstone Operations

```java
// Create tombstone for soft deletion
client.sync().createTombstone("deleted:user:123", TombstoneKey.Type.SOFT_DELETE);

// Create tombstone with TTL for cache invalidation
client.sync().createTombstone("cache:invalidated:product:456", 
    TombstoneKey.Type.CACHE_INVALIDATION, Duration.ofMinutes(5));

// Create tombstone for distributed lock cleanup
client.sync().createTombstone("lock:expired:resource", 
    TombstoneKey.Type.DISTRIBUTED_LOCK, Duration.ofSeconds(30));

// Check if key is tombstoned
boolean isTombstoned = client.sync().isTombstoned("user:123");

// Get tombstone information
TombstoneKey tombstone = client.sync().getTombstone("deleted:user:123");
if (tombstone != null) {
    System.out.println("Tombstone type: " + tombstone.getType());
    System.out.println("Created at: " + tombstone.getCreatedAt());
    System.out.println("TTL: " + tombstone.getTtl());
}

// Remove tombstone when no longer needed
client.sync().removeTombstone("cleaned:user:123");
```

### Tombstone Types

- **`SOFT_DELETE`**: Mark data as deleted without immediate removal
- **`CACHE_INVALIDATION`**: Signal cache entries need refresh
- **`DISTRIBUTED_LOCK`**: Track lock expiration and cleanup
- **`REPLICATION_MARKER`**: Assist with CRDB conflict resolution

### Async Tombstone Operations

```java
// Async tombstone creation
CompletableFuture<Void> futureTombstone = client.async().createTombstone(
    "async:deleted:session:789", TombstoneKey.Type.SOFT_DELETE);

// Async tombstone checking
CompletableFuture<Boolean> futureCheck = client.async().isTombstoned("session:789");

// Async tombstone retrieval
CompletableFuture<TombstoneKey> futureTombstoneInfo = client.async().getTombstone("session:789");
```

### Distributed Locks

High-availability distributed locking with automatic cleanup and timeout handling.

```java
// Basic distributed lock
boolean acquired = client.sync().acquireLock(
    "resource:lock", 
    "owner-id-" + UUID.randomUUID(), 
    Duration.ofMinutes(5)
);

if (acquired) {
    try {
        // Critical section - only one process across all datacenters can execute this
        performCriticalOperation();
        processSharedResource();
    } finally {
        // Always release lock in finally block
        client.sync().releaseLock("resource:lock", "owner-id");
    }
}

// Lock with datacenter preference
boolean localLock = client.sync().acquireLock(
    "local:resource", 
    "local-owner", 
    Duration.ofSeconds(30),
    DatacenterPreference.LOCAL_ONLY
);

// Check lock status
boolean isCurrentlyLocked = client.sync().isLocked("resource:lock");

// Async distributed locks
CompletableFuture<Boolean> futureLock = client.async().acquireLock(
    "async:resource", "async-owner", Duration.ofMinutes(2));

futureLock.thenCompose(acquired -> {
    if (acquired) {
        return performAsyncOperation()
            .whenComplete((result, ex) -> {
                // Release lock after async operation
                client.async().releaseLock("async:resource", "async-owner");
            });
    }
    return CompletableFuture.completedFuture(null);
});
```

### Lock Best Practices

```java
public class DistributedLockExample {
    private final MultiDatacenterRedisClient client;
    
    public void safeLockedOperation(String resourceId) {
        String lockKey = "lock:resource:" + resourceId;
        String lockValue = generateUniqueLockValue();
        Duration lockTimeout = Duration.ofMinutes(5);
        
        boolean acquired = client.sync().acquireLock(lockKey, lockValue, lockTimeout);
        
        if (!acquired) {
            throw new ResourceLockedException("Could not acquire lock for resource: " + resourceId);
        }
        
        try {
            // Perform critical operation
            processResource(resourceId);
        } finally {
            // Always attempt to release
            boolean released = client.sync().releaseLock(lockKey, lockValue);
            if (!released) {
                logger.warn("Failed to release lock for resource: {}", resourceId);
            }
        }
    }
    
    private String generateUniqueLockValue() {
        return System.currentTimeMillis() + "-" + UUID.randomUUID().toString();
    }
}
```

## Monitoring and Observability

### Health Monitoring

Comprehensive health monitoring with real-time datacenter status tracking.

```java
// Get all datacenter information
List<DatacenterInfo> datacenters = client.getDatacenters();
for (DatacenterInfo dc : datacenters) {
    System.out.printf("Datacenter: %s, Region: %s, Status: %s, Latency: %dms%n",
        dc.getId(), dc.getRegion(), dc.getStatus(), dc.getLatency().toMillis());
}

// Get local datacenter info
DatacenterInfo local = client.getLocalDatacenter();
System.out.println("Local datacenter: " + local.getId());

// Perform health check on all datacenters
CompletableFuture<List<DatacenterInfo>> healthCheck = client.checkDatacenterHealth();
healthCheck.thenAccept(dcList -> {
    long healthyCount = dcList.stream().filter(DatacenterInfo::isHealthy).count();
    System.out.println("Healthy datacenters: " + healthyCount + "/" + dcList.size());
});

// Subscribe to health changes
Disposable subscription = client.subscribeToHealthChanges((datacenter, healthy) -> {
    if (!healthy) {
        logger.warn("Datacenter {} is now unhealthy - triggering failover", datacenter.getId());
        // Implement custom failover logic
        handleDatacenterFailure(datacenter);
    } else {
        logger.info("Datacenter {} is now healthy", datacenter.getId());
        handleDatacenterRecovery(datacenter);
    }
});

// Don't forget to dispose subscription when done
// subscription.dispose();
```

### Utility Operations

Administrative and diagnostic operations across datacenters.

```java
// Ping operations for connectivity testing
String pongResponse = client.sync().ping(); // Ping local datacenter
String specificPong = client.sync().ping("us-west-1"); // Ping specific datacenter

// Database size information
long localDbSize = client.sync().dbSize();
long specificDbSize = client.sync().dbSize("eu-central-1");

// Flush operations (use with caution!)
client.sync().flushAll(); // Flush all datacenters
client.sync().flushAll("test-datacenter"); // Flush specific datacenter

// Async utility operations
CompletableFuture<String> futurePing = client.async().ping("backup-dc");
CompletableFuture<Long> futureSize = client.async().dbSize("analytics-dc");
```

### Metrics Integration

The library automatically collects comprehensive metrics using Micrometer.

#### Built-in Metrics

- **Request Metrics**: Count, latency, and success rate per datacenter and operation
- **Connection Pool Metrics**: Active connections, pool utilization, connection creation/destruction
- **Circuit Breaker Metrics**: State changes, failure rates, recovery attempts
- **Health Check Metrics**: Datacenter availability, response times, failure counts
- **Routing Metrics**: Routing decisions, fallback usage, load distribution

#### Custom Metrics Setup

```java
// Use Prometheus metrics
MeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

// Or use other registries
MeterRegistry micrometerRegistry = new SimpleMeterRegistry();

// Configure with custom registry
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .datacenters(endpoints)
    .metricsRegistry(prometheusRegistry)
    .enableDetailedMetrics(true)
    .build();

// Access metrics programmatically
MetricsCollector collector = client.getMetricsCollector();
Duration avgLatency = collector.getAverageLatency("us-east-1");
long requestCount = collector.getRequestCount("us-west-1", "GET");
double successRate = collector.getSuccessRate("eu-central-1");
```

#### Available Metric Names

```java
// Request metrics
"redis.multidc.requests.total"      // Counter: Total requests per datacenter
"redis.multidc.requests.duration"   // Timer: Request duration per datacenter/operation
"redis.multidc.requests.errors"     // Counter: Error count per datacenter/error_type

// Connection metrics
"redis.multidc.connections.active"  // Gauge: Active connections per datacenter
"redis.multidc.connections.pool.size" // Gauge: Connection pool size
"redis.multidc.connections.created" // Counter: Connections created
"redis.multidc.connections.closed"  // Counter: Connections closed

// Circuit breaker metrics
"redis.multidc.circuit.state"       // Gauge: Circuit breaker state (0=closed, 1=open, 2=half-open)
"redis.multidc.circuit.failures"    // Counter: Circuit breaker failures
"redis.multidc.circuit.successes"   // Counter: Circuit breaker successes

// Health metrics
"redis.multidc.health.status"       // Gauge: Datacenter health status (0=unhealthy, 1=healthy)
"redis.multidc.health.latency"      // Gauge: Health check latency per datacenter
"redis.multidc.health.checks"       // Counter: Health check attempts
```

## Reactive Programming

Full reactive programming support using Project Reactor for high-throughput, non-blocking operations.

### Reactive Operations

```java
// Basic reactive operations
Mono<String> getValue = client.reactive().get("user:123");
Mono<Void> setValue = client.reactive().set("user:123", "John Doe");

// Reactive pipeline with transformations
Mono<String> pipeline = client.reactive()
    .set("counter", "0")
    .then(client.reactive().get("counter"))
    .map(Integer::parseInt)
    .map(count -> count + 1)
    .map(String::valueOf)
    .flatMap(newValue -> client.reactive().set("counter", newValue))
    .then(client.reactive().get("counter"))
    .doOnNext(value -> logger.info("Final counter value: {}", value))
    .doOnError(error -> logger.error("Pipeline failed", error));

// Execute the pipeline
String result = pipeline.block(); // For demonstration - avoid blocking in production

// Non-blocking execution with subscription
pipeline.subscribe(
    value -> System.out.println("Success: " + value),
    error -> System.err.println("Error: " + error.getMessage()),
    () -> System.out.println("Pipeline completed")
);
```

### Reactive Streaming and Batch Processing

```java
// Reactive key scanning and processing
Flux<String> processKeys = client.reactive()
    .scan("user:*")                          // Scan for user keys
    .take(1000)                              // Limit to 1000 keys
    .flatMap(key -> client.reactive().get(key), 10) // Parallel get with concurrency=10
    .filter(Objects::nonNull)                // Filter out null values
    .map(this::processUserData)              // Transform data
    .buffer(50)                              // Batch into groups of 50
    .flatMap(this::processBatch);            // Process each batch

// Subscribe with backpressure handling
processKeys.subscribe(
    batch -> logger.info("Processed batch of {} items", batch.size()),
    error -> logger.error("Stream processing failed", error),
    () -> logger.info("Stream processing completed")
);

// Reactive hash operations
Mono<Map<String, String>> userProfile = client.reactive()
    .hgetAll("user:profile:123")
    .filter(map -> !map.isEmpty())
    .switchIfEmpty(Mono.fromCallable(() -> loadDefaultProfile()));

// Reactive list processing
Flux<String> processList = client.reactive()
    .lrange("queue:tasks", 0, -1)
    .flatMapIterable(list -> list)           // Convert List<String> to Flux<String>
    .filter(task -> !task.isEmpty())
    .map(this::processTask)
    .onErrorContinue((error, item) -> {
        logger.warn("Failed to process task: {}", item, error);
    });
```

### Advanced Reactive Patterns

```java
// Reactive retry with exponential backoff
Mono<String> resilientGet = client.reactive()
    .get("critical:data")
    .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
        .maxBackoff(Duration.ofSeconds(2))
        .filter(throwable -> throwable instanceof RedisConnectionException))
    .timeout(Duration.ofSeconds(5))
    .onErrorResume(error -> {
        logger.error("Failed to get critical data after retries", error);
        return Mono.just("default-value");
    });

// Reactive circuit breaker pattern
Mono<String> circuitBreakerGet = client.reactive()
    .get("external:api:data")
    .transform(CircuitBreaker.ofName("external-api").toReactiveTransformer())
    .onErrorResume(CircuitBreakerOpenException.class, 
        ex -> getCachedValue())
    .onErrorResume(TimeoutException.class,
        ex -> getDefaultValue());

// Parallel processing across datacenters
Flux<String> parallelDatacenterQuery = Flux.fromIterable(datacenterIds)
    .parallel(datacenterIds.size())
    .runOn(Schedulers.parallel())
    .flatMap(datacenterId -> 
        client.reactive().crossDatacenterGet("global:config", datacenterId))
    .filter(Objects::nonNull)
    .sequential()
    .take(1); // Take first successful result
```

### Backpressure Handling

```java
// Controlled backpressure with buffering
Flux<String> controlledStream = client.reactive()
    .scan("large:dataset:*")
    .onBackpressureBuffer(1000, BufferOverflowStrategy.DROP_OLDEST)
    .flatMap(key -> client.reactive().get(key), 5) // Limit concurrency to 5
    .sample(Duration.ofMillis(100))              // Sample every 100ms
    .doOnNext(this::processData);

// Custom backpressure strategy
Flux<String> customBackpressure = client.reactive()
    .lrange("high:volume:queue", 0, -1)
    .flatMapIterable(list -> list)
    .onBackpressureDrop(item -> logger.warn("Dropped item due to backpressure: {}", item))
    .publishOn(Schedulers.boundedElastic())
    .doOnNext(this::heavyProcessing);
```

### Reactive Error Handling

```java
// Comprehensive error handling
Mono<String> robustOperation = client.reactive()
    .get("sensitive:data")
    .doOnError(RedisConnectionException.class, 
        error -> logger.error("Connection failed", error))
    .doOnError(TimeoutException.class,
        error -> logger.warn("Operation timed out", error))
    .onErrorResume(RedisConnectionException.class,
        error -> client.reactive().get("backup:data"))
    .onErrorReturn(TimeoutException.class, "cached-value")
    .onErrorMap(RuntimeException.class,
        error -> new ServiceException("Redis operation failed", error));

// Fallback chains
Mono<String> fallbackChain = client.reactive()
    .get("primary:source")
    .onErrorResume(ex -> client.reactive().get("secondary:source"))
    .onErrorResume(ex -> client.reactive().get("tertiary:source"))
    .onErrorReturn("default-value");
```

## Best Practices

### Connection Management

Proper connection lifecycle management is crucial for optimal performance and resource utilization.

```java
// Use try-with-resources for automatic cleanup
try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
    // Perform operations
    client.sync().set("key", "value");
    String value = client.sync().get("key");
} // Client automatically closed

// Or explicit cleanup
MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config);
try {
    // Perform operations
} finally {
    client.close(); // Explicit cleanup
}

// Connection pool configuration
DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
    .id("us-east-1")
    .host("redis.example.com")
    .port(6379)
    .connectionPoolSize(20)              // Optimal pool size
    .connectionTimeout(Duration.ofSeconds(5))
    .idleTimeout(Duration.ofMinutes(10))
    .maxConnectionAge(Duration.ofHours(1))
    .build();
```

### Error Handling and Resilience

Implement comprehensive error handling for production resilience.

```java
public class ResilientRedisService {
    private final MultiDatacenterRedisClient client;
    
    public Optional<String> safeGet(String key) {
        try {
            return Optional.ofNullable(client.sync().get(key, DatacenterPreference.LOCAL_PREFERRED));
        } catch (RedisConnectionException e) {
            logger.warn("Connection failed, trying remote datacenter", e);
            try {
                return Optional.ofNullable(client.sync().get(key, DatacenterPreference.REMOTE_ONLY));
            } catch (Exception fallbackError) {
                logger.error("All datacenters failed for key: " + key, fallbackError);
                return Optional.empty();
            }
        } catch (TimeoutException e) {
            logger.warn("Operation timed out for key: " + key, e);
            return Optional.empty();
        }
    }
    
    public void safeSet(String key, String value) {
        int maxRetries = 3;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                client.sync().set(key, value, DatacenterPreference.LOCAL_PREFERRED);
                return; // Success
            } catch (Exception e) {
                logger.warn("Set operation failed (attempt {}/{}): {}", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    throw new ServiceException("Failed to set key after " + maxRetries + " attempts", e);
                }
                // Wait before retry
                try {
                    Thread.sleep(100 * attempt); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ServiceException("Interrupted during retry", ie);
                }
            }
        }
    }
}
```

### Performance Optimization

Optimize performance through proper configuration and usage patterns.

```java
// Choose optimal routing strategies
DatacenterConfiguration highPerformanceConfig = DatacenterConfiguration.builder()
    .routingStrategy(RoutingStrategy.LATENCY_BASED)    // Route to fastest datacenter
    .healthCheckInterval(Duration.ofSeconds(15))       // Frequent health checks
    .connectionTimeout(Duration.ofSeconds(2))          // Fast timeouts
    .requestTimeout(Duration.ofSeconds(5))
    .enableCircuitBreaker(true)
    .circuitBreakerConfig(CircuitBreakerConfig.builder()
        .failureThreshold(5)                           // Open circuit after 5 failures
        .recoveryTimeout(Duration.ofSeconds(30))       // Try recovery after 30s
        .halfOpenMaxCalls(3)                          // Max calls in half-open state
        .build())
    .build();

// Use batch operations for bulk data
Map<String, String> bulkData = prepareBulkData();
client.sync().mset(bulkData, DatacenterPreference.LOCAL_PREFERRED);

// Async operations for non-blocking performance
List<CompletableFuture<String>> futures = keys.stream()
    .map(key -> client.async().get(key))
    .collect(Collectors.toList());

CompletableFuture<List<String>> allResults = CompletableFuture.allOf(
    futures.toArray(new CompletableFuture[0]))
    .thenApply(v -> futures.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList()));

// Reactive streaming for high-throughput
Flux<ProcessedData> highThroughputPipeline = client.reactive()
    .scan("data:*")
    .buffer(100)                                       // Process in batches
    .flatMap(keyBatch -> processBatchReactively(keyBatch), 4) // Parallel processing
    .publishOn(Schedulers.parallel())
    .map(this::transformData)
    .onErrorContinue((error, item) -> 
        logger.warn("Failed to process item: {}", item, error));
```

### Security Best Practices

Implement security measures for production deployments.

```java
// SSL/TLS configuration
DatacenterEndpoint secureEndpoint = DatacenterEndpoint.builder()
    .id("secure-dc")
    .host("redis.secure.example.com")
    .port(6380)
    .ssl(true)
    .sslTruststore("/path/to/truststore.jks")
    .sslTruststorePassword("truststore-password")
    .sslKeystore("/path/to/keystore.jks")
    .sslKeystorePassword("keystore-password")
    .sslProtocol("TLSv1.3")
    .build();

// Authentication configuration
DatacenterEndpoint authEndpoint = DatacenterEndpoint.builder()
    .id("auth-dc")
    .host("redis.auth.example.com")
    .port(6379)
    .username("redis-user")
    .password("secure-password")
    .database(1)
    .build();

// Network security
DatacenterConfiguration secureConfig = DatacenterConfiguration.builder()
    .datacenters(List.of(secureEndpoint, authEndpoint))
    .enableSslHostnameVerification(true)
    .sslContext(createCustomSslContext())
    .networkTimeout(Duration.ofSeconds(30))
    .build();

// Sensitive data handling
public class SecureRedisService {
    private final MultiDatacenterRedisClient client;
    private final Encryptor encryptor;
    
    public void setSecureData(String key, String sensitiveValue) {
        String encryptedValue = encryptor.encrypt(sensitiveValue);
        client.sync().set(key, encryptedValue, Duration.ofMinutes(30));
    }
    
    public Optional<String> getSecureData(String key) {
        return Optional.ofNullable(client.sync().get(key))
            .map(encryptor::decrypt);
    }
}
```

### Monitoring and Alerting Setup

Set up comprehensive monitoring for production environments.

```java
// Metrics configuration
MeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

// Custom health indicators
HealthIndicator redisHealthIndicator = () -> {
    try {
        String pong = client.sync().ping();
        return Health.up()
            .withDetail("response", pong)
            .withDetail("datacenters", client.getDatacenters().size())
            .build();
    } catch (Exception e) {
        return Health.down()
            .withDetail("error", e.getMessage())
            .build();
    }
};

// Alerting on health changes
client.subscribeToHealthChanges((datacenter, healthy) -> {
    if (!healthy) {
        // Send alert
        alertingService.sendAlert(
            "Redis datacenter " + datacenter.getId() + " is unhealthy",
            AlertLevel.WARNING
        );
    }
});

// Performance monitoring
Timer.Sample sample = Timer.start(registry);
try {
    String result = client.sync().get("performance:test");
    sample.stop(Timer.builder("redis.operation.duration")
        .tag("operation", "get")
        .tag("datacenter", client.getLocalDatacenter().getId())
        .register(registry));
} catch (Exception e) {
    sample.stop(Timer.builder("redis.operation.duration")
        .tag("operation", "get")
        .tag("status", "error")
        .register(registry));
}
```

### Testing Strategies

Comprehensive testing approaches for multi-datacenter Redis operations.

```java
@ExtendWith(MockitoExtension.class)
class MultiDatacenterRedisClientTest {
    
    @Mock
    private DatacenterRouter router;
    
    @Mock
    private MetricsCollector metricsCollector;
    
    private MultiDatacenterRedisClient client;
    
    @BeforeEach
    void setUp() {
        // Use embedded Redis for testing
        RedisServer redisServer = RedisServer.builder()
            .port(6370)
            .setting("maxmemory 128M")
            .build();
        redisServer.start();
        
        DatacenterConfiguration testConfig = DatacenterConfiguration.builder()
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("test-dc")
                    .host("localhost")
                    .port(6370)
                    .build()
            ))
            .build();
            
        client = MultiDatacenterRedisClientBuilder.create(testConfig);
    }
    
    @Test
    void testBasicOperations() {
        // Test basic set/get
        client.sync().set("test:key", "test:value");
        String value = client.sync().get("test:key");
        assertThat(value).isEqualTo("test:value");
    }
    
    @Test
    void testDatacenterFailover() {
        // Simulate datacenter failure
        when(router.selectDatacenterForRead(any()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of("backup-dc"));
            
        // Test failover behavior
        assertThatThrownBy(() -> client.sync().get("test:key"))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("No available datacenter");
    }
    
    @Test
    void testAsyncOperations() {
        CompletableFuture<Void> setFuture = client.async().set("async:key", "async:value");
        CompletableFuture<String> getFuture = setFuture
            .thenCompose(v -> client.async().get("async:key"));
            
        String result = getFuture.join();
        assertThat(result).isEqualTo("async:value");
    }
}
```

## Architecture

### Core Components

The library is built with a modular architecture designed for enterprise-scale multi-datacenter deployments.

#### Client Layer

- **`MultiDatacenterRedisClient`**: Main client interface providing sync, async, and reactive APIs
- **`SyncOperations`**: Synchronous blocking operations interface
- **`AsyncOperations`**: Asynchronous CompletableFuture-based operations interface  
- **`ReactiveOperations`**: Reactive Streams operations using Project Reactor

#### Routing and Load Balancing

- **`DatacenterRouter`**: Intelligent routing logic with pluggable strategies
- **`RoutingStrategy`**: Configurable routing algorithms (latency-based, priority-based, etc.)
- **`DatacenterSelector`**: Datacenter selection based on preferences and availability
- **`LoadBalancer`**: Traffic distribution across available datacenters

#### Health and Monitoring

- **`DatacenterHealthMonitor`**: Continuous health monitoring with configurable intervals
- **`CircuitBreaker`**: Fault tolerance with automatic failure detection and recovery
- **`MetricsCollector`**: Comprehensive observability using Micrometer
- **`HealthEventPublisher`**: Real-time health change notifications

#### Data Management

- **`TombstoneKeyManager`**: Advanced key lifecycle management for soft deletion and cache invalidation
- **`ReplicationManager`**: CRDB-aware conflict resolution and replication handling
- **`DataLocalityManager`**: Intelligent data placement and access optimization

#### Network and Connection Layer

- **`ConnectionPool`**: Per-datacenter connection pooling with lifecycle management
- **`ConnectionFactory`**: Lettuce connection creation and configuration
- **`SSLContextProvider`**: Security configuration for encrypted connections

### Architecture Diagram

```text
┌─────────────────────────────────────────────────────────────────┐
│                   MultiDatacenterRedisClient                   │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   SyncOps       │    AsyncOps     │      ReactiveOps            │
│   Interface     │    Interface    │      Interface              │
└─────────────────┴─────────────────┴─────────────────────────────┘
                           │
┌─────────────────────────────────────────────────────────────────┐
│                   DatacenterRouter                             │
├─────────────────┬─────────────────┬─────────────────────────────┤
│ RoutingStrategy │ DatacenterSel   │    LoadBalancer             │
│ • Latency-based │ • Preference    │    • Weighted RR            │
│ • Priority-based│ • Availability  │    • Random                 │
│ • Custom        │ • Health        │    • Custom                 │
└─────────────────┴─────────────────┴─────────────────────────────┘
                           │
┌─────────────────────────────────────────────────────────────────┐
│                 Health & Monitoring Layer                      │
├─────────────────┬─────────────────┬─────────────────────────────┤
│ HealthMonitor   │ CircuitBreaker  │    MetricsCollector         │
│ • Periodic      │ • Failure Det   │    • Request Metrics        │
│ • On-demand     │ • Auto Recovery │    • Connection Metrics     │
│ • Event-driven  │ • Configurable  │    • Health Metrics         │
└─────────────────┴─────────────────┴─────────────────────────────┘
                           │
┌─────────────────────────────────────────────────────────────────┐
│                   Connection Layer                             │
├─────────────────┬─────────────────┬─────────────────────────────┤
│ ConnectionPool  │ Lettuce Client  │    SSL/TLS Provider         │
│ • Per-DC Pool   │ • Async/Sync    │    • Cert Management        │
│ • Lifecycle Mgmt│ • Reactive      │    • Protocol Config        │
│ • Monitoring    │ • Pipeline      │    • Hostname Verify        │
└─────────────────┴─────────────────┴─────────────────────────────┘
                           │
┌─────────────────────────────────────────────────────────────────┐
│                     Redis Datacenters                          │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   US-East-1     │    US-West-2    │      EU-Central-1           │
│   Primary       │    Secondary    │      DR Site                │
│   Read/Write    │    Read/Write   │      Read Only              │
└─────────────────┴─────────────────┴─────────────────────────────┘
```

### Data Flow

#### Read Operations Flow

1. **Request Received**: Client receives read request with optional DatacenterPreference
2. **Routing Decision**: DatacenterRouter selects optimal datacenter based on:
   - Preference (LOCAL_PREFERRED, ANY_AVAILABLE, etc.)
   - Datacenter health status
   - Current latency metrics
   - Circuit breaker state
3. **Connection Acquisition**: ConnectionPool provides connection from selected datacenter
4. **Operation Execution**: Lettuce executes Redis command asynchronously
5. **Metrics Collection**: Request duration, success/failure recorded
6. **Result Processing**: Response transformed and returned to client

#### Write Operations Flow

1. **Request Received**: Client receives write request
2. **Write Routing**: Router selects write datacenter (usually local or primary)
3. **Circuit Breaker Check**: Verify datacenter is healthy and accepting requests
4. **Operation Execution**: Execute write operation with timeout
5. **Replication Handling**: Handle any CRDB replication requirements
6. **Tombstone Management**: Process any tombstone key requirements
7. **Metrics & Monitoring**: Record operation metrics and health data

### Thread Safety

All client operations are thread-safe and designed for concurrent access:

- **Connection Pools**: Thread-safe per-datacenter connection management
- **Routing Logic**: Immutable routing decisions with concurrent health updates
- **Metrics Collection**: Lock-free metrics aggregation using Micrometer
- **Health Monitoring**: Asynchronous health checks with thread-safe state updates

### Memory Management

Efficient memory usage through:

- **Connection Pooling**: Reuse connections to minimize allocation overhead
- **Lazy Initialization**: Components initialized only when needed
- **Resource Cleanup**: Automatic cleanup of connections and resources
- **Bounded Queues**: Prevent memory leaks with bounded internal queues

## Configuration Reference

### Complete Configuration Example

```java
DatacenterConfiguration config = DatacenterConfiguration.builder()
    // Datacenter endpoints
    .datacenters(List.of(
        DatacenterEndpoint.builder()
            .id("us-east-1")
            .region("us-east")
            .host("redis-1.us-east.example.com")
            .port(6379)
            .ssl(true)
            .username("redis-user")
            .password("secure-password")
            .database(0)
            .priority(1)
            .weight(1.0)
            .readOnly(false)
            .connectionPoolSize(20)
            .connectionTimeout(Duration.ofSeconds(5))
            .idleTimeout(Duration.ofMinutes(10))
            .build(),
        DatacenterEndpoint.builder()
            .id("us-west-2")
            .region("us-west")
            .host("redis-1.us-west.example.com")
            .port(6379)
            .ssl(true)
            .priority(2)
            .weight(0.8)
            .build()
    ))
    
    // Global configuration
    .localDatacenter("us-east-1")
    .routingStrategy(RoutingStrategy.LATENCY_BASED)
    .fallbackStrategy(FallbackStrategy.NEXT_AVAILABLE)
    
    // Timeouts and retries
    .connectionTimeout(Duration.ofSeconds(5))
    .requestTimeout(Duration.ofSeconds(10))
    .maxRetries(3)
    .retryDelay(Duration.ofMillis(100))
    .retryBackoffMultiplier(2.0)
    
    // Health monitoring
    .healthCheckInterval(Duration.ofSeconds(30))
    .healthCheckTimeout(Duration.ofSeconds(5))
    .enableCircuitBreaker(true)
    .circuitBreakerConfig(CircuitBreakerConfig.builder()
        .failureRateThreshold(5)
        .waitDurationInOpenState(Duration.ofSeconds(30))
        .slidingWindowSize(10)
        .minimumNumberOfCalls(5)
        .build())
    
    // Security
    .enableSslHostnameVerification(true)
    .sslContext(customSslContext)
    .authenticationMode(AuthenticationMode.PASSWORD)
    
    // Observability
    .enableDetailedMetrics(true)
    .metricsRegistry(meterRegistry)
    .enableDistributedTracing(true)
    
    // Advanced features
    .enableTombstoneKeys(true)
    .tombstoneKeyTtl(Duration.ofMinutes(10))
    .enableDataLocalityOptimization(true)
    .compressionEnabled(true)
    .build();
```

### Environment-Specific Configurations

#### Development Configuration

```java
DatacenterConfiguration devConfig = DatacenterConfiguration.builder()
    .datacenters(List.of(
        DatacenterEndpoint.builder()
            .id("local")
            .host("localhost")
            .port(6379)
            .build()
    ))
    .localDatacenter("local")
    .routingStrategy(RoutingStrategy.LOCAL_ONLY)
    .requestTimeout(Duration.ofSeconds(30))
    .enableCircuitBreaker(false)
    .build();
```

#### Production Configuration

```java
DatacenterConfiguration prodConfig = DatacenterConfiguration.builder()
    .datacenters(productionEndpoints)
    .routingStrategy(RoutingStrategy.LATENCY_BASED)
    .connectionTimeout(Duration.ofSeconds(2))
    .requestTimeout(Duration.ofSeconds(5))
    .maxRetries(3)
    .enableCircuitBreaker(true)
    .healthCheckInterval(Duration.ofSeconds(15))
    .enableDetailedMetrics(true)
    .enableDistributedTracing(true)
    .build();
```

## Examples and Demonstrations

### Getting Started with Examples

To quickly explore the library's capabilities, check out our comprehensive examples:

#### 🚀 **Start Here**: [Simple Usage Example](lib/src/main/java/com/redis/multidc/example/SimpleUsageExample.java)
Basic working example demonstrating core functionality - perfect for getting started.

#### 🏭 **Production Ready**: [Production Usage Example](lib/src/main/java/com/redis/multidc/example/ProductionUsageExample.java)
Enterprise configuration with SSL/TLS, authentication, monitoring, and best practices.

#### ⚡ **Performance**: [Reactive Streaming Example](lib/src/main/java/com/redis/multidc/example/ReactiveStreamingExample.java)
High-performance reactive programming patterns with backpressure handling and error recovery.

#### 📊 **Monitoring**: [Health Monitoring Example](lib/src/main/java/com/redis/multidc/example/HealthMonitoringExample.java)
Real-time health monitoring, event subscription, and operational observability.

#### 💾 **Advanced Features**: [Data Locality & Tombstone Example](lib/src/main/java/com/redis/multidc/example/DataLocalityManagerExample.java)
Advanced data management patterns including locality optimization and tombstone key management.

Each example includes detailed documentation and can be run independently to demonstrate specific functionality.

### Production Connection Pool Demo

The library includes a comprehensive production demo showcasing enterprise-grade connection pooling:

```java
// Run the production connection pool demonstration
java -cp "lib/build/classes/java/main:$(dependencies)" \
    com.redis.multidc.demo.ProductionConnectionPoolDemo
```

This demo demonstrates:

- **Production-Grade Configuration**: Enterprise resilience patterns and connection pooling
- **High-Performance Pool Setup**: Optimized configurations for different scenarios
- **Load Testing**: Simulated high-load operations with metrics monitoring
- **Metrics Monitoring**: Real-time pool utilization and performance tracking
- **Health Monitoring**: Datacenter health checks and failover scenarios

Key features showcased:

```java
// Production resilience configuration
ResilienceConfig resilienceConfig = ResilienceConfig.builder()
    .enableAllPatterns()
    .circuitBreakerConfig(30.0f, Duration.ofSeconds(60), 100, 10)
    .retryConfig(5, Duration.ofMillis(200))
    .rateLimiterConfig(2000, Duration.ofSeconds(1), Duration.ofMillis(50))
    .bulkheadConfig(50, Duration.ofMillis(100))
    .timeLimiterConfig(Duration.ofSeconds(10), true)
    .build();

// High-performance connection pools
ConnectionPoolConfig primaryPoolConfig = ConnectionPoolConfig.builder()
    .lowLatency()
    .maxPoolSize(100)
    .minPoolSize(20)
    .acquisitionTimeout(Duration.ofSeconds(2))
    .validateOnAcquire(true)
    .build();

// Load testing with metrics
ExecutorService executor = Executors.newFixedThreadPool(50);
for (int i = 0; i < 10000; i++) {
    executor.submit(() -> {
        client.sync().set("load_test_" + UUID.randomUUID(), "value", 
            DatacenterPreference.LOCAL_PREFERRED);
    });
}

// Real-time metrics monitoring
ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics("primary");
System.out.printf("Pool Utilization: %.1f%%, Efficiency: %.3f%n",
    metrics.getUtilizationPercentage(), metrics.getEfficiencyRatio());
```

### Resilience Demo

Demonstrates connection pooling with comprehensive resilience patterns:

```java
// Run the resilience demonstration
java -cp "lib/build/classes/java/main:$(dependencies)" \
    com.redis.multidc.demo.ResilienceDemo
```

Features demonstrated:

- **Circuit Breaker Integration**: Automatic failure detection and recovery
- **Retry Mechanisms**: Configurable retry logic with exponential backoff
- **Rate Limiting**: Traffic shaping and overload protection
- **Bulkhead Isolation**: Resource isolation between operations
- **Time Limiting**: Operation timeout handling
- **Connection Pool Resilience**: Pool behavior under failure scenarios

### Basic Usage Examples

#### Simple Connection Pool Setup

```java
// Basic connection pool configuration
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .datacenters(List.of(
        DatacenterEndpoint.builder()
            .id("primary")
            .host("redis.primary.example.com")
            .port(6379)
            .connectionPoolSize(20)
            .build(),
        DatacenterEndpoint.builder()
            .id("secondary")
            .host("redis.secondary.example.com")
            .port(6379)
            .connectionPoolSize(15)
            .build()
    ))
    .localDatacenter("primary")
    .routingStrategy(RoutingStrategy.LATENCY_BASED)
    .build();

try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
    // Use client with automatic connection pooling
    client.sync().set("user:123", "John Doe");
    String value = client.sync().get("user:123");
    
    // Monitor pool health
    ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics("primary");
    System.out.println("Pool utilization: " + metrics.getUtilizationPercentage() + "%");
}
```

#### High-Performance Async Operations

```java
// High-throughput async operations using connection pools
List<CompletableFuture<Void>> futures = new ArrayList<>();

for (int i = 0; i < 10000; i++) {
    String key = "async_key_" + i;
    String value = "value_" + i;
    
    CompletableFuture<Void> future = client.async()
        .set(key, value, DatacenterPreference.LOCAL_PREFERRED)
        .thenCompose(v -> client.async().expire(key, Duration.ofMinutes(30)))
        .exceptionally(throwable -> {
            logger.error("Failed to set key: " + key, throwable);
            return null;
        });
    
    futures.add(future);
}

// Wait for all operations to complete
CompletableFuture<Void> allOperations = CompletableFuture.allOf(
    futures.toArray(new CompletableFuture[0]));

allOperations.thenRun(() -> {
    // Log final metrics
    AggregatedPoolMetrics aggregated = client.getAggregatedPoolMetrics();
    System.out.printf("Completed 10,000 operations. Total active connections: %d%n",
        aggregated.getTotalActiveConnections());
});
```

#### Reactive Streaming with Connection Pools

```java
// Reactive operations leveraging connection pools
Flux<String> dataStream = client.reactive()
    .scan("user:*")
    .take(10000)
    .flatMap(key -> client.reactive().get(key), 20)  // Parallel with connection pool
    .filter(Objects::nonNull)
    .buffer(100)
    .flatMap(batch -> processBatchReactively(batch), 5)
    .doOnNext(result -> logger.info("Processed batch: {}", result))
    .doOnError(error -> logger.error("Stream processing failed", error))
    .onErrorContinue((error, item) -> {
        logger.warn("Failed to process item, continuing stream", error);
    });

// Subscribe with backpressure handling
dataStream.subscribe(
    result -> updateMetrics(result),
    error -> alertingService.sendAlert("Stream failed", error),
    () -> logger.info("Stream processing completed")
);
```

#### Connection Pool Monitoring and Alerting

```java
// Set up comprehensive pool monitoring
public class PoolMonitoringService {
    private final MultiDatacenterRedisClient client;
    private final MeterRegistry meterRegistry;
    
    public void startMonitoring() {
        // Monitor pool utilization
        Gauge.builder("redis.pool.utilization")
            .description("Connection pool utilization percentage")
            .register(meterRegistry, this, this::getAveragePoolUtilization);
        
        // Monitor acquisition timeouts
        client.subscribeToPoolEvents((datacenterId, event, details) -> {
            if (event == PoolEvent.ACQUISITION_TIMEOUT) {
                Counter.builder("redis.pool.timeouts")
                    .tag("datacenter", datacenterId)
                    .register(meterRegistry)
                    .increment();
                
                // Alert if timeouts exceed threshold
                if (getTimeoutRate(datacenterId) > 0.05) { // 5% timeout rate
                    alertingService.sendAlert(
                        "High connection pool timeout rate for " + datacenterId,
                        AlertLevel.WARNING
                    );
                }
            }
        });
        
        // Periodic health checks
        Schedulers.newSingle("pool-monitor").schedulePeriodically(() -> {
            Map<String, ConnectionPoolMetrics> allMetrics = client.getAllConnectionPoolMetrics();
            for (Map.Entry<String, ConnectionPoolMetrics> entry : allMetrics.entrySet()) {
                String datacenterId = entry.getKey();
                ConnectionPoolMetrics metrics = entry.getValue();
                
                // Check pool health
                if (metrics.getUtilizationPercentage() > 90) {
                    logger.warn("Pool utilization high for {}: {}%", 
                        datacenterId, metrics.getUtilizationPercentage());
                }
                
                if (metrics.getEfficiencyRatio() < 0.95) {
                    logger.warn("Pool efficiency low for {}: {}", 
                        datacenterId, metrics.getEfficiencyRatio());
                }
            }
        }, 0, 30, TimeUnit.SECONDS);
    }
    
    private double getAveragePoolUtilization() {
        return client.getAggregatedPoolMetrics().getOverallUtilization();
    }
}
```

#### Production Deployment Example

```java
// Production-ready configuration with full observability
@Configuration
@EnableConfigurationProperties(RedisMultiDcProperties.class)
public class RedisMultiDcConfiguration {
    
    @Bean
    public MultiDatacenterRedisClient redisClient(
            RedisMultiDcProperties properties,
            MeterRegistry meterRegistry) {
        
        List<DatacenterEndpoint> endpoints = properties.getDatacenters().stream()
            .map(dc -> DatacenterEndpoint.builder()
                .id(dc.getId())
                .region(dc.getRegion())
                .host(dc.getHost())
                .port(dc.getPort())
                .ssl(dc.isSsl())
                .password(dc.getPassword())
                .poolConfig(ConnectionPoolConfig.builder()
                    .maxPoolSize(dc.getPoolSize())
                    .minPoolSize(dc.getMinPoolSize())
                    .acquisitionTimeout(Duration.ofSeconds(dc.getAcquisitionTimeoutSeconds()))
                    .idleTimeout(Duration.ofMinutes(dc.getIdleTimeoutMinutes()))
                    .validateOnAcquire(true)
                    .build())
                .resilienceConfig(ResilienceConfig.builder()
                    .enableAllPatterns()
                    .circuitBreakerConfig(
                        dc.getCircuitBreakerFailureThreshold(),
                        Duration.ofSeconds(dc.getCircuitBreakerWaitDurationSeconds()),
                        dc.getCircuitBreakerSlidingWindowSize(),
                        dc.getCircuitBreakerMinimumCalls()
                    )
                    .retryConfig(
                        dc.getMaxRetries(),
                        Duration.ofMillis(dc.getRetryDelayMs())
                    )
                    .build())
                .build())
            .collect(Collectors.toList());
        
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .datacenters(endpoints)
            .localDatacenter(properties.getLocalDatacenter())
            .routingStrategy(properties.getRoutingStrategy())
            .healthCheckInterval(Duration.ofSeconds(properties.getHealthCheckIntervalSeconds()))
            .metricsRegistry(meterRegistry)
            .enableDetailedMetrics(true)
            .build();
        
        MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config);
        
        // Set up monitoring
        setupPoolMonitoring(client, meterRegistry);
        
        return client;
    }
    
    private void setupPoolMonitoring(MultiDatacenterRedisClient client, MeterRegistry registry) {
        // Register custom metrics
        Gauge.builder("redis.multidc.pools.total")
            .description("Total number of connection pools")
            .register(registry, client, c -> c.getAllConnectionPoolMetrics().size());
        
        Gauge.builder("redis.multidc.connections.total.active")
            .description("Total active connections across all pools")
            .register(registry, client, c -> c.getAggregatedPoolMetrics().getTotalActiveConnections());
    }
}
```

## Troubleshooting

### Common Issues

#### Connection Pool Exhaustion

**Problem**: Applications experiencing connection acquisition timeouts

**Symptoms**:
```
ConnectionPoolException: Failed to acquire connection within timeout
Pool utilization consistently > 95%
High number of acquisition timeouts in metrics
```

**Solution**:

```java
// Increase pool size
DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
    .connectionPoolSize(50)  // Increase from default
    .minPoolSize(15)         // Ensure minimum connections
    .acquisitionTimeout(Duration.ofSeconds(5))  // Increase timeout if needed
    .build();

// Monitor pool metrics to right-size
ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics("datacenter-id");
if (metrics.getUtilizationPercentage() > 90) {
    // Consider increasing pool size
    logger.warn("Pool utilization high: {}%", metrics.getUtilizationPercentage());
}

// Check for connection leaks
long createdCount = metrics.getTotalConnectionsCreated();
long destroyedCount = metrics.getTotalConnectionsDestroyed();
if (createdCount - destroyedCount > metrics.getMaxPoolSize() * 2) {
    logger.error("Possible connection leak detected");
}
```

#### Connection Timeouts

**Problem**: Operations timing out frequently

**Solution**:

```java
// Increase timeouts
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .connectionTimeout(Duration.ofSeconds(10))
    .requestTimeout(Duration.ofSeconds(15))
    .build();

// Check network connectivity
String pong = client.sync().ping("problematic-datacenter");
```

#### Circuit Breaker Tripping

**Problem**: Circuit breaker frequently opening

**Solution**:

```java
// Adjust circuit breaker thresholds
CircuitBreakerConfig cbConfig = CircuitBreakerConfig.builder()
    .failureThreshold(10)          // Increase threshold
    .recoveryTimeout(Duration.ofMinutes(2))  // Longer recovery time
    .build();

// Monitor circuit breaker state
client.subscribeToHealthChanges((datacenter, healthy) -> {
    if (!healthy) {
        logger.warn("Circuit breaker opened for datacenter: {}", datacenter.getId());
    }
});
```

#### Memory Leaks

**Problem**: Application memory usage growing over time

**Solution**:

```java
// Ensure proper client cleanup
try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
    // Use client
} // Automatic cleanup

// Monitor connection pools
long activeConnections = client.getMetricsCollector()
    .getActiveConnectionCount("datacenter-id");
```

#### Connection Pool Performance Issues

**Problem**: Slow connection acquisition or poor pool performance

**Symptoms**:
```
High average acquisition times (> 100ms)
Frequent pool validation failures
Low pool efficiency ratio (< 0.95)
```

**Solution**:

```java
// Optimize pool configuration
ConnectionPoolConfig optimizedConfig = ConnectionPoolConfig.builder()
    .lowLatency()                           // Use low-latency preset
    .maxPoolSize(30)                        // Right-size for workload
    .minPoolSize(10)                        // Pre-warm connections
    .acquisitionTimeout(Duration.ofSeconds(1))  // Fast acquisition
    .validateOnAcquire(false)               // Disable if not needed
    .validatePeriodically(true)             // Use periodic validation instead
    .validationInterval(Duration.ofMinutes(5))
    .evictionInterval(Duration.ofMinutes(2))
    .build();

// Monitor and tune based on metrics
ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics("datacenter-id");
System.out.printf("Avg acquisition time: %.2f ms%n", metrics.getAverageAcquisitionTime());
System.out.printf("Efficiency ratio: %.3f%n", metrics.getEfficiencyRatio());

// If efficiency is low, check for:
// 1. Network issues to Redis
// 2. Redis server overload
// 3. Pool configuration mismatches with workload
```

#### Connection Validation Failures

**Problem**: High number of validation failures in pool metrics

**Solution**:

```java
// Check validation configuration
ConnectionPoolConfig config = ConnectionPoolConfig.builder()
    .validateOnAcquire(true)                // Validate before use
    .validateOnReturn(false)                // Skip return validation for performance
    .validatePeriodically(true)             // Background validation
    .validationInterval(Duration.ofMinutes(2))  // Tune validation frequency
    .evictionInterval(Duration.ofMinutes(2))
    .build();

// Monitor validation failures
ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics("datacenter-id");
long validationFailures = metrics.getTotalValidationFailures();
if (validationFailures > 0) {
    logger.warn("Validation failures detected: {}", validationFailures);
    // Check network connectivity and Redis server health
}

// Custom validation logic if needed
DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
    .customValidator(connection -> {
        try {
            return connection.ping().block(Duration.ofSeconds(1)) != null;
        } catch (Exception e) {
            return false;
        }
    })
    .build();
```

#### Performance Issues

**Problem**: Slow operation response times

**Solution**:

```java
// Use appropriate routing strategy
config.withRoutingStrategy(RoutingStrategy.LATENCY_BASED);

// Enable connection pooling
DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
    .connectionPoolSize(50)  // Increase pool size
    .build();

// Use async operations for better throughput
List<CompletableFuture<String>> futures = keys.stream()
    .map(key -> client.async().get(key))
    .collect(Collectors.toList());
```

### Debug Logging

Enable debug logging for detailed internal state and request/response logging.

```xml
<configuration>
    <logger name="com.redis.multidc" level="DEBUG"/>
    <logger name="io.lettuce.core" level="DEBUG"/>
</configuration>
```

### Metrics Dashboard

```java
// Export metrics to monitoring system
MeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

// Key metrics to monitor
Counter requestCount = Counter.builder("redis.requests")
    .tag("datacenter", datacenterId)
    .register(registry);

Timer requestDuration = Timer.builder("redis.duration")
    .tag("operation", operationType)
    .register(registry);
```

## FAQ

### General Questions

**Q: How does the library handle datacenter failures?**

A: The library uses circuit breakers and health monitoring to detect failures automatically. When a datacenter becomes unhealthy, the routing logic automatically fails over to available datacenters based on your configured strategy.

**Q: Can I use this library with Redis Cluster?**

A: Yes, the library works with Redis Cluster. Configure each cluster endpoint as a separate datacenter:

```java
DatacenterEndpoint clusterEndpoint = DatacenterEndpoint.builder()
       .id("cluster-dc-1")
    .host("redis-cluster.example.com")
    .port(6379)
    .clusterMode(true)
    .build();
```

**Q: How do I handle read/write splitting?**

A: Use datacenter preferences and routing strategies:

```java
// Force writes to primary
client.sync().set("key", "value", DatacenterPreference.LOCAL_ONLY);

// Allow reads from any datacenter
String value = client.sync().get("key", DatacenterPreference.ANY_AVAILABLE);
```

### Performance Questions

**Q: What's the optimal connection pool size for my use case?**

A: Pool size depends on your application's concurrency and latency requirements:

```java
// For high-concurrency applications (> 100 concurrent operations)
ConnectionPoolConfig highConcurrency = ConnectionPoolConfig.builder()
    .highThroughput()
    .maxPoolSize(50-100)
    .minPoolSize(20)
    .build();

// For low-latency applications (< 10ms response time requirements)
ConnectionPoolConfig lowLatency = ConnectionPoolConfig.builder()
    .lowLatency()
    .maxPoolSize(30)
    .minPoolSize(15)
    .acquisitionTimeout(Duration.ofMillis(500))
    .build();

// Monitor and adjust based on metrics
ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics("datacenter-id");
double utilization = metrics.getUtilizationPercentage();
if (utilization > 85) {
    // Consider increasing pool size
} else if (utilization < 20) {
    // Consider decreasing pool size to save resources
}
```

**Q: How do I monitor connection pool health in production?**

A: Use the comprehensive metrics provided:

```java
// Set up monitoring dashboard
public class PoolDashboard {
    public void displayMetrics() {
        // Key metrics to monitor
        AggregatedPoolMetrics aggregated = client.getAggregatedPoolMetrics();
        
        System.out.printf("Overall Pool Health:%n");
        System.out.printf("  Total Active: %d%n", aggregated.getTotalActiveConnections());
        System.out.printf("  Overall Utilization: %.1f%%%n", aggregated.getOverallUtilization());
        System.out.printf("  Total Pools: %d%n", aggregated.getTotalPools());
        
        // Per-datacenter metrics
        Map<String, ConnectionPoolMetrics> allMetrics = client.getAllConnectionPoolMetrics();
        for (Map.Entry<String, ConnectionPoolMetrics> entry : allMetrics.entrySet()) {
            ConnectionPoolMetrics metrics = entry.getValue();
            System.out.printf("Datacenter %s:%n", entry.getKey());
            System.out.printf("  Utilization: %.1f%%%n", metrics.getUtilizationPercentage());
            System.out.printf("  Efficiency: %.3f%n", metrics.getEfficiencyRatio());
            System.out.printf("  Avg Acquisition: %.2f ms%n", metrics.getAverageAcquisitionTime());
        }
    }
}

// Set up alerts
client.subscribeToPoolEvents((datacenterId, event, details) -> {
    switch (event) {
        case POOL_EXHAUSTED:
            alertingService.sendCriticalAlert("Pool exhausted: " + datacenterId);
            break;
        case HIGH_ACQUISITION_TIME:
            alertingService.sendWarningAlert("Slow acquisitions: " + datacenterId);
            break;
        case VALIDATION_FAILURE_SPIKE:
            alertingService.sendWarningAlert("Validation failures: " + datacenterId);
            break;
    }
});
```

**Q: What's the best routing strategy for my use case?**

A: It depends on your requirements:

- **LATENCY_BASED**: Best for read-heavy workloads requiring fast response times
- **PRIORITY_BASED**: Good for predictable routing with manual failover control
- **LOCAL_PREFERRED**: Ideal for data locality requirements
- **WEIGHTED_ROUND_ROBIN**: Best for load distribution across multiple equal datacenters

**Q: How many connections should I configure per datacenter?**

A: Start with 10-20 connections per datacenter and adjust based on load:

```java
// Monitor connection utilization
long activeConnections = client.getConnectionPoolMetrics("datacenter-id").getActiveConnections();
long totalConnections = client.getConnectionPoolMetrics("datacenter-id").getMaxPoolSize();
double utilization = (double) activeConnections / totalConnections;

// Adjust if utilization consistently > 80%
if (utilization > 0.8) {
    // Increase pool size
} else if (utilization < 0.2) {
    // Decrease pool size to save resources
}
```

**Q: How do connection pools interact with resilience patterns?**

A: Connection pools work seamlessly with all resilience patterns:

```java
// Circuit breakers protect pools from cascading failures
ResilienceConfig config = ResilienceConfig.builder()
    .circuitBreakerConfig(
        30.0f,                              // Failure threshold
        Duration.ofSeconds(60),             // Recovery time
        100,                                // Sliding window
        10                                  // Minimum calls
    )
    .build();

// When circuit breaker opens:
// 1. Pool stops creating new connections to failed datacenter
// 2. Existing connections are drained gracefully
// 3. Traffic routes to healthy datacenters
// 4. Pool recovers when circuit breaker closes

// Rate limiters control connection acquisition
.rateLimiterConfig(
    1000,                                   // Max operations per second
    Duration.ofSeconds(1),                  // Period
    Duration.ofMillis(100)                  // Timeout
)

// Bulkheads isolate connection usage
.bulkheadConfig(
    50,                                     // Max concurrent operations
    Duration.ofMillis(100)                  // Max wait time
)
```

**Q: What happens when a datacenter fails?**

A: The connection pool handles failures gracefully:

```java
// Automatic failure detection
client.subscribeToHealthChanges((datacenter, healthy) -> {
    if (!healthy) {
        // Pool automatically:
        // 1. Stops accepting new requests
        // 2. Drains existing connections
        // 3. Routes traffic to healthy datacenters
        // 4. Continues health monitoring for recovery
        
        logger.warn("Datacenter {} failed, pool drained", datacenter.getId());
    } else {
        // Pool automatically:
        // 1. Re-enables the datacenter
        // 2. Pre-warms minimum connections
        // 3. Starts accepting traffic again
        
        logger.info("Datacenter {} recovered, pool restored", datacenter.getId());
    }
});

// Manual pool management during maintenance
client.drainConnectionPool("maintenance-datacenter");  // Graceful drain
// Perform maintenance
client.restoreConnectionPool("maintenance-datacenter"); // Restore when ready
```

## Advanced Examples

This library includes comprehensive examples demonstrating advanced usage patterns:

### 🔗 [Connection Pool Management](lib/src/main/java/com/redis/multidc/example/ConnectionPoolManagementExample.java)

Advanced connection pool configuration, monitoring, health checks, and optimization strategies for production deployments.

### ⚡ [Reactive Streaming Operations](lib/src/main/java/com/redis/multidc/example/ReactiveStreamingExample.java)

Comprehensive reactive programming patterns including backpressure handling, error recovery, and streaming data processing with Project Reactor.

### 📊 [Health Monitoring & Events](lib/src/main/java/com/redis/multidc/example/HealthMonitoringExample.java)

Real-time health monitoring, event subscription, datacenter failover detection, and operational observability patterns.

### 🚀 [Production Usage Patterns](lib/src/main/java/com/redis/multidc/example/ProductionUsageExample.java)

Enterprise-ready configuration with SSL/TLS, authentication, monitoring, resilience patterns, and operational best practices.

### 💾 [Data Locality & Tombstone Management](lib/src/main/java/com/redis/multidc/example/DataLocalityManagerExample.java)

Advanced data locality patterns, tombstone key management, cache invalidation strategies, and distributed cache coherence.

Each example is fully documented with inline comments explaining the patterns, best practices, and production considerations.
