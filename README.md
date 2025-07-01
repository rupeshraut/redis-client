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
- **Automatic Reconnection**: Transparent reconnection with multi-layer resilience (Lettuce client, connection pools, and datacenter routing)
- **Circuit Breaker Pattern**: Built-in fault tolerance with configurable thresholds
- **Health Monitoring**: Continuous datacenter health checks and automatic failover
- **Retry Logic**: Configurable retry mechanisms with exponential backoff
- **Production-Ready Fallback Strategies**: Comprehensive fallback behaviors for datacenter failures
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

For production environments, you can configure comprehensive resilience patterns with fallback strategies:

```java
import com.redis.multidc.config.ResilienceConfig;
import com.redis.multidc.config.FallbackConfiguration;
import com.redis.multidc.config.FallbackStrategy;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.RetryConfig;

// Production-ready resilience configuration with fallback
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

// Configure comprehensive fallback strategy
FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
    .strategy(FallbackStrategy.NEXT_AVAILABLE)         // Most common production strategy
    .fallbackTimeout(Duration.ofSeconds(5))            // Max time for fallback attempts
    .maxFallbackAttempts(3)                            // Max fallback attempts
    .fallbackRetryDelay(Duration.ofMillis(100))        // Delay between fallback attempts
    .fallbackDatacenterOrder(List.of("us-west-2", "eu-central-1")) // Specific fallback order
    .build();

// Use in configuration
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .datacenters(/* ... */)
    .resilienceConfig(resilienceConfig)                 // Apply resilience config
    .fallbackConfiguration(fallbackConfig)             // Apply fallback config
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
    .resilienceConfig(ResilienceConfig.defaultConfig()) // Modern approach
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

## Fallback Strategies

The library provides comprehensive fallback strategies to handle datacenter failures and ensure high availability. These strategies define how the client behaves when the preferred datacenter is unavailable.

### Available Fallback Strategies

#### `NEXT_AVAILABLE` (Recommended for Production)
Falls back to the next available datacenter based on priority or latency. Most common strategy for production environments.

```java
FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
    .strategy(FallbackStrategy.NEXT_AVAILABLE)
    .fallbackTimeout(Duration.ofSeconds(5))
    .maxFallbackAttempts(3)
    .fallbackRetryDelay(Duration.ofMillis(100))
    .build();
```

#### `TRY_ALL`
Tries all datacenters in order of preference until one succeeds. More resilient but may introduce higher latency.

```java
FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
    .strategy(FallbackStrategy.TRY_ALL)
    .fallbackTimeout(Duration.ofSeconds(10))
    .maxFallbackAttempts(5)
    .fallbackDatacenterOrder(List.of("us-west-2", "eu-central-1", "us-east-1"))
    .build();
```

#### `LOCAL_ONLY`
Fallback to local datacenter only, ensuring data locality in failure scenarios.

```java
FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
    .strategy(FallbackStrategy.LOCAL_ONLY)
    .fallbackTimeout(Duration.ofSeconds(3))
    .build();
```

#### `REMOTE_ONLY`
Fallback to remote datacenters only, avoiding the local datacenter when issues are suspected.

```java
FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
    .strategy(FallbackStrategy.REMOTE_ONLY)
    .fallbackTimeout(Duration.ofSeconds(5))
    .build();
```

#### `BEST_EFFORT`
Use cached/stale data from any available datacenter when preferred is unavailable. Prioritizes availability over consistency.

```java
FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
    .strategy(FallbackStrategy.BEST_EFFORT)
    .enableStaleReads(true)
    .staleReadTolerance(Duration.ofMinutes(5))
    .fallbackTimeout(Duration.ofSeconds(2))
    .build();
```

#### `QUEUE_AND_RETRY`
Queue operations and retry when datacenters become available. Includes configurable timeout and queue size limits.

```java
FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
    .strategy(FallbackStrategy.QUEUE_AND_RETRY)
    .queueSize(1000)
    .queueTimeout(Duration.ofSeconds(5))
    .fallbackTimeout(Duration.ofSeconds(30))
    .maxFallbackAttempts(5)
    .fallbackRetryDelay(Duration.ofSeconds(2))
    .build();
```

#### `CUSTOM`
Custom fallback strategy with pluggable logic.

```java
FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
    .strategy(FallbackStrategy.CUSTOM)
    .customFallbackLogic(() -> {
        // Custom logic: prefer EU datacenter during US business hours
        LocalTime now = LocalTime.now();
        if (now.getHour() >= 9 && now.getHour() <= 17) {
            return "eu-central-1";
        } else {
            return "us-west-2";
        }
    })
    .build();
```

#### `FAIL_FAST`
No fallback - fail immediately if preferred datacenter is unavailable. Operations will fail with an exception.

```java
FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
    .strategy(FallbackStrategy.FAIL_FAST)
    .build();
```

### Fallback Configuration in Practice

```java
// Complete configuration with fallback strategy
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .datacenters(List.of(
        DatacenterEndpoint.builder()
            .id("us-east-1")
            .host("redis-us-east.example.com")
            .port(6379)
            .priority(1)
            .build(),
        DatacenterEndpoint.builder()
            .id("us-west-2")
            .host("redis-us-west.example.com")
            .port(6379)
            .priority(2)
            .build()
    ))
    .localDatacenter("us-east-1")
    .routingStrategy(RoutingStrategy.LATENCY_BASED)
    .fallbackStrategy(FallbackStrategy.NEXT_AVAILABLE)  // Simple fallback configuration
    .fallbackConfiguration(FallbackConfiguration.builder() // Or detailed configuration
        .strategy(FallbackStrategy.TRY_ALL)
        .fallbackTimeout(Duration.ofSeconds(10))
        .maxFallbackAttempts(3)
        .fallbackDatacenterOrder(List.of("us-west-2", "eu-central-1"))
        .build())
    .build();

// Use the client - fallback is automatic
try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
    // Operations automatically use fallback strategy when needed
    client.sync().set("key", "value", DatacenterPreference.LOCAL_PREFERRED);
    String value = client.sync().get("key", DatacenterPreference.LOCAL_PREFERRED);
}
```

### Fallback Strategy Examples

For comprehensive examples of all fallback strategies, see:
**🔄 [Fallback Strategy Example](lib/src/main/java/com/redis/multidc/example/FallbackStrategyExample.java)**

This example demonstrates:
- Production-ready fallback configurations
- Different fallback strategies for various scenarios  
- Error handling and resilience patterns
- Custom fallback logic implementation
- Performance and availability trade-offs

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
    .resilienceConfig(ResilienceConfig.builder()
        .circuitBreaker(CircuitBreakerConfig.custom()
            .failureRateThreshold(50.0f)              // 50% failure rate threshold
            .waitDurationInOpenState(Duration.ofSeconds(30)) // Recovery timeout
            .slidingWindowSize(10)                     // Sliding window for evaluation
            .minimumNumberOfCalls(5)                   // Min calls before evaluation
            .build())
        .retry(RetryConfig.custom()
            .maxAttempts(3)
            .waitDuration(Duration.ofMillis(100))
            .build())
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

## Automatic Reconnection

The Redis Multi-Datacenter Client Library provides **comprehensive automatic reconnection capabilities** built on top of Lettuce's robust reconnection features, ensuring your application maintains connectivity even during network disruptions or Redis server restarts.

### 🔄 How Automatic Reconnection Works

#### Multi-Layer Reconnection Strategy

The library implements a sophisticated multi-layer approach to handle connection failures:

**Layer 1: Lettuce Client Level**
- Automatic reconnection when connections are lost
- Connection validation with ping checks
- Command queueing during reconnection
- Graceful protocol failure handling

**Layer 2: Connection Pool Level**  
- Continuous validation of pooled connections
- Automatic replacement of failed connections
- Background maintenance and cleanup
- Health metrics tracking

**Layer 3: Datacenter Router Level**
- Intelligent failover to healthy datacenters
- Circuit breaker protection
- Real-time health monitoring
- Smart traffic routing

### ⚙️ Configuration

#### Basic Reconnection Settings

```java
// Auto-reconnect is enabled by default
ConnectionFactoryConfig config = ConnectionFactoryConfig.builder()
    .autoReconnect(true)                                    // ✅ Enable automatic reconnection
    .connectionTimeout(Duration.ofSeconds(10))              // Initial connection timeout
    .commandTimeout(Duration.ofSeconds(5))                  // Command execution timeout
    .validationTimeout(Duration.ofSeconds(3))               // Connection validation timeout
    .pingBeforeActivateConnection(true)                     // Validate before use
    .cancelCommandsOnReconnectFailure(false)                // Commands survive reconnection
    .build();
```

#### Advanced Resilience Configuration

```java
// Comprehensive reconnection with fallback strategies
MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create()
    .datacenterConfiguration(DatacenterConfiguration.builder()
        .connectionTimeout(Duration.ofSeconds(10))
        .requestTimeout(Duration.ofSeconds(15))
        .healthCheckInterval(Duration.ofSeconds(30))        // Health check frequency
        
        // Connection pool settings
        .addDatacenter(DatacenterEndpoint.builder()
            .id("primary-dc")
            .host("redis.primary.example.com")
            .port(6379)
            .connectionPoolSize(20)                         // Pool size
            .minPoolSize(5)                                 // Minimum connections
            .acquisitionTimeout(Duration.ofSeconds(2))      // Pool acquisition timeout
            .idleTimeout(Duration.ofMinutes(10))            // Keep connections alive
            .maxConnectionAge(Duration.ofHours(1))          // Periodic connection rotation
            .validateOnAcquire(true)                        // Validate before use
            .build())
        
        // Fallback strategies for connection failures
        .fallbackConfiguration(FallbackConfiguration.builder()
            .strategy(FallbackStrategy.NEXT_AVAILABLE)      // Auto-fallback to healthy DC
            .maxRetries(3)
            .retryDelay(Duration.ofMillis(200))
            .queueSize(1000)                                // Queue operations during outages
            .build())
        
        // Circuit breaker and resilience patterns
        .resilienceConfig(ResilienceConfig.builder()
            .circuitBreaker(CircuitBreakerConfig.custom()
                .failureRateThreshold(30.0f)               // 30% failure rate triggers circuit
                .waitDurationInOpenState(Duration.ofSeconds(60))  // Recovery time
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .build())
            .retry(RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(100))
                .retryOnException(throwable -> 
                    throwable instanceof RedisConnectionException)
                .build())
            .build())
    .build();
```

### 🔍 Connection Pool Behavior

#### Automatic Connection Management

The connection pool automatically handles the entire connection lifecycle:

```java
public class DefaultConnectionPool {
    
    // Validates connections before use
    private boolean isConnectionValid(PooledConnectionImpl connection) {
        try {
            // Performs ping validation with configurable timeout
            String result = connection.async().ping()
                .get(validationTimeout.toMillis(), TimeUnit.MILLISECONDS);
            return "PONG".equals(result) && connection.isValid();
        } catch (Exception e) {
            // Failed connections are automatically marked for removal
            totalValidationFailures.incrementAndGet();
            return false;
        }
    }
    
    // Background maintenance removes failed connections and creates new ones
    public void maintainPool() {
        // Remove expired/failed connections
        allConnections.entrySet().removeIf(entry -> {
            PooledConnectionImpl connection = entry.getKey();
            boolean expired = isConnectionExpired(connection, currentTime);
            if (expired) {
                destroyConnection(connection);  // Clean up failed connection
                logger.debug("Removed expired connection for datacenter: {}", datacenterId);
            }
            return expired;
        });
        
        // Ensure minimum pool size (creates new connections automatically)
        while (allConnections.size() < config.getMinPoolSize()) {
            try {
                PooledConnectionImpl connection = createConnection();
                availableConnections.offer(connection);
                logger.debug("Created new connection to maintain minimum pool size");
            } catch (Exception e) {
                logger.warn("Failed to create connection during maintenance: {}", e.getMessage());
                break; // Stop trying if creation fails
            }
        }
    }
}
```

#### Pool Events During Reconnection

```java
// Monitor pool events to understand reconnection activity
client.subscribeToPoolEvents((datacenterId, event, details) -> {
    switch (event) {
        case CONNECTION_CREATED:
            logger.info("New connection created for datacenter: {} (likely due to reconnection)", datacenterId);
            break;
        case CONNECTION_DESTROYED:
            logger.info("Failed connection destroyed for datacenter: {} (automatic cleanup)", datacenterId);
            break;
        case VALIDATION_FAILED:
            logger.warn("Connection validation failed for datacenter: {} (will be replaced)", datacenterId);
            break;
        case ACQUISITION_TIMEOUT:
            logger.warn("Connection acquisition timeout for datacenter: {} (may indicate issues)", datacenterId);
            break;
    }
});
```

### 📊 Monitoring Reconnection Activity

#### Connection Health Metrics

```java
// Monitor connection health and reconnection activity
ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics("datacenter-id");

// Key metrics for understanding reconnection behavior
long connectionsCreated = metrics.getTotalConnectionsCreated();
long connectionsDestroyed = metrics.getTotalConnectionsDestroyed();
long validationFailures = metrics.getTotalValidationFailures();
double utilizationPercent = metrics.getUtilizationPercentage();

// Log reconnection activity
if (validationFailures > 0) {
    logger.info("Connection failures detected: {} (auto-reconnecting)", validationFailures);
}

// Check for connection churn (may indicate network issues)
long connectionChurn = connectionsCreated - connectionsDestroyed;
if (connectionChurn > metrics.getMaxPoolSize() * 2) {
    logger.warn("High connection churn detected: created={}, destroyed={}", 
        connectionsCreated, connectionsDestroyed);
}

// Monitor pool health
boolean isHealthy = client.isConnectionPoolHealthy("datacenter-id");
if (!isHealthy) {
    logger.warn("Connection pool unhealthy for datacenter: {}", "datacenter-id");
}
```

#### Real-Time Health Monitoring

```java
// Subscribe to datacenter health changes
client.subscribeToHealthChanges((datacenter, healthy) -> {
    if (!healthy) {
        logger.warn("Datacenter {} unhealthy - automatic failover activated. " +
                   "Connections will be re-established automatically", datacenter.getId());
    } else {
        logger.info("Datacenter {} recovered - connections restored and traffic resumed", 
                   datacenter.getId());
    }
});

// Monitor circuit breaker state changes
client.subscribeToCircuitBreakerEvents((datacenterId, state) -> {
    switch (state) {
        case OPEN:
            logger.warn("Circuit breaker OPENED for datacenter: {} - stopping requests temporarily", datacenterId);
            break;
        case HALF_OPEN:
            logger.info("Circuit breaker HALF_OPEN for datacenter: {} - testing reconnection", datacenterId);
            break;
        case CLOSED:
            logger.info("Circuit breaker CLOSED for datacenter: {} - normal operation resumed", datacenterId);
            break;
    }
});
```

### 🛡️ Application-Level Resilience

#### Handling Temporary Connection Failures

Your application code doesn't need special handling for reconnection - it happens transparently:

```java
public class ResilientRedisService {
    private final MultiDatacenterRedisClient client;
    
    public Optional<String> getValue(String key) {
        try {
            // This operation will automatically:
            // 1. Use a healthy connection from the pool
            // 2. Validate the connection before use  
            // 3. Automatically failover to another datacenter if needed
            // 4. Queue the operation if all connections are temporarily down
            return Optional.ofNullable(
                client.sync().get(key, DatacenterPreference.LOCAL_PREFERRED));
                
        } catch (RedisConnectionException e) {
            // This is rare - only happens if all datacenters are down
            logger.warn("All datacenters temporarily unavailable for key: {}", key, e);
            return Optional.empty();
            
        } catch (TimeoutException e) {
            // Operation timed out - may retry automatically based on configuration
            logger.warn("Operation timed out for key: " + key, e);
            return Optional.empty();
        }
    }
    
    public void setValue(String key, String value) {
        try {
            // Write operations also benefit from automatic reconnection
            client.sync().set(key, value, DatacenterPreference.LOCAL_PREFERRED);
            logger.debug("Successfully set key: {}", key);
            
        } catch (Exception e) {
            // With proper fallback configuration, this is very rare
            logger.error("Failed to set key after all retry attempts: {}", key, e);
            throw new ServiceException("Unable to store data", e);
        }
    }
}
```

#### Proactive Connection Management

```java
// Warm up connections proactively
CompletableFuture<Void> warmupFuture = client.warmUpConnections();
warmupFuture.thenRun(() -> {
    logger.info("All connection pools warmed up and ready");
}).exceptionally(throwable -> {
    logger.warn("Some connection pools failed to warm up: {}", throwable.getMessage());
    return null;
});

// Periodic health checks (optional - automatic health monitoring is always active)
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    Map<String, Boolean> healthStatus = client.getConnectionPoolHealth();
    healthStatus.forEach((datacenterId, healthy) -> {
        if (!healthy) {
            logger.warn("Datacenter {} connection pool unhealthy - automatic recovery in progress", 
                       datacenterId);
        }
    });
}, 0, 30, TimeUnit.SECONDS);
```

### 🎯 Best Practices

#### 1. Configure Appropriate Timeouts

```java
// Balance responsiveness with stability
DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
    .id("production-dc")
    .host("redis.prod.example.com")
    .port(6379)
    .connectionTimeout(Duration.ofSeconds(10))              // Allow time for reconnection
    .acquisitionTimeout(Duration.ofSeconds(5))              // Pool acquisition timeout
    .idleTimeout(Duration.ofMinutes(10))                    // Keep connections alive
    .maxConnectionAge(Duration.ofHours(1))                  // Periodic rotation
    .validateOnAcquire(true)                                // Always validate
    .build();
```

#### 2. Use Circuit Breakers for Protection

```java
// Protect against cascading failures during reconnection storms
ResilienceConfig resilience = ResilienceConfig.builder()
    .circuitBreakerConfig(
        30.0f,                                              // 30% failure rate threshold
        Duration.ofSeconds(60),                             // Open state duration
        100,                                                // Sliding window size
        10                                                  // Minimum calls before evaluation
    )
    .retryConfig(
        3,                                                  // Max retry attempts
        Duration.ofMillis(200)                              // 200ms between retries
    )
    .build();
```

#### 3. Monitor Connection Metrics

```java
// Set up monitoring dashboards
public class ConnectionMonitoringService {
    
    public void logConnectionStatus() {
        Map<String, ConnectionPoolMetrics> allMetrics = client.getAllConnectionPoolMetrics();
        
        allMetrics.forEach((datacenterId, metrics) -> {
            logger.info("Datacenter: {} | Active: {}/{} | Created: {} | Destroyed: {} | Failures: {}", 
                datacenterId,
                metrics.getActiveConnections(),
                metrics.getMaxPoolSize(),
                metrics.getTotalConnectionsCreated(),
                metrics.getTotalConnectionsDestroyed(),
                metrics.getTotalValidationFailures()
            );
        });
    }
    
    public void alertOnConnectionIssues() {
        Map<String, ConnectionPoolMetrics> allMetrics = client.getAllConnectionPoolMetrics();
        
        allMetrics.forEach((datacenterId, metrics) -> {
            double utilization = metrics.getUtilizationPercentage();
            long failures = metrics.getTotalValidationFailures();
            
            if (utilization > 90) {
                logger.warn("HIGH UTILIZATION: Datacenter {} pool at {}% capacity", 
                           datacenterId, utilization);
            }
            
            if (failures > 10) {
                logger.warn("HIGH FAILURE RATE: Datacenter {} has {} connection failures", 
                           datacenterId, failures);
            }
        });
    }
}
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
    
    public void alertOnPoolIssues() {
        // Monitor and alert on key metrics
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
    }
}
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
        1000,                                   // Permits per period
        Duration.ofSeconds(1),                  // Period duration
        Duration.ofMillis(100)                  // Timeout for permit acquisition
    )
    .bulkheadConfig(
        50,                                     // Max concurrent calls
        Duration.ofMillis(100)                  // Max wait duration
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

## Implementation Status

### ✅ **Fully Implemented Features**
- Multi-datacenter configuration and routing
- Synchronous, asynchronous, and reactive operations  
- **Automatic reconnection** with transparent multi-layer resilience (Lettuce client, connection pools, datacenter routing)
- Comprehensive resilience patterns (Circuit Breaker, Retry, Rate Limiter, Bulkhead, Time Limiter)
- **Production-ready fallback strategies** (NEW - 8 different strategies including NEXT_AVAILABLE, TRY_ALL, BEST_EFFORT, etc.)
- Connection pooling with monitoring and metrics
- Health monitoring and event subscription
- Data locality management and tombstone key management
- SSL/TLS support and authentication
- Comprehensive observability with Micrometer metrics

### 🚧 **Features in Development**
- Custom authentication modes beyond password/no-auth
- Advanced SSL configuration options
- Distributed tracing integration

### 📋 **Roadmap**
- Automatic data migration based on access patterns
- Advanced conflict resolution for CRDB scenarios
- Enhanced monitoring dashboards and alerting
- Performance optimization recommendations

The library is production-ready for the implemented features. All examples and documentation reflect only the currently available functionality.
```
