# DataLocalityManager and TombstoneKeyManager Usage Guide

This guide shows how to effectively use the DataLocalityManager and TombstoneKeyManager in your Redis Multi-Datacenter Client applications.

## Table of Contents

1. [Overview](#overview)
2. [DataLocalityManager](#datalocalitymanager)
3. [TombstoneKeyManager](#tombstonekeymanager)
4. [Integration Patterns](#integration-patterns)
5. [Best Practices](#best-practices)
6. [Configuration Examples](#configuration-examples)

## Overview

The Redis Multi-Datacenter Client provides two powerful managers for advanced data management:

- **DataLocalityManager**: Intelligent data placement and access optimization
- **TombstoneKeyManager**: Advanced key lifecycle management with soft deletion and distributed locking

## DataLocalityManager

### Purpose
Optimizes data placement across datacenters based on access patterns, latency, and usage analytics.

### Key Features
- 📊 **Access Pattern Analysis**: Tracks read/write patterns per datacenter
- 🎯 **Optimal Datacenter Selection**: Recommends best datacenter for reads/writes
- 🚀 **Migration Recommendations**: Suggests when to move data for better performance
- ⚡ **Automatic Optimization**: Optional auto-migration based on thresholds
- 📈 **Real-time Statistics**: Monitoring and health metrics

### Basic Usage

```java
// 1. Create configuration
DataLocalityManager.DataLocalityConfig config = DataLocalityManager.DataLocalityConfig.builder()
    .analysisInterval(Duration.ofMinutes(15))         // Analyze every 15 minutes
    .optimizationInterval(Duration.ofHours(1))        // Optimize every hour
    .migrationLatencyThreshold(Duration.ofMillis(50)) // Migrate if >50ms improvement
    .minAccessesForAnalysis(10)                       // Need 10+ accesses to analyze
    .minAccessesForMigration(50)                      // Need 50+ accesses to migrate
    .autoMigrationEnabled(false)                      // Manual migration for safety
    .build();

// 2. Create manager
DataLocalityManager localityManager = new DataLocalityManager(
    client, router, datacenterConfig, config
);

// 3. Record access patterns
localityManager.recordAccess("user:12345", "us-east-1", "GET", 15); // 15ms latency
localityManager.recordAccess("user:12345", "eu-west-1", "GET", 45); // 45ms latency

// 4. Get optimal datacenter recommendations
CompletableFuture<Optional<String>> optimalDC = 
    localityManager.getOptimalReadDatacenter("user:12345");

optimalDC.thenAccept(dc -> {
    if (dc.isPresent()) {
        System.out.println("Best datacenter for reads: " + dc.get());
    }
});

// 5. Analyze migration opportunities
CompletableFuture<DataLocalityManager.MigrationRecommendation> analysis = 
    localityManager.analyzeMigrationOpportunity("user:12345");

analysis.thenAccept(recommendation -> {
    if (recommendation.isRecommended()) {
        System.out.println("Migration recommended: " + recommendation);
        
        // Perform migration
        localityManager.migrateData(
            recommendation.getKey(),
            recommendation.getCurrentDatacenter(),
            recommendation.getTargetDatacenter()
        );
    }
});

// 6. Get statistics
DataLocalityManager.LocalityStats stats = localityManager.getLocalityStats();
System.out.println("Optimization ratio: " + stats.getOptimizationRatio());
```

### Advanced Usage Patterns

#### Smart Cache Placement
```java
// Store cache data in optimal location
String cacheKey = "cache:expensive-data";

// Record where cache is accessed from
localityManager.recordAccess(cacheKey, "us-east-1", "GET", 20);
localityManager.recordAccess(cacheKey, "us-west-1", "GET", 80);
localityManager.recordAccess(cacheKey, "eu-west-1", "GET", 120);

// Get optimal placement
CompletableFuture<Optional<String>> optimalDC = 
    localityManager.getOptimalReadDatacenter(cacheKey);

optimalDC.thenAccept(dc -> {
    if (dc.isPresent()) {
        // Store cache in optimal datacenter
        client.sync().set(cacheKey, "cached-data", DatacenterPreference.SPECIFIC_DATACENTER);
    }
});
```

#### Multi-Region Session Management
```java
// Track user session access across regions
String sessionKey = "session:" + userId;

// User accesses from different locations
localityManager.recordAccess(sessionKey, getCurrentUserRegion(), "GET", latency);

// Find best datacenter for user's session
CompletableFuture<Optional<String>> sessionDC = 
    localityManager.getOptimalReadDatacenter(sessionKey);

// Store session in optimal location
sessionDC.thenAccept(dc -> {
    if (dc.isPresent()) {
        storeSessionInDatacenter(sessionKey, sessionData, dc.get());
    }
});
```

## TombstoneKeyManager

### Purpose
Provides advanced key lifecycle management including soft deletion, cache invalidation, and distributed locking.

### Key Features
- 🗑️ **Soft Deletion**: Mark keys as deleted without immediate removal
- ⚡ **Cache Invalidation**: Invalidate keys across all datacenters
- 🔒 **Distributed Locking**: Cross-datacenter distributed locks
- 🧹 **Automatic Cleanup**: Background cleanup of expired tombstones
- 📊 **Tombstone Analytics**: Track deletion patterns and statistics

### Basic Usage

```java
// 1. Create configuration
TombstoneKeyManager.TombstoneConfig config = TombstoneKeyManager.TombstoneConfig.builder()
    .defaultTombstoneTtl(Duration.ofDays(1))    // Keep tombstones for 1 day
    .cleanupInterval(Duration.ofHours(6))       // Cleanup every 6 hours
    .batchSize(100)                             // Process 100 keys per cleanup
    .lockTimeout(Duration.ofMinutes(5))         // Lock timeout
    .build();

// 2. Create manager
TombstoneKeyManager tombstoneManager = new TombstoneKeyManager(client, config);

// 3. Soft delete (keeps audit trail)
boolean deleted = tombstoneManager.softDelete(
    "user:12345",
    "user-deleted-gdpr",     // deletion reason
    Duration.ofDays(30),     // keep tombstone for 30 days
    true                     // remove original key
);

// 4. Check if key is soft deleted
boolean isSoftDeleted = tombstoneManager.isSoftDeleted("user:12345");

// 5. Cache invalidation
boolean invalidated = tombstoneManager.invalidateKey(
    "cache:product:123",
    "product-updated",       // invalidation reason
    Duration.ofMinutes(30)   // keep invalidation marker
);

// 6. Distributed locking
TombstoneKeyManager.DistributedLock lock = tombstoneManager.acquireLock(
    "lock:order-processing",
    "service-instance-1",    // lock owner
    Duration.ofMinutes(2)    // lock timeout
);

if (lock != null) {
    try {
        // Critical section work
        processOrder();
    } finally {
        lock.release();
    }
}
```

### Advanced Usage Patterns

#### GDPR Compliance with Audit Trail
```java
// Soft delete user data for GDPR compliance
public void deleteUserDataGDPR(String userId) {
    String userKey = "user:profile:" + userId;
    
    // Soft delete with audit trail
    boolean deleted = tombstoneManager.softDelete(
        userKey,
        "gdpr-deletion-request",
        Duration.ofDays(90),    // Keep audit trail for 90 days
        true                    // Remove personal data
    );
    
    if (deleted) {
        logger.info("User {} data soft deleted for GDPR compliance", userId);
        
        // Optional: Also invalidate related caches
        tombstoneManager.invalidateKey(
            "cache:user:" + userId,
            "user-data-deleted",
            Duration.ofHours(24)
        );
    }
}
```

#### Cache Invalidation with Propagation
```java
// Invalidate cache across all datacenters when data changes
public void updateProductAndInvalidateCache(String productId, Product product) {
    // Update product data
    String productKey = "product:" + productId;
    client.sync().set(productKey, serialize(product), DatacenterPreference.LOCAL_PREFERRED);
    
    // Invalidate all related caches
    List<String> cacheKeys = Arrays.asList(
        "cache:product:" + productId,
        "cache:category:" + product.getCategoryId(),
        "cache:search:products"
    );
    
    for (String cacheKey : cacheKeys) {
        tombstoneManager.invalidateKey(
            cacheKey,
            "product-updated:" + productId,
            Duration.ofMinutes(30)
        );
    }
}
```

#### Distributed Coordination
```java
// Coordinate work across multiple service instances
public void processOrderWithCoordination(String orderId) {
    String lockKey = "lock:order:" + orderId;
    
    TombstoneKeyManager.DistributedLock lock = tombstoneManager.acquireLock(
        lockKey,
        getServiceInstanceId(),
        Duration.ofMinutes(5)
    );
    
    if (lock != null) {
        try {
            // Only one instance processes this order
            Order order = loadOrder(orderId);
            
            if (order.getStatus() == OrderStatus.PENDING) {
                processOrder(order);
                
                // Extend lock if needed
                if (needMoreTime() && lock.isValid()) {
                    // Lock automatically extends on valid operations
                    continueProcessing(order);
                }
            }
            
        } finally {
            lock.release();
        }
    } else {
        logger.info("Order {} is being processed by another instance", orderId);
    }
}
```

## Integration Patterns

### Pattern 1: Smart Cache with Locality and Invalidation
```java
public class SmartCache {
    private final DataLocalityManager localityManager;
    private final TombstoneKeyManager tombstoneManager;
    private final MultiDatacenterRedisClient client;
    
    public <T> CompletableFuture<T> get(String key, Function<String, T> loader) {
        // Check if cache is invalidated
        if (tombstoneManager.isInvalidated(key)) {
            return reload(key, loader);
        }
        
        // Get from optimal datacenter
        return localityManager.getOptimalReadDatacenter(key)
            .thenCompose(optimalDC -> {
                if (optimalDC.isPresent()) {
                    localityManager.recordAccess(key, optimalDC.get(), "GET", getCurrentLatency());
                    return loadFromDatacenter(key, optimalDC.get());
                } else {
                    return reload(key, loader);
                }
            });
    }
    
    public void invalidate(String key, String reason) {
        tombstoneManager.invalidateKey(key, reason, Duration.ofMinutes(30));
    }
    
    private CompletableFuture<T> reload(String key, Function<String, T> loader) {
        T value = loader.apply(key);
        
        // Store in optimal datacenter
        return localityManager.getOptimalWriteDatacenter(key)
            .thenApply(optimalDC -> {
                if (optimalDC.isPresent()) {
                    storeInDatacenter(key, value, optimalDC.get());
                    localityManager.recordAccess(key, optimalDC.get(), "SET", getCurrentLatency());
                }
                return value;
            });
    }
}
```

### Pattern 2: Session Management with Soft Delete
```java
public class SessionManager {
    private final DataLocalityManager localityManager;
    private final TombstoneKeyManager tombstoneManager;
    
    public void createSession(String sessionId, String userId, String region) {
        String sessionKey = "session:" + sessionId;
        
        // Record where session is created
        localityManager.recordAccess(sessionKey, region, "SET", getCurrentLatency());
        
        // Store session data
        SessionData session = new SessionData(userId, Instant.now());
        client.sync().set(sessionKey, serialize(session), Duration.ofHours(8), 
            DatacenterPreference.SPECIFIC_DATACENTER);
    }
    
    public void logout(String sessionId, boolean keepAuditTrail) {
        String sessionKey = "session:" + sessionId;
        
        if (keepAuditTrail) {
            // Soft delete for audit
            tombstoneManager.softDelete(
                sessionKey,
                "user-logout",
                Duration.ofDays(30),  // Audit trail
                true                  // Remove session data
            );
        } else {
            // Hard delete
            client.sync().delete(sessionKey, DatacenterPreference.LOCAL_PREFERRED);
        }
    }
    
    public Optional<SessionData> getSession(String sessionId) {
        String sessionKey = "session:" + sessionId;
        
        // Check if session is soft deleted
        if (tombstoneManager.isSoftDeleted(sessionKey)) {
            return Optional.empty();
        }
        
        // Get from optimal datacenter
        return localityManager.getOptimalReadDatacenter(sessionKey)
            .thenApply(optimalDC -> {
                if (optimalDC.isPresent()) {
                    localityManager.recordAccess(sessionKey, optimalDC.get(), "GET", getCurrentLatency());
                    String data = client.sync().get(sessionKey, DatacenterPreference.SPECIFIC_DATACENTER);
                    return data != null ? Optional.of(deserialize(data)) : Optional.<SessionData>empty();
                }
                return Optional.<SessionData>empty();
            }).join();
    }
}
```

## Best Practices

### DataLocalityManager Best Practices

1. **Record All Access Patterns**
   ```java
   // Always record actual latency measurements
   long start = System.currentTimeMillis();
   String value = client.sync().get(key, preference);
   long latency = System.currentTimeMillis() - start;
   localityManager.recordAccess(key, datacenterId, "GET", latency);
   ```

2. **Use Conservative Migration Thresholds**
   ```java
   // Don't migrate for small improvements
   .migrationLatencyThreshold(Duration.ofMillis(100))  // At least 100ms improvement
   .minAccessesForMigration(50)                        // Need substantial access data
   ```

3. **Monitor Migration Success**
   ```java
   localityManager.migrateData(key, from, to)
       .thenAccept(success -> {
           if (success) {
               metricsCollector.incrementCounter("migration.success");
           } else {
               metricsCollector.incrementCounter("migration.failure");
               // Consider rollback or retry
           }
       });
   ```

### TombstoneKeyManager Best Practices

1. **Choose Appropriate TTLs**
   ```java
   // Short TTL for cache invalidation
   tombstoneManager.invalidateKey(cacheKey, reason, Duration.ofMinutes(30));
   
   // Long TTL for audit trails
   tombstoneManager.softDelete(userKey, "gdpr", Duration.ofDays(90), true);
   ```

2. **Always Release Locks**
   ```java
   TombstoneKeyManager.DistributedLock lock = tombstoneManager.acquireLock(key, owner, timeout);
   if (lock != null) {
       try {
           // Critical section
       } finally {
           lock.release(); // Always release in finally block
       }
   }
   ```

3. **Monitor Tombstone Growth**
   ```java
   TombstoneKeyManager.TombstoneStats stats = tombstoneManager.getStats();
   if (stats.getActiveTombstones() > 10000) {
       logger.warn("High number of active tombstones: {}", stats.getActiveTombstones());
       // Consider manual cleanup or configuration adjustment
   }
   ```

### Integration Best Practices

1. **Combine for Cache Management**
   - Use DataLocalityManager for optimal cache placement
   - Use TombstoneKeyManager for cache invalidation
   - Monitor both for comprehensive cache health

2. **Implement Graceful Degradation**
   ```java
   try {
       return localityManager.getOptimalReadDatacenter(key).get();
   } catch (Exception e) {
       logger.warn("Locality manager unavailable, falling back to default", e);
       return client.getDefaultDatacenter();
   }
   ```

3. **Use Circuit Breakers**
   - Both managers integrate with Resilience4j patterns
   - Configure appropriate timeouts and retry policies
   - Monitor and alert on manager health

## Configuration Examples

### Production Configuration
```java
// Data Locality Configuration for Production
DataLocalityManager.DataLocalityConfig localityConfig = DataLocalityManager.DataLocalityConfig.builder()
    .analysisInterval(Duration.ofMinutes(30))         // Less frequent analysis
    .optimizationInterval(Duration.ofHours(4))        // Conservative optimization
    .migrationLatencyThreshold(Duration.ofMillis(200)) // Significant improvement needed
    .minAccessesForAnalysis(25)                       // More data required
    .minAccessesForMigration(100)                     // High confidence needed
    .autoMigrationEnabled(false)                      // Manual approval required
    .build();

// Tombstone Configuration for Production
TombstoneKeyManager.TombstoneConfig tombstoneConfig = TombstoneKeyManager.TombstoneConfig.builder()
    .defaultTombstoneTtl(Duration.ofDays(7))          // Week-long audit trail
    .cleanupInterval(Duration.ofHours(12))            // Twice daily cleanup
    .batchSize(250)                                   // Larger batches
    .lockTimeout(Duration.ofMinutes(10))              // Longer timeout for stability
    .build();
```

### Development Configuration
```java
// Data Locality Configuration for Development
DataLocalityManager.DataLocalityConfig devLocalityConfig = DataLocalityManager.DataLocalityConfig.builder()
    .analysisInterval(Duration.ofMinutes(2))          // Rapid analysis
    .optimizationInterval(Duration.ofMinutes(10))     // Quick optimization
    .migrationLatencyThreshold(Duration.ofMillis(50)) // Lower threshold
    .minAccessesForAnalysis(3)                        // Minimal data needed
    .minAccessesForMigration(10)                      // Quick migration testing
    .autoMigrationEnabled(true)                       // Auto migration for testing
    .build();

// Tombstone Configuration for Development
TombstoneKeyManager.TombstoneConfig devTombstoneConfig = TombstoneKeyManager.TombstoneConfig.builder()
    .defaultTombstoneTtl(Duration.ofHours(2))         // Short-lived for testing
    .cleanupInterval(Duration.ofMinutes(30))          // Frequent cleanup
    .batchSize(50)                                    // Smaller batches
    .lockTimeout(Duration.ofMinutes(1))               // Quick timeout
    .build();
```

This completes the comprehensive usage guide for DataLocalityManager and TombstoneKeyManager. Both managers provide powerful capabilities for optimizing Redis operations across multiple datacenters while maintaining data consistency and providing advanced lifecycle management.
