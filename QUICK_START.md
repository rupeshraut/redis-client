# Quick Start Guide: DataLocalityManager & TombstoneKeyManager

This guide shows you how to quickly get started with the two powerful management components in the Redis Multi-Datacenter Client.

## Overview

- **DataLocalityManager**: Optimizes data placement across datacenters based on access patterns
- **TombstoneKeyManager**: Provides advanced key lifecycle management with soft deletion and distributed locking

## Quick Setup

### 1. Basic Setup

```java
// Create the multi-datacenter client
MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create()
    .configuration(datacenterConfig)
    .build();

// Create the managers
DataLocalityManager localityManager = new DataLocalityManager(client, router, config, localityConfig);
TombstoneKeyManager tombstoneManager = new TombstoneKeyManager(client, tombstoneConfig);
```

### 2. DataLocalityManager - Track & Optimize Data Placement

```java
// Record where your data is accessed from
localityManager.recordAccess("user:12345", "us-east-1", "GET", 15); // 15ms latency
localityManager.recordAccess("user:12345", "eu-west-1", "GET", 120); // 120ms latency

// Get optimal datacenter for reads
CompletableFuture<Optional<String>> bestDC = localityManager.getOptimalReadDatacenter("user:12345");
bestDC.thenAccept(dc -> {
    if (dc.isPresent()) {
        System.out.println("Best datacenter: " + dc.get()); // "us-east-1"
    }
});

// Analyze if migration would help
CompletableFuture<MigrationRecommendation> analysis = 
    localityManager.analyzeMigrationOpportunity("user:12345");

analysis.thenAccept(rec -> {
    if (rec.isRecommended()) {
        // Migrate data for better performance
        localityManager.migrateData(rec.getKey(), rec.getCurrentDatacenter(), rec.getTargetDatacenter());
    }
});
```

### 3. TombstoneKeyManager - Advanced Key Management

```java
// Soft delete (keeps audit trail)
boolean deleted = tombstoneManager.softDelete(
    "user:12345",           // key
    "user-deleted-gdpr",    // reason
    Duration.ofDays(30),    // keep tombstone for 30 days
    true                    // remove original data
);

// Check if key is soft deleted
boolean isSoftDeleted = tombstoneManager.isSoftDeleted("user:12345");

// Cache invalidation across all datacenters
boolean invalidated = tombstoneManager.invalidateKey(
    "cache:product:123",
    "product-updated",
    Duration.ofMinutes(30)
);

// Distributed locking
TombstoneKeyManager.DistributedLock lock = tombstoneManager.acquireLock(
    "lock:order-processing",
    "service-instance-1",
    Duration.ofMinutes(2)
);

if (lock != null) {
    try {
        // Critical section - only one instance executes this
        processOrder();
    } finally {
        lock.release(); // Always release
    }
}
```

## Common Use Cases

### Smart Caching
```java
public class SmartCache {
    public String get(String key) {
        // Check if cache is invalidated
        if (tombstoneManager.isInvalidated(key)) {
            return reloadFromSource(key);
        }
        
        // Get from optimal datacenter
        Optional<String> bestDC = localityManager.getOptimalReadDatacenter(key).join();
        if (bestDC.isPresent()) {
            return client.sync().get(key, DatacenterPreference.SPECIFIC_DATACENTER);
        }
        
        return reloadFromSource(key);
    }
    
    public void invalidate(String key) {
        tombstoneManager.invalidateKey(key, "data-updated", Duration.ofMinutes(30));
    }
}
```

### Session Management
```java
public class SessionManager {
    public void logout(String sessionId, boolean keepAuditTrail) {
        if (keepAuditTrail) {
            // Soft delete for compliance
            tombstoneManager.softDelete(
                "session:" + sessionId,
                "user-logout",
                Duration.ofDays(90),  // 90-day audit trail
                true                  // Remove session data
            );
        } else {
            // Hard delete
            client.sync().delete("session:" + sessionId, DatacenterPreference.LOCAL_PREFERRED);
        }
    }
}
```

### Distributed Coordination
```java
public void processUniqueTask(String taskId) {
    String lockKey = "lock:task:" + taskId;
    
    TombstoneKeyManager.DistributedLock lock = tombstoneManager.acquireLock(
        lockKey, getInstanceId(), Duration.ofMinutes(5)
    );
    
    if (lock != null) {
        try {
            // Only one service instance processes this task
            performTaskProcessing(taskId);
        } finally {
            lock.release();
        }
    } else {
        logger.info("Task {} is being processed by another instance", taskId);
    }
}
```

## Configuration Examples

### Development Configuration (Fast & Aggressive)
```java
// Quick feedback for development
DataLocalityManager.DataLocalityConfig devConfig = DataLocalityManager.DataLocalityConfig.builder()
    .analysisInterval(Duration.ofMinutes(1))          // Analyze every minute
    .migrationLatencyThreshold(Duration.ofMillis(20)) // Low threshold
    .minAccessesForMigration(5)                       // Few accesses needed
    .autoMigrationEnabled(true)                       // Auto migrate
    .build();

TombstoneKeyManager.TombstoneConfig devTombstoneConfig = TombstoneKeyManager.TombstoneConfig.builder()
    .defaultTombstoneTtl(Duration.ofMinutes(30))      // Short-lived
    .cleanupInterval(Duration.ofMinutes(5))           // Frequent cleanup
    .build();
```

### Production Configuration (Conservative & Stable)
```java
// Stable and conservative for production
DataLocalityManager.DataLocalityConfig prodConfig = DataLocalityManager.DataLocalityConfig.builder()
    .analysisInterval(Duration.ofMinutes(30))         // Less frequent
    .migrationLatencyThreshold(Duration.ofMillis(100)) // Higher threshold
    .minAccessesForMigration(100)                     // More confidence needed
    .autoMigrationEnabled(false)                      // Manual approval
    .build();

TombstoneKeyManager.TombstoneConfig prodTombstoneConfig = TombstoneKeyManager.TombstoneConfig.builder()
    .defaultTombstoneTtl(Duration.ofDays(7))          // Week-long audit
    .cleanupInterval(Duration.ofHours(12))            // Twice daily
    .lockTimeout(Duration.ofMinutes(10))              // Longer timeout
    .build();
```

## Monitoring & Health

### Get Statistics
```java
// Data locality health
DataLocalityManager.LocalityStats localityStats = localityManager.getLocalityStats();
System.out.println("Optimized keys: " + localityStats.getOptimizationRatio() * 100 + "%");
System.out.println("Average latency: " + localityStats.getAverageLatency() + "ms");

// Tombstone health
TombstoneKeyManager.TombstoneStats tombstoneStats = tombstoneManager.getStats();
System.out.println("Active tombstones: " + tombstoneStats.getActiveTombstones());
System.out.println("Cleanup efficiency: " + tombstoneStats.getCleanupEfficiency());
```

### Health Checks
```java
public boolean isHealthy() {
    LocalityStats stats = localityManager.getLocalityStats();
    
    // Check if optimization ratio is reasonable
    if (stats.getOptimizationRatio() < 0.7) {
        return false; // Less than 70% optimized
    }
    
    // Check if average latency is acceptable
    if (stats.getAverageLatency() > 100) {
        return false; // More than 100ms average latency
    }
    
    // Check tombstone growth
    TombstoneStats tombstoneStats = tombstoneManager.getStats();
    if (tombstoneStats.getActiveTombstones() > 10000) {
        return false; // Too many active tombstones
    }
    
    return true;
}
```

## Best Practices

### ✅ Do's
- Always record actual latency measurements for locality analysis
- Use conservative migration thresholds in production
- Always release distributed locks in finally blocks
- Monitor tombstone growth and cleanup efficiency
- Use appropriate TTLs for different use cases (cache vs audit)

### ❌ Don'ts
- Don't enable auto-migration in production without testing
- Don't use very short TTLs for audit trails
- Don't forget to handle lock acquisition failures
- Don't ignore migration recommendations without analysis
- Don't skip monitoring and alerting on manager health

## Running the Examples

To see these managers in action, run the provided examples:

```bash
# Compile the project
./gradlew build

# Run the simple usage example
./gradlew run --args="simple-usage"

# Run the comprehensive example
./gradlew run --args="comprehensive"
```

The examples demonstrate real-world usage patterns and show how the managers work together to optimize your Redis operations across multiple datacenters.

For more detailed information, see the full [USAGE_GUIDE.md](USAGE_GUIDE.md).
