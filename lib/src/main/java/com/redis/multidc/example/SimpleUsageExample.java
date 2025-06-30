package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.management.DataLocalityManager;
import com.redis.multidc.management.TombstoneKeyManager;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import com.redis.multidc.routing.DatacenterRouter;
import com.redis.multidc.observability.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Simple working example of DataLocalityManager and TombstoneKeyManager usage.
 * This example shows the basic patterns without requiring complex setup.
 */
public class SimpleUsageExample {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleUsageExample.class);
    
    public static void main(String[] args) throws Exception {
        
        logger.info("Starting Redis Multi-Datacenter Client Example");
        
        // 1. Create basic datacenter configuration
        DatacenterConfiguration datacenterConfig = createDatacenterConfiguration();
        
        // 2. Create multi-datacenter client
        MultiDatacenterRedisClient client = createClient(datacenterConfig);
        
        // 3. Create supporting components
        DatacenterRouter router = createRouter(datacenterConfig);
        
        // 4. Create managers
        DataLocalityManager localityManager = createLocalityManager(client, router, datacenterConfig);
        TombstoneKeyManager tombstoneManager = createTombstoneManager(client);
        
        try {
            // 5. Demonstrate basic usage
            demonstrateBasicUsage(client, localityManager, tombstoneManager);
            
        } finally {
            // 6. Cleanup
            localityManager.close();
            tombstoneManager.close();
            client.close();
        }
        
        logger.info("Example completed successfully");
    }
    
    /**
     * Creates a basic datacenter configuration.
     */
    private static DatacenterConfiguration createDatacenterConfiguration() {
        return DatacenterConfiguration.builder()
            .localDatacenterId("us-east-1")
            .datacenters(Arrays.asList(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .region("us-east")
                    .host("localhost")
                    .port(6379)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("us-west-1")
                    .region("us-west")
                    .host("localhost")
                    .port(6380)
                    .build()
            ))
            .build();
    }
    
    /**
     * Creates the multi-datacenter Redis client.
     */
    private static MultiDatacenterRedisClient createClient(DatacenterConfiguration config) {
        return MultiDatacenterRedisClientBuilder.create()
            .configuration(config)
            .build();
    }
    
    /**
     * Creates a datacenter router for the locality manager.
     */
    private static DatacenterRouter createRouter(DatacenterConfiguration config) {
        // Create a simple metrics collector for the router
        MetricsCollector metricsCollector = new MetricsCollector();
        return new DatacenterRouter(config, metricsCollector);
    }
    
    /**
     * Creates the DataLocalityManager with basic configuration.
     */
    private static DataLocalityManager createLocalityManager(MultiDatacenterRedisClient client,
                                                           DatacenterRouter router,
                                                           DatacenterConfiguration config) {
        
        DataLocalityManager.DataLocalityConfig localityConfig = 
            DataLocalityManager.DataLocalityConfig.builder()
                .analysisInterval(Duration.ofMinutes(5))
                .optimizationInterval(Duration.ofMinutes(15))
                .migrationLatencyThreshold(Duration.ofMillis(50))
                .minAccessesForAnalysis(3)
                .minAccessesForMigration(10)
                .autoMigrationEnabled(false)
                .build();
        
        return new DataLocalityManager(client, router, config, localityConfig);
    }
    
    /**
     * Creates the TombstoneKeyManager with basic configuration.
     */
    private static TombstoneKeyManager createTombstoneManager(MultiDatacenterRedisClient client) {
        
        TombstoneKeyManager.TombstoneConfig tombstoneConfig = 
            TombstoneKeyManager.TombstoneConfig.builder()
                .defaultTombstoneTtl(Duration.ofHours(24))
                .cleanupInterval(Duration.ofHours(6))
                .batchSize(50)
                .lockTimeout(Duration.ofMinutes(2))
                .build();
        
        return new TombstoneKeyManager(client, tombstoneConfig);
    }
    
    /**
     * Demonstrates basic usage patterns.
     */
    private static void demonstrateBasicUsage(MultiDatacenterRedisClient client,
                                            DataLocalityManager localityManager,
                                            TombstoneKeyManager tombstoneManager) throws Exception {
        
        logger.info("=== Basic Usage Demonstration ===");
        
        // Test key for demonstrations
        String testKey = "example:user:12345";
        String testValue = "user-data-example";
        
        // 1. DataLocalityManager: Record access patterns
        logger.info("1. Recording access patterns...");
        
        localityManager.recordAccess(testKey, "us-east-1", "GET", 15);
        localityManager.recordAccess(testKey, "us-west-1", "GET", 45);
        localityManager.recordAccess(testKey, "us-east-1", "SET", 12);
        localityManager.recordAccess(testKey, "us-east-1", "GET", 18);
        
        logger.info("Recorded {} access patterns for key: {}", 4, testKey);
        
        // 2. DataLocalityManager: Get optimal datacenter
        logger.info("2. Getting optimal datacenter recommendations...");
        
        CompletableFuture<Optional<String>> optimalRead = 
            localityManager.getOptimalReadDatacenter(testKey);
        
        optimalRead.thenAccept(dc -> {
            if (dc.isPresent()) {
                logger.info("Optimal read datacenter for {}: {}", testKey, dc.get());
            } else {
                logger.info("No specific recommendation for {}", testKey);
            }
        }).get(); // Wait for completion
        
        // 3. Store some test data
        logger.info("3. Storing test data...");
        
        client.sync().set(testKey, testValue, DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Stored data: {} = {}", testKey, testValue);
        
        // 4. TombstoneKeyManager: Soft delete
        logger.info("4. Performing soft delete...");
        
        boolean softDeleted = tombstoneManager.softDelete(
            testKey,
            "example-soft-delete",
            Duration.ofHours(1),
            false  // Keep original data for demo
        );
        
        logger.info("Soft delete result for {}: {}", testKey, softDeleted);
        
        // 5. Check soft delete status
        logger.info("5. Checking soft delete status...");
        
        boolean isSoftDeleted = tombstoneManager.isSoftDeleted(testKey);
        logger.info("Is {} soft deleted? {}", testKey, isSoftDeleted);
        
        // 6. Get tombstone information
        logger.info("6. Getting tombstone information...");
        
        Optional<TombstoneKey> tombstoneInfo = tombstoneManager.getTombstoneInfo(testKey);
        if (tombstoneInfo.isPresent()) {
            TombstoneKey tombstone = tombstoneInfo.get();
            logger.info("Tombstone info - Reason: {}, Created: {}", 
                tombstone.getReason(), tombstone.getCreatedAt());
        } else {
            logger.info("No tombstone information found for {}", testKey);
        }
        
        // 7. Cache invalidation example
        logger.info("7. Testing cache invalidation...");
        
        String cacheKey = "cache:example:data";
        client.sync().set(cacheKey, "cached-value", DatacenterPreference.LOCAL_PREFERRED);
        
        boolean invalidated = tombstoneManager.invalidateKey(
            cacheKey,
            "data-updated-example",
            Duration.ofMinutes(30)
        );
        
        logger.info("Cache invalidation result for {}: {}", cacheKey, invalidated);
        
        boolean isInvalidated = tombstoneManager.isInvalidated(cacheKey);
        logger.info("Is {} invalidated? {}", cacheKey, isInvalidated);
        
        // 8. Distributed lock example
        logger.info("8. Testing distributed lock...");
        
        String lockKey = "lock:example:critical-section";
        
        TombstoneKeyManager.DistributedLock lock = tombstoneManager.acquireLock(
            lockKey,
            "example-owner",
            Duration.ofMinutes(1)
        );
        
        if (lock != null) {
            logger.info("Successfully acquired lock: {}", lockKey);
            
            try {
                // Simulate some work
                Thread.sleep(500);
                
                boolean isValid = lock.isValid();
                logger.info("Lock {} is still valid: {}", lockKey, isValid);
                
            } finally {
                boolean released = lock.release();
                logger.info("Released lock {}: {}", lockKey, released);
            }
        } else {
            logger.warn("Failed to acquire lock: {}", lockKey);
        }
        
        // 9. Get statistics
        logger.info("9. Getting statistics...");
        
        DataLocalityManager.LocalityStats localityStats = localityManager.getLocalityStats();
        logger.info("Locality stats: Total keys: {}, Optimized: {}, Avg latency: {:.2f}ms",
            localityStats.getTotalKeys(), 
            localityStats.getOptimizedKeys(),
            localityStats.getAverageLatency());
        
        TombstoneKeyManager.TombstoneStats tombstoneStats = tombstoneManager.getStats();
        logger.info("Tombstone stats: Active: {}, Total processed: {}",
            tombstoneStats.getActiveTombstones(),
            tombstoneStats.getTotalProcessed());
        
        // 10. Migration analysis (optional)
        logger.info("10. Analyzing migration opportunity...");
        
        CompletableFuture<DataLocalityManager.MigrationRecommendation> migration = 
            localityManager.analyzeMigrationOpportunity(testKey);
        
        migration.thenAccept(recommendation -> {
            logger.info("Migration recommendation for {}: {}", testKey, recommendation.isRecommended());
            if (recommendation.isRecommended()) {
                logger.info("  Recommendation: {} -> {}, Reason: {}", 
                    recommendation.getCurrentDatacenter(),
                    recommendation.getTargetDatacenter(),
                    recommendation.getReason());
            }
        }).get(); // Wait for completion
        
        logger.info("=== Basic demonstration completed ===");
    }
}
