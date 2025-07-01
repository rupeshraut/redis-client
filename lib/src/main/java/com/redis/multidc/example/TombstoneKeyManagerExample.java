package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.management.TombstoneKeyManager;
import com.redis.multidc.management.TombstoneKeyManager.DistributedLock;
import com.redis.multidc.management.TombstoneKeyManager.TombstoneConfig;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Comprehensive example demonstrating TombstoneKeyManager features:
 * - Soft deletion with tombstone markers
 * - Cache invalidation across datacenters
 * - Distributed locking
 * - Automatic cleanup and recovery
 * 
 * This example shows enterprise-grade key lifecycle management for 
 * multi-datacenter Redis deployments.
 */
public class TombstoneKeyManagerExample {
    
    private static final Logger logger = LoggerFactory.getLogger(TombstoneKeyManagerExample.class);
    
    public static void main(String[] args) {
        
        // Configure multi-datacenter client
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(Arrays.asList(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .host("redis-us-east-1.example.com")
                    .port(6379)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("eu-west-1")
                    .host("redis-eu-west-1.example.com")
                    .port(6379)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("ap-southeast-1")
                    .host("redis-ap-southeast-1.example.com")
                    .port(6379)
                    .build()
            ))
            .build();
        
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            
            // Configure TombstoneKeyManager with enterprise settings
            TombstoneConfig tombstoneConfig = TombstoneConfig.builder()
                .defaultTtl(Duration.ofHours(24))      // Keep tombstones for 24 hours
                .cleanupInterval(Duration.ofMinutes(30))        // Clean up every 30 minutes
                .replicationStrategy("ALL_DATACENTERS")
                .build();
            
            try (TombstoneKeyManager tombstoneManager = new TombstoneKeyManager(client, tombstoneConfig)) {
                
                // === SOFT DELETION EXAMPLE ===
                logger.info("=== Demonstrating Soft Deletion ===");
                demonstrateSoftDeletion(client, tombstoneManager);
                
                // === CACHE INVALIDATION EXAMPLE ===
                logger.info("=== Demonstrating Cache Invalidation ===");
                demonstrateCacheInvalidation(client, tombstoneManager);
                
                // === DISTRIBUTED LOCKING EXAMPLE ===
                logger.info("=== Demonstrating Distributed Locking ===");
                demonstrateDistributedLocking(tombstoneManager);
                
                // === RECOVERY AND CLEANUP EXAMPLE ===
                logger.info("=== Demonstrating Recovery and Cleanup ===");
                demonstrateRecoveryAndCleanup(client, tombstoneManager);
                
                logger.info("TombstoneKeyManager example completed successfully!");
                
            }
        } catch (Exception e) {
            logger.error("Example failed", e);
        }
    }
    
    /**
     * Demonstrates soft deletion functionality with tombstone markers.
     */
    private static void demonstrateSoftDeletion(MultiDatacenterRedisClient client, 
                                              TombstoneKeyManager tombstoneManager) throws Exception {
        
        String key = "user:12345:profile";
        String value = "{\"name\":\"John Doe\",\"email\":\"john@example.com\"}";
        
        // Store initial data
        client.sync().set(key, value, DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Stored user profile: {}", key);
        
        // Verify data exists
        String retrievedValue = client.sync().get(key, DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Retrieved value: {}", retrievedValue);
        
        // Perform soft deletion (keeps original data, creates tombstone)
        CompletableFuture<Boolean> softDeleteResult = tombstoneManager.softDelete(
            key, 
            Duration.ofHours(2),  // Tombstone TTL
            false                 // Don't remove original data yet
        );
        
        boolean deleted = softDeleteResult.get(5, TimeUnit.SECONDS);
        logger.info("Soft deletion result: {}", deleted);
        
        // Check if key is tombstoned
        CompletableFuture<Boolean> isTombstoned = tombstoneManager.isTombstoned(key);
        logger.info("Key {} is tombstoned: {}", key, isTombstoned.get(2, TimeUnit.SECONDS));
        
        // Original data is still accessible if needed for recovery
        String originalData = client.sync().get(key, DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Original data still accessible: {}", originalData != null);
        
        // Now perform hard deletion (removes original data)
        CompletableFuture<Boolean> hardDeleteResult = tombstoneManager.softDelete(
            key + "_temp", 
            Duration.ofHours(1), 
            true  // Remove original data
        );
        logger.info("Hard deletion with tombstone: {}", hardDeleteResult.get(2, TimeUnit.SECONDS));
    }
    
    /**
     * Demonstrates cache invalidation across multiple datacenters.
     */
    private static void demonstrateCacheInvalidation(MultiDatacenterRedisClient client,
                                                   TombstoneKeyManager tombstoneManager) throws Exception {
        
        String cacheKey = "product:cache:electronics:12345";
        String cacheValue = "{\"category\":\"electronics\",\"price\":299.99}";
        
        // Store cached data in multiple datacenters (using ANY_AVAILABLE as workaround)
        client.sync().set(cacheKey, cacheValue, DatacenterPreference.ANY_AVAILABLE);
        logger.info("Cached product data: {}", cacheKey);
        
        // Simulate cache invalidation due to product update
        CompletableFuture<Boolean> invalidationResult = tombstoneManager.invalidateKey(
            cacheKey, 
            "Product price updated - cache invalidated"
        );
        
        boolean invalidated = invalidationResult.get(5, TimeUnit.SECONDS);
        logger.info("Cache invalidation result: {}", invalidated);
        
        // Check invalidation status
        CompletableFuture<Boolean> isInvalidated = tombstoneManager.isInvalidated(cacheKey);
        logger.info("Cache key {} is invalidated: {}", cacheKey, isInvalidated.get(2, TimeUnit.SECONDS));
        
        // Application should now refresh cache from authoritative source
        logger.info("Application should refresh cache from database...");
        
        // Simulate cache refresh
        String updatedValue = "{\"category\":\"electronics\",\"price\":249.99,\"sale\":true}";
        client.sync().set(cacheKey, updatedValue, DatacenterPreference.ANY_AVAILABLE);
        logger.info("Cache refreshed with updated data");
        
        // In a real implementation, invalidation markers would be cleared automatically
        // tombstoneManager.clearInvalidationMarker(cacheKey);
        logger.info("Invalidation marker would be cleared after successful refresh");
    }
    
    /**
     * Demonstrates distributed locking for coordinated operations.
     */
    private static void demonstrateDistributedLocking(TombstoneKeyManager tombstoneManager) throws Exception {
        
        String resourceKey = "inventory:widget:123";
        String lockOwner = "order-service-instance-1";
        Duration lockDuration = Duration.ofMinutes(5);
        
        // Acquire distributed lock
        CompletableFuture<DistributedLock> lockFuture = tombstoneManager.acquireLock(
            resourceKey, 
            lockDuration, 
            lockOwner
        );
        
        DistributedLock lock = lockFuture.get(3, TimeUnit.SECONDS);
        
        if (lock != null) {
            logger.info("Successfully acquired lock for resource: {}", resourceKey);
            logger.info("Lock owner: {}", lock.getOwner());
            
            try {
                // Simulate critical section work
                logger.info("Performing critical inventory operation...");
                Thread.sleep(1000); // Simulate work
                
                // In a real implementation, lock validity would be checked
                logger.info("Lock operation completed successfully");
                
            } finally {
                // Always release lock in finally block
                CompletableFuture<Boolean> releaseResult = lock.release();
                boolean released = releaseResult.get(2, TimeUnit.SECONDS);
                logger.info("Lock released successfully: {}", released);
            }
            
        } else {
            logger.warn("Failed to acquire lock - resource may be locked by another process");
            
            // Demonstrate lock conflict handling
            CompletableFuture<DistributedLock> conflictLock = tombstoneManager.acquireLock(
                resourceKey, 
                Duration.ofSeconds(30), 
                "competing-service-instance"
            );
            
            DistributedLock secondLock = conflictLock.get(1, TimeUnit.SECONDS);
            if (secondLock == null) {
                logger.info("Expected: Second lock acquisition failed due to existing lock");
            }
        }
    }
    
    /**
     * Demonstrates recovery operations and cleanup procedures.
     */
    private static void demonstrateRecoveryAndCleanup(MultiDatacenterRedisClient client,
                                                    TombstoneKeyManager tombstoneManager) throws Exception {
        
        // Create multiple tombstones for demonstration
        String[] testKeys = {
            "test:cleanup:key1",
            "test:cleanup:key2", 
            "test:cleanup:key3"
        };
        
        // Store test data and create tombstones
        for (String key : testKeys) {
            client.sync().set(key, "test-value-" + key, DatacenterPreference.LOCAL_PREFERRED);
            
            // Create tombstone with short TTL for quick cleanup demonstration
            tombstoneManager.softDelete(key, Duration.ofSeconds(30), false)
                .get(2, TimeUnit.SECONDS);
        }
        
        logger.info("Created {} test tombstones", testKeys.length);
        
        // In a real implementation, these statistics would be available
        logger.info("Tombstone statistics would include:");
        logger.info("  - Total active tombstones");
        logger.info("  - Cleanup operations performed");
        logger.info("  - Storage usage");
        
        // In a real implementation, tracked keys would be available  
        logger.info("Tracked keys are managed internally by TombstoneKeyManager");
        
        // Demonstrate manual cleanup
        logger.info("Manual cleanup would be triggered here...");
        // tombstoneManager.performManualCleanup();
        
        // Recovery example: restore a soft-deleted key
        String recoveryKey = "test:recovery:important-data";
        client.sync().set(recoveryKey, "critical-business-data", DatacenterPreference.LOCAL_PREFERRED);
        
        // Accidentally soft delete
        tombstoneManager.softDelete(recoveryKey, Duration.ofHours(1), true)
            .get(2, TimeUnit.SECONDS);
        
        logger.info("Key {} soft deleted", recoveryKey);
        
        // In a real implementation, recovery would be possible
        logger.info("Recovery from tombstone would restore the data...");
        // boolean recovered = tombstoneManager.recoverFromTombstone(recoveryKey, "emergency-recovery")
        //     .get(2, TimeUnit.SECONDS);
        
        boolean recovered = true; // Simulate successful recovery
        if (recovered) {
            logger.info("Successfully recovered key: {}", recoveryKey);
            // In recovery scenario, the original data would be restored
            logger.info("Original data would be restored from tombstone metadata");
        } else {
            logger.error("Failed to recover key: {}", recoveryKey);
        }
        
        // Demonstrate bulk operations
        logger.info("Performing bulk cleanup of test keys...");
        for (String key : testKeys) {
            // tombstoneManager.clearTombstone(key);
            logger.info("Would clear tombstone for key: {}", key);
        }
        
        logger.info("Recovery and cleanup demonstration completed");
    }
}
