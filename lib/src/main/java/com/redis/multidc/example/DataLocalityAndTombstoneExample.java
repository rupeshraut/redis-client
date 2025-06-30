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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Comprehensive example demonstrating how to use DataLocalityManager and TombstoneKeyManager
 * for intelligent data placement and advanced key lifecycle management.
 */
public class DataLocalityAndTombstoneExample {
    
    private static final Logger logger = LoggerFactory.getLogger(DataLocalityAndTombstoneExample.class);
    
    public static void main(String[] args) throws Exception {
        
        // 1. Setup Multi-Datacenter Client
        MultiDatacenterRedisClient client = createMultiDatacenterClient();
        
        // 2. Create managers
        DataLocalityManager localityManager = createDataLocalityManager(client);
        TombstoneKeyManager tombstoneManager = createTombstoneKeyManager(client);
        
        try {
            // 3. Demonstrate DataLocalityManager usage
            demonstrateDataLocalityManager(client, localityManager);
            
            // 4. Demonstrate TombstoneKeyManager usage
            demonstrateTombstoneKeyManager(client, tombstoneManager);
            
            // 5. Demonstrate advanced patterns
            demonstrateAdvancedPatterns(client, localityManager, tombstoneManager);
            
        } finally {
            // 6. Cleanup
            localityManager.close();
            tombstoneManager.close();
            client.close();
        }
    }
    
    /**
     * Creates a multi-datacenter Redis client for testing.
     */
    private static MultiDatacenterRedisClient createMultiDatacenterClient() {
        DatacenterConfiguration config = DatacenterConfiguration.builder()
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
                    .build(),
                DatacenterEndpoint.builder()
                    .id("eu-west-1")
                    .region("europe")
                    .host("localhost")
                    .port(6381)
                    .build()
            ))
            .build();
        
        return MultiDatacenterRedisClientBuilder.create()
            .configuration(config)
            .build();
    }
    
    /**
     * Creates and configures the DataLocalityManager.
     */
    private static DataLocalityManager createDataLocalityManager(MultiDatacenterRedisClient client) {
        // Get the datacenter router from the client (assuming it's accessible)
        DatacenterRouter router = client.getDatacenterRouter();
        DatacenterConfiguration config = client.getConfiguration();
        
        // Configure data locality settings
        DataLocalityManager.DataLocalityConfig localityConfig = DataLocalityManager.DataLocalityConfig.builder()
            .analysisInterval(Duration.ofMinutes(5))         // Analyze every 5 minutes
            .optimizationInterval(Duration.ofMinutes(15))    // Optimize every 15 minutes
            .migrationLatencyThreshold(Duration.ofMillis(100)) // Migrate if >100ms improvement
            .minAccessesForAnalysis(5)                       // Need at least 5 accesses to analyze
            .minAccessesForMigration(20)                     // Need at least 20 accesses to migrate
            .autoMigrationEnabled(false)                     // Manual migration for safety
            .build();
        
        return new DataLocalityManager(client, router, config, localityConfig);
    }
    
    /**
     * Creates and configures the TombstoneKeyManager.
     */
    private static TombstoneKeyManager createTombstoneKeyManager(MultiDatacenterRedisClient client) {
        TombstoneKeyManager.TombstoneConfig tombstoneConfig = TombstoneKeyManager.TombstoneConfig.builder()
            .defaultTombstoneTtl(Duration.ofHours(24))       // Keep tombstones for 24 hours
            .cleanupInterval(Duration.ofHours(6))            // Cleanup every 6 hours
            .batchSize(100)                                  // Process 100 keys per batch
            .lockTimeout(Duration.ofMinutes(5))              // Lock timeout for operations
            .build();
        
        return new TombstoneKeyManager(client, tombstoneConfig);
    }
    
    /**
     * Demonstrates DataLocalityManager functionality.
     */
    private static void demonstrateDataLocalityManager(MultiDatacenterRedisClient client, 
                                                      DataLocalityManager localityManager) throws Exception {
        
        logger.info("=== DataLocalityManager Examples ===");
        
        // Example 1: Record access patterns for intelligent placement
        logger.info("1. Recording access patterns...");
        
        String sessionKey = "user:session:12345";
        String productKey = "product:details:98765";
        
        // Simulate user accessing session data from different datacenters
        localityManager.recordAccess(sessionKey, "us-east-1", "GET", 15);    // Fast local access
        localityManager.recordAccess(sessionKey, "us-west-1", "GET", 45);    // Slower cross-region
        localityManager.recordAccess(sessionKey, "eu-west-1", "GET", 120);   // Slowest cross-continent
        
        localityManager.recordAccess(sessionKey, "us-east-1", "SET", 12);
        localityManager.recordAccess(sessionKey, "us-east-1", "GET", 18);
        localityManager.recordAccess(sessionKey, "us-east-1", "GET", 14);
        
        // Simulate product data accessed globally
        localityManager.recordAccess(productKey, "us-east-1", "GET", 25);
        localityManager.recordAccess(productKey, "eu-west-1", "GET", 30);
        localityManager.recordAccess(productKey, "us-west-1", "GET", 35);
        localityManager.recordAccess(productKey, "eu-west-1", "GET", 28);
        localityManager.recordAccess(productKey, "eu-west-1", "GET", 32);
        
        Thread.sleep(1000); // Allow pattern recording
        
        // Example 2: Get optimal datacenter recommendations
        logger.info("2. Getting optimal datacenter recommendations...");
        
        CompletableFuture<Optional<String>> optimalReadDC = localityManager.getOptimalReadDatacenter(sessionKey);
        optimalReadDC.thenAccept(dc -> 
            logger.info("Optimal read datacenter for {}: {}", sessionKey, dc.orElse("none"))
        ).get();
        
        CompletableFuture<Optional<String>> optimalWriteDC = localityManager.getOptimalWriteDatacenter(sessionKey);
        optimalWriteDC.thenAccept(dc -> 
            logger.info("Optimal write datacenter for {}: {}", sessionKey, dc.orElse("none"))
        ).get();
        
        // Example 3: Analyze migration opportunities
        logger.info("3. Analyzing migration opportunities...");
        
        CompletableFuture<DataLocalityManager.MigrationRecommendation> migrationAnalysis = 
            localityManager.analyzeMigrationOpportunity(sessionKey);
        
        migrationAnalysis.thenAccept(recommendation -> {
            logger.info("Migration recommendation for {}: {}", sessionKey, recommendation);
            
            if (recommendation.isRecommended()) {
                logger.info("  -> Migrating from {} to {} would improve performance", 
                    recommendation.getCurrentDatacenter(), recommendation.getTargetDatacenter());
            }
        }).get();
        
        // Example 4: Perform manual migration
        logger.info("4. Performing manual data migration...");
        
        // First, store some data
        client.sync().set(sessionKey, "session-data-value", DatacenterPreference.SPECIFIC_DATACENTER);
        
        // Then migrate it
        CompletableFuture<Boolean> migrationResult = 
            localityManager.migrateData(sessionKey, "us-east-1", "us-west-1");
        
        migrationResult.thenAccept(success -> {
            if (success) {
                logger.info("Successfully migrated {} from us-east-1 to us-west-1", sessionKey);
            } else {
                logger.warn("Failed to migrate {}", sessionKey);
            }
        }).get();
        
        // Example 5: Get locality statistics
        logger.info("5. Getting locality statistics...");
        
        DataLocalityManager.LocalityStats stats = localityManager.getLocalityStats();
        logger.info("Locality stats: {}", stats);
    }
    
    /**
     * Demonstrates TombstoneKeyManager functionality.
     */
    private static void demonstrateTombstoneKeyManager(MultiDatacenterRedisClient client, 
                                                      TombstoneKeyManager tombstoneManager) throws Exception {
        
        logger.info("=== TombstoneKeyManager Examples ===");
        
        // Example 1: Soft delete with tombstone
        logger.info("1. Performing soft delete with tombstone...");
        
        String userKey = "user:profile:67890";
        
        // Store user data first
        client.sync().set(userKey, "user-profile-data", DatacenterPreference.LOCAL_PREFERRED);
        client.sync().expire(userKey, Duration.ofHours(2), DatacenterPreference.LOCAL_PREFERRED);
        
        // Soft delete with tombstone (keep original key)
        boolean softDeleteResult = tombstoneManager.softDelete(
            userKey, 
            "user-deleted-gdpr",           // reason
            Duration.ofDays(30),           // tombstone TTL
            false                          // don't remove original
        );
        
        logger.info("Soft delete result for {}: {}", userKey, softDeleteResult);
        
        // Check if key is soft deleted
        boolean isSoftDeleted = tombstoneManager.isSoftDeleted(userKey);
        logger.info("Is {} soft deleted? {}", userKey, isSoftDeleted);
        
        // Example 2: Cache invalidation across datacenters
        logger.info("2. Performing cache invalidation...");
        
        String cacheKey = "cache:expensive-computation:abc123";
        
        // Store cache data
        client.sync().set(cacheKey, "expensive-result", DatacenterPreference.LOCAL_PREFERRED);
        
        // Invalidate cache across all datacenters
        boolean invalidationResult = tombstoneManager.invalidateKey(
            cacheKey,
            "data-updated",
            Duration.ofMinutes(30)         // Keep invalidation marker for 30 minutes
        );
        
        logger.info("Cache invalidation result for {}: {}", cacheKey, invalidationResult);
        
        // Check if key is invalidated
        boolean isInvalidated = tombstoneManager.isInvalidated(cacheKey);
        logger.info("Is {} invalidated? {}", cacheKey, isInvalidated);
        
        // Example 3: Distributed locking
        logger.info("3. Using distributed locks...");
        
        String lockKey = "lock:critical-section:order-processing";
        
        // Acquire distributed lock
        TombstoneKeyManager.DistributedLock lock = tombstoneManager.acquireLock(
            lockKey,
            "order-service-1",             // lock owner
            Duration.ofMinutes(2)          // lock timeout
        );
        
        if (lock != null) {
            logger.info("Acquired distributed lock: {}", lockKey);
            
            try {
                // Simulate critical section work
                logger.info("Performing critical section work...");
                Thread.sleep(1000);
                
                // Check if we still hold the lock
                boolean isValid = lock.isValid();
                logger.info("Lock still valid? {}", isValid);
                
            } finally {
                // Always release the lock
                boolean released = lock.release();
                logger.info("Released lock {}: {}", lockKey, released);
            }
        } else {
            logger.warn("Failed to acquire lock: {}", lockKey);
        }
        
        // Example 4: Get tombstone information
        logger.info("4. Getting tombstone information...");
        
        Optional<TombstoneKey> tombstoneInfo = tombstoneManager.getTombstoneInfo(userKey);
        if (tombstoneInfo.isPresent()) {
            TombstoneKey tombstone = tombstoneInfo.get();
            logger.info("Tombstone info for {}: reason={}, created={}, ttl={}", 
                userKey, tombstone.getReason(), tombstone.getCreatedAt(), tombstone.getTtl());
        }
        
        // Example 5: Manual cleanup
        logger.info("5. Performing manual cleanup...");
        
        int cleanedUp = tombstoneManager.cleanup();
        logger.info("Cleaned up {} expired tombstones", cleanedUp);
    }
    
    /**
     * Demonstrates advanced patterns combining both managers.
     */
    private static void demonstrateAdvancedPatterns(MultiDatacenterRedisClient client,
                                                   DataLocalityManager localityManager,
                                                   TombstoneKeyManager tombstoneManager) throws Exception {
        
        logger.info("=== Advanced Integration Patterns ===");
        
        // Pattern 1: Smart cache eviction with locality awareness
        logger.info("1. Smart cache eviction with locality awareness...");
        
        String hotCacheKey = "cache:hot-data:trending-product";
        
        // Store cache in optimal location based on access patterns
        CompletableFuture<Optional<String>> optimalDC = localityManager.getOptimalWriteDatacenter(hotCacheKey);
        optimalDC.thenAccept(dc -> {
            if (dc.isPresent()) {
                logger.info("Storing hot cache data in optimal datacenter: {}", dc.get());
                client.sync().set(hotCacheKey, "trending-product-data", DatacenterPreference.SPECIFIC_DATACENTER);
            }
        }).get();
        
        // When data changes, invalidate cache intelligently
        boolean invalidated = tombstoneManager.invalidateKey(
            hotCacheKey, 
            "product-data-updated", 
            Duration.ofMinutes(15)
        );
        
        if (invalidated) {
            logger.info("Cache invalidated, will be refreshed in optimal location on next access");
        }
        
        // Pattern 2: Distributed session management with soft delete
        logger.info("2. Distributed session management...");
        
        String sessionKey = "session:distributed:user-789";
        
        // Create session in optimal datacenter
        localityManager.recordAccess(sessionKey, "us-east-1", "SET", 10);
        client.sync().set(sessionKey, "session-data", Duration.ofHours(8), DatacenterPreference.LOCAL_PREFERRED);
        
        // When user logs out, soft delete but keep for audit
        boolean logoutResult = tombstoneManager.softDelete(
            sessionKey,
            "user-logout",
            Duration.ofDays(7),            // Keep audit trail for 7 days
            true                           // Remove original session data
        );
        
        logger.info("Session logout (soft delete) result: {}", logoutResult);
        
        // Pattern 3: Data migration with tombstone safety
        logger.info("3. Safe data migration with tombstone markers...");
        
        String importantKey = "data:important:financial-record";
        
        // Store important data
        client.sync().set(importantKey, "sensitive-financial-data", DatacenterPreference.LOCAL_PREFERRED);
        
        // Before migration, create safety tombstone
        boolean safetyMarker = tombstoneManager.invalidateKey(
            importantKey + ":migration-in-progress",
            "migration-safety-marker",
            Duration.ofHours(1)
        );
        
        if (safetyMarker) {
            // Perform migration
            CompletableFuture<Boolean> migrationResult = localityManager.migrateData(
                importantKey, "us-east-1", "us-west-1"
            );
            
            migrationResult.thenAccept(success -> {
                if (success) {
                    logger.info("Safe migration completed for: {}", importantKey);
                    // Clear safety marker
                    tombstoneManager.restoreKey(importantKey + ":migration-in-progress");
                } else {
                    logger.error("Migration failed, safety marker remains active");
                }
            }).get();
        }
        
        // Pattern 4: Global statistics and monitoring
        logger.info("4. Global statistics and monitoring...");
        
        DataLocalityManager.LocalityStats localityStats = localityManager.getLocalityStats();
        TombstoneKeyManager.TombstoneStats tombstoneStats = tombstoneManager.getStats();
        
        logger.info("=== System Health Report ===");
        logger.info("Locality: {}", localityStats);
        logger.info("Tombstones: {}", tombstoneStats);
        
        double healthScore = calculateHealthScore(localityStats, tombstoneStats);
        logger.info("Overall system health score: {:.2f}/100", healthScore);
    }
    
    /**
     * Calculates a simple health score based on locality and tombstone metrics.
     */
    private static double calculateHealthScore(DataLocalityManager.LocalityStats localityStats,
                                             TombstoneKeyManager.TombstoneStats tombstoneStats) {
        double localityScore = localityStats.getOptimizationRatio() * 50; // 0-50 points
        double tombstoneScore = Math.max(0, 50 - (tombstoneStats.getActiveTombstones() * 0.1)); // 0-50 points
        return localityScore + tombstoneScore;
    }
}
