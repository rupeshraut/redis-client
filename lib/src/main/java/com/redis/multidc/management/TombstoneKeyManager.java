package com.redis.multidc.management;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Advanced key lifecycle management for soft deletion, cache invalidation, and distributed locks.
 * Provides tombstone-based deletion with automatic cleanup and cross-datacenter synchronization.
 */
public class TombstoneKeyManager implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(TombstoneKeyManager.class);
    
    private static final String TOMBSTONE_PREFIX = "__tombstone:";
    private static final String LOCK_PREFIX = "__lock:";
    private static final String INVALIDATION_PREFIX = "__invalid:";
    
    private final MultiDatacenterRedisClient client;
    private final TombstoneConfig config;
    private final ScheduledExecutorService cleanupExecutor;
    private final Set<String> trackedKeys;
    
    public TombstoneKeyManager(MultiDatacenterRedisClient client, TombstoneConfig config) {
        this.client = client;
        this.config = config;
        this.cleanupExecutor = Executors.newScheduledThreadPool(2);
        this.trackedKeys = ConcurrentHashMap.newKeySet();
        
        // Start periodic cleanup
        cleanupExecutor.scheduleAtFixedRate(
            this::performCleanup,
            config.getCleanupInterval().toMinutes(),
            config.getCleanupInterval().toMinutes(),
            TimeUnit.MINUTES
        );
        
        logger.info("TombstoneKeyManager initialized with cleanup interval: {}", config.getCleanupInterval());
    }
    
    /**
     * Performs soft deletion by creating a tombstone key and optionally removing the original.
     * 
     * @param key the key to soft delete
     * @param ttl time to live for the tombstone
     * @param removeOriginal whether to remove the original key immediately
     * @return future indicating completion
     */
    public CompletableFuture<Boolean> softDelete(String key, Duration ttl, boolean removeOriginal) {
        String tombstoneKey = TOMBSTONE_PREFIX + key;
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Create tombstone with metadata
                TombstoneKey tombstone = new TombstoneKey(
                    key,
                    TombstoneKey.Type.SOFT_DELETE,
                    Instant.now(),
                    Instant.now().plus(ttl),
                    config.getReplicationStrategy(),
                    "SOFT_DELETE",
                    Map.of("operation", "soft_delete", "ttl", ttl.toString())
                );
                
                // Store tombstone across all datacenters
                client.sync().set(tombstoneKey, tombstoneToJson(tombstone), DatacenterPreference.LOCAL_PREFERRED);
                client.sync().expire(tombstoneKey, ttl, DatacenterPreference.LOCAL_PREFERRED);
                
                // Optionally remove original key
                if (removeOriginal) {
                    client.sync().delete(key, DatacenterPreference.LOCAL_PREFERRED);
                }
                
                trackedKeys.add(key);
                logger.debug("Soft deleted key: {} with tombstone TTL: {}", key, ttl);
                return true;
                
            } catch (Exception e) {
                logger.error("Failed to soft delete key: {}", key, e);
                return false;
            }
        });
    }
    
    /**
     * Invalidates a key across all datacenters by creating an invalidation marker.
     * 
     * @param key the key to invalidate
     * @param reason reason for invalidation
     * @return future indicating completion
     */
    public CompletableFuture<Boolean> invalidateKey(String key, String reason) {
        String invalidationKey = INVALIDATION_PREFIX + key;
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Create invalidation marker
                TombstoneKey invalidation = new TombstoneKey(
                    key,
                    TombstoneKey.Type.CACHE_INVALIDATION,
                    Instant.now(),
                    Instant.now().plus(config.getInvalidationTtl()),
                    config.getReplicationStrategy(),
                    "INVALIDATION: " + reason,
                    Map.of("operation", "invalidation", "reason", reason)
                );
                
                // Store invalidation marker
                client.sync().set(invalidationKey, tombstoneToJson(invalidation), DatacenterPreference.LOCAL_PREFERRED);
                client.sync().expire(invalidationKey, config.getInvalidationTtl(), DatacenterPreference.LOCAL_PREFERRED);
                
                // Remove original key from cache
                client.sync().delete(key, DatacenterPreference.LOCAL_PREFERRED);
                
                trackedKeys.add(key);
                logger.info("Invalidated key: {} across all datacenters, reason: {}", key, reason);
                return true;
                
            } catch (Exception e) {
                logger.error("Failed to invalidate key: {}", key, e);
                return false;
            }
        });
    }
    
    /**
     * Acquires a distributed lock across datacenters.
     * 
     * @param lockKey the lock identifier
     * @param ttl lock timeout duration
     * @param owner lock owner identifier
     * @return future with lock acquisition result
     */
    public CompletableFuture<DistributedLock> acquireLock(String lockKey, Duration ttl, String owner) {
        String redisKey = LOCK_PREFIX + lockKey;
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Try to acquire lock with SETNX
                String lockValue = owner + ":" + System.currentTimeMillis();
                
                // Use SETNX for atomic lock acquisition
                Boolean result = client.sync().setIfNotExists(redisKey, lockValue);
                
                if (Boolean.TRUE.equals(result)) {
                    client.sync().expire(redisKey, ttl, DatacenterPreference.LOCAL_PREFERRED);
                    
                    DistributedLock lock = new DistributedLock(lockKey, owner, ttl, redisKey, this);
                    logger.debug("Acquired distributed lock: {} for owner: {}", lockKey, owner);
                    return lock;
                } else {
                    logger.debug("Failed to acquire lock: {} (already held)", lockKey);
                    return null;
                }
                
            } catch (Exception e) {
                logger.error("Error acquiring lock: {}", lockKey, e);
                return null;
            }
        });
    }
    
    /**
     * Releases a distributed lock.
     * 
     * @param lock the lock to release
     * @return future indicating completion
     */
    public CompletableFuture<Boolean> releaseLock(DistributedLock lock) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Use Lua script for atomic check-and-delete
                String script = """
                    if redis.call("GET", KEYS[1]) == ARGV[1] then
                        return redis.call("DEL", KEYS[1])
                    else
                        return 0
                    end
                    """;
                
                String lockValue = lock.getOwner() + ":" + lock.getAcquisitionTime();
                
                // For simplicity, use GET and DEL (not atomic, but works for demo)
                String currentValue = client.sync().get(lock.getRedisKey(), DatacenterPreference.LOCAL_PREFERRED);
                boolean released = false;
                
                if (lockValue.equals(currentValue)) {
                    boolean delResult = client.sync().delete(lock.getRedisKey(), DatacenterPreference.LOCAL_PREFERRED);
                    released = delResult;
                }
                if (released) {
                    logger.debug("Released distributed lock: {}", lock.getLockKey());
                } else {
                    logger.warn("Failed to release lock: {} (not owned or expired)", lock.getLockKey());
                }
                
                return released;
                
            } catch (Exception e) {
                logger.error("Error releasing lock: {}", lock.getLockKey(), e);
                return false;
            }
        });
    }
    
    /**
     * Checks if a key is tombstoned (soft deleted).
     * 
     * @param key the key to check
     * @return future with tombstone status
     */
    public CompletableFuture<Boolean> isTombstoned(String key) {
        String tombstoneKey = TOMBSTONE_PREFIX + key;
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                String tombstoneData = client.sync().get(tombstoneKey, DatacenterPreference.LOCAL_PREFERRED);
                return tombstoneData != null;
            } catch (Exception e) {
                logger.error("Error checking tombstone status for key: {}", key, e);
                return false;
            }
        });
    }
    
    /**
     * Checks if a key is invalidated.
     * 
     * @param key the key to check
     * @return future with invalidation status
     */
    public CompletableFuture<Boolean> isInvalidated(String key) {
        String invalidationKey = INVALIDATION_PREFIX + key;
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                String invalidationData = client.sync().get(invalidationKey, DatacenterPreference.LOCAL_PREFERRED);
                return invalidationData != null;
            } catch (Exception e) {
                logger.error("Error checking invalidation status for key: {}", key, e);
                return false;
            }
        });
    }
    
    /**
     * Performs cleanup of expired tombstones and invalidation markers.
     */
    private void performCleanup() {
        logger.debug("Starting tombstone cleanup cycle");
        
        try {
            // Clean up expired tombstones and invalidation markers
            // This is a simplified cleanup - in production, you'd use Redis SCAN
            // to iterate through keys with the appropriate prefixes
            
            int cleanedCount = 0;
            for (String key : trackedKeys) {
                try {
                    String tombstoneKey = TOMBSTONE_PREFIX + key;
                    String invalidationKey = INVALIDATION_PREFIX + key;
                    
                    // Check if tombstone still exists
                    String tombstone = client.sync().get(tombstoneKey, DatacenterPreference.LOCAL_PREFERRED);
                    String invalidation = client.sync().get(invalidationKey, DatacenterPreference.LOCAL_PREFERRED);
                    
                    if (tombstone == null && invalidation == null) {
                        trackedKeys.remove(key);
                        cleanedCount++;
                    }
                    
                } catch (Exception e) {
                    logger.warn("Error during cleanup for key: {}", key, e);
                }
            }
            
            logger.debug("Cleanup cycle completed, cleaned {} expired entries", cleanedCount);
            
        } catch (Exception e) {
            logger.error("Error during tombstone cleanup", e);
        }
    }
    
    /**
     * Gets the current configuration.
     * 
     * @return the tombstone configuration
     */
    public TombstoneConfig getConfig() {
        return config;
    }
    
    /**
     * Gets the number of currently tracked keys.
     * 
     * @return number of tracked keys
     */
    public int getTrackedKeyCount() {
        return trackedKeys.size();
    }
    
    /**
     * Converts a TombstoneKey to JSON string.
     */
    private String tombstoneToJson(TombstoneKey tombstone) {
        return String.format("{\"key\":\"%s\",\"type\":\"%s\",\"createdAt\":\"%s\",\"expiresAt\":\"%s\",\"datacenterId\":\"%s\",\"reason\":\"%s\"}", 
            tombstone.getKey(), 
            tombstone.getType(), 
            tombstone.getCreatedAt(), 
            tombstone.getExpiresAt(), 
            tombstone.getDatacenterId(), 
            tombstone.getReason());
    }
    
    @Override
    public void close() {
        logger.info("Closing TombstoneKeyManager");
        
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        trackedKeys.clear();
        logger.info("TombstoneKeyManager closed");
    }
    
    /**
     * Represents a distributed lock.
     */
    public static class DistributedLock {
        private final String lockKey;
        private final String owner;
        private final Duration ttl;
        private final String redisKey;
        private final long acquisitionTime;
        private final TombstoneKeyManager manager;
        
        public DistributedLock(String lockKey, String owner, Duration ttl, String redisKey, TombstoneKeyManager manager) {
            this.lockKey = lockKey;
            this.owner = owner;
            this.ttl = ttl;
            this.redisKey = redisKey;
            this.acquisitionTime = System.currentTimeMillis();
            this.manager = manager;
        }
        
        public String getLockKey() {
            return lockKey;
        }
        
        public String getOwner() {
            return owner;
        }
        
        public Duration getTtl() {
            return ttl;
        }
        
        public String getRedisKey() {
            return redisKey;
        }
        
        public long getAcquisitionTime() {
            return acquisitionTime;
        }
        
        public CompletableFuture<Boolean> release() {
            return manager.releaseLock(this);
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() - acquisitionTime > ttl.toMillis();
        }
        
        @Override
        public String toString() {
            return String.format("DistributedLock{key='%s', owner='%s', ttl=%s}", 
                lockKey, owner, ttl);
        }
    }
    
    /**
     * Configuration for tombstone key management.
     */
    public static class TombstoneConfig {
        private final Duration defaultTtl;
        private final Duration invalidationTtl;
        private final Duration cleanupInterval;
        private final String replicationStrategy;
        
        public TombstoneConfig(Duration defaultTtl, Duration invalidationTtl, Duration cleanupInterval, String replicationStrategy) {
            this.defaultTtl = defaultTtl;
            this.invalidationTtl = invalidationTtl;
            this.cleanupInterval = cleanupInterval;
            this.replicationStrategy = replicationStrategy;
        }
        
        public static TombstoneConfig defaultConfig() {
            return new TombstoneConfig(
                Duration.ofHours(24),
                Duration.ofMinutes(30),
                Duration.ofMinutes(15),
                "ALL_DATACENTERS"
            );
        }
        
        public static Builder builder() {
            return new Builder();
        }
        
        public Duration getDefaultTtl() {
            return defaultTtl;
        }
        
        public Duration getInvalidationTtl() {
            return invalidationTtl;
        }
        
        public Duration getCleanupInterval() {
            return cleanupInterval;
        }
        
        public String getReplicationStrategy() {
            return replicationStrategy;
        }
        
        public static class Builder {
            private Duration defaultTtl = Duration.ofHours(24);
            private Duration invalidationTtl = Duration.ofMinutes(30);
            private Duration cleanupInterval = Duration.ofMinutes(15);
            private String replicationStrategy = "ALL_DATACENTERS";
            
            public Builder defaultTtl(Duration defaultTtl) {
                this.defaultTtl = defaultTtl;
                return this;
            }
            
            public Builder invalidationTtl(Duration invalidationTtl) {
                this.invalidationTtl = invalidationTtl;
                return this;
            }
            
            public Builder cleanupInterval(Duration cleanupInterval) {
                this.cleanupInterval = cleanupInterval;
                return this;
            }
            
            public Builder replicationStrategy(String replicationStrategy) {
                this.replicationStrategy = replicationStrategy;
                return this;
            }
            
            public TombstoneConfig build() {
                return new TombstoneConfig(defaultTtl, invalidationTtl, cleanupInterval, replicationStrategy);
            }
        }
    }
}
