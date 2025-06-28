package com.redis.multidc.model;

import java.time.Instant;
import java.time.Duration;
import java.util.Map;

/**
 * Represents a tombstone key for soft deletion, cache invalidation,
 * distributed locks, and data lifecycle management.
 */
public class TombstoneKey {
    
    private final String key;
    private final Type type;
    private final Instant createdAt;
    private final Instant expiresAt;
    private final String datacenterId;
    private final String reason;
    private final Map<String, String> metadata;
    
    public TombstoneKey(String key, Type type, Instant createdAt, Instant expiresAt,
                       String datacenterId, String reason, Map<String, String> metadata) {
        this.key = key;
        this.type = type;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
        this.datacenterId = datacenterId;
        this.reason = reason;
        this.metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
    }
    
    public String getKey() {
        return key;
    }
    
    public Type getType() {
        return type;
    }
    
    public Instant getCreatedAt() {
        return createdAt;
    }
    
    public Instant getExpiresAt() {
        return expiresAt;
    }
    
    public String getDatacenterId() {
        return datacenterId;
    }
    
    public String getReason() {
        return reason;
    }
    
    public Map<String, String> getMetadata() {
        return metadata;
    }
    
    public boolean isExpired() {
        return expiresAt != null && Instant.now().isAfter(expiresAt);
    }
    
    public Duration getTimeToLive() {
        if (expiresAt == null) {
            return null;
        }
        Duration ttl = Duration.between(Instant.now(), expiresAt);
        return ttl.isNegative() ? Duration.ZERO : ttl;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Type of tombstone key indicating its purpose.
     */
    public enum Type {
        
        /**
         * Soft deletion marker - indicates the key was deleted but may be recoverable.
         */
        SOFT_DELETE,
        
        /**
         * Cache invalidation marker - indicates cached data should be refreshed.
         */
        CACHE_INVALIDATION,
        
        /**
         * Distributed lock - prevents concurrent access to a resource.
         */
        DISTRIBUTED_LOCK,
        
        /**
         * Data migration marker - indicates data is being moved between datacenters.
         */
        DATA_MIGRATION,
        
        /**
         * Maintenance marker - indicates the key is under maintenance.
         */
        MAINTENANCE,
        
        /**
         * Conflict resolution marker - used for CRDB conflict resolution.
         */
        CONFLICT_RESOLUTION,
        
        /**
         * Custom tombstone type for application-specific use cases.
         */
        CUSTOM
    }
    
    public static class Builder {
        private String key;
        private Type type;
        private Instant createdAt = Instant.now();
        private Instant expiresAt;
        private String datacenterId;
        private String reason;
        private Map<String, String> metadata;
        
        public Builder key(String key) {
            this.key = key;
            return this;
        }
        
        public Builder type(Type type) {
            this.type = type;
            return this;
        }
        
        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }
        
        public Builder expiresAt(Instant expiresAt) {
            this.expiresAt = expiresAt;
            return this;
        }
        
        public Builder expiresIn(Duration ttl) {
            this.expiresAt = Instant.now().plus(ttl);
            return this;
        }
        
        public Builder datacenterId(String datacenterId) {
            this.datacenterId = datacenterId;
            return this;
        }
        
        public Builder reason(String reason) {
            this.reason = reason;
            return this;
        }
        
        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }
        
        public TombstoneKey build() {
            if (key == null || key.trim().isEmpty()) {
                throw new IllegalArgumentException("Key is required");
            }
            if (type == null) {
                throw new IllegalArgumentException("Type is required");
            }
            return new TombstoneKey(key, type, createdAt, expiresAt, datacenterId, reason, metadata);
        }
    }
    
    @Override
    public String toString() {
        return String.format("TombstoneKey{key='%s', type=%s, createdAt=%s, expiresAt=%s, datacenterId='%s', reason='%s'}",
            key, type, createdAt, expiresAt, datacenterId, reason);
    }
}
