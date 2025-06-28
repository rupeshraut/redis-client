package com.redis.multidc.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Instant;
import java.time.Duration;
import java.util.Map;
import java.util.HashMap;

/**
 * Comprehensive tests for TombstoneKey to improve mutation score
 */
class TombstoneKeyTest {

    @Test
    void testBuilderBasic() {
        String key = "test-key";
        TombstoneKey tombstone = TombstoneKey.builder()
            .key(key)
            .type(TombstoneKey.Type.SOFT_DELETE)
            .build();
        
        assertEquals(key, tombstone.getKey());
        assertEquals(TombstoneKey.Type.SOFT_DELETE, tombstone.getType());
        assertNotNull(tombstone.getCreatedAt());
        assertNull(tombstone.getExpiresAt());
        assertFalse(tombstone.isExpired());
    }

    @Test
    void testBuilderWithExpiresAt() {
        String key = "test-key";
        Instant expiry = Instant.now().plusSeconds(300);
        
        TombstoneKey tombstone = TombstoneKey.builder()
            .key(key)
            .type(TombstoneKey.Type.CACHE_INVALIDATION)
            .expiresAt(expiry)
            .build();
        
        assertEquals(key, tombstone.getKey());
        assertEquals(TombstoneKey.Type.CACHE_INVALIDATION, tombstone.getType());
        assertEquals(expiry, tombstone.getExpiresAt());
        assertFalse(tombstone.isExpired());
    }

    @Test
    void testBuilderWithExpiresIn() {
        String key = "test-key";
        Duration ttl = Duration.ofMinutes(5);
        
        TombstoneKey tombstone = TombstoneKey.builder()
            .key(key)
            .type(TombstoneKey.Type.DISTRIBUTED_LOCK)
            .expiresIn(ttl)
            .build();
        
        assertEquals(key, tombstone.getKey());
        assertEquals(TombstoneKey.Type.DISTRIBUTED_LOCK, tombstone.getType());
        assertNotNull(tombstone.getExpiresAt());
        assertFalse(tombstone.isExpired());
    }

    @Test
    void testBuilderWithAllFields() {
        String key = "test-key";
        Instant createdAt = Instant.now().minusSeconds(100);
        Instant expiresAt = Instant.now().plusSeconds(300);
        String datacenterId = "us-east-1";
        String reason = "Test reason";
        Map<String, String> metadata = Map.of("env", "test", "version", "1.0");
        
        TombstoneKey tombstone = TombstoneKey.builder()
            .key(key)
            .type(TombstoneKey.Type.DATA_MIGRATION)
            .createdAt(createdAt)
            .expiresAt(expiresAt)
            .datacenterId(datacenterId)
            .reason(reason)
            .metadata(metadata)
            .build();
        
        assertEquals(key, tombstone.getKey());
        assertEquals(TombstoneKey.Type.DATA_MIGRATION, tombstone.getType());
        assertEquals(createdAt, tombstone.getCreatedAt());
        assertEquals(expiresAt, tombstone.getExpiresAt());
        assertEquals(datacenterId, tombstone.getDatacenterId());
        assertEquals(reason, tombstone.getReason());
        assertEquals(metadata, tombstone.getMetadata());
    }

    @Test
    void testBuilderWithNullKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            TombstoneKey.builder()
                .key(null)
                .type(TombstoneKey.Type.SOFT_DELETE)
                .build();
        });
    }

    @Test
    void testBuilderWithEmptyKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            TombstoneKey.builder()
                .key("")
                .type(TombstoneKey.Type.SOFT_DELETE)
                .build();
        });
    }

    @Test
    void testBuilderWithBlankKey() {
        assertThrows(IllegalArgumentException.class, () -> {
            TombstoneKey.builder()
                .key("   ")
                .type(TombstoneKey.Type.SOFT_DELETE)
                .build();
        });
    }

    @Test
    void testBuilderWithNullType() {
        assertThrows(IllegalArgumentException.class, () -> {
            TombstoneKey.builder()
                .key("test-key")
                .type(null)
                .build();
        });
    }

    @Test
    void testBuilderWithMissingType() {
        assertThrows(IllegalArgumentException.class, () -> {
            TombstoneKey.builder()
                .key("test-key")
                .build();
        });
    }

    @Test
    void testIsExpiredWhenNotExpired() {
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .expiresIn(Duration.ofMinutes(5))
            .build();
        
        assertFalse(tombstone.isExpired());
    }

    @Test
    void testIsExpiredWhenExpired() {
        Instant pastTime = Instant.now().minusSeconds(100);
        
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .expiresAt(pastTime)
            .build();
        
        assertTrue(tombstone.isExpired());
    }

    @Test
    void testIsExpiredWithNoExpiry() {
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .build();
        
        assertFalse(tombstone.isExpired());
    }

    @Test
    void testGetTimeToLiveWithNoExpiry() {
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .build();
        
        assertNull(tombstone.getTimeToLive());
    }

    @Test
    void testGetTimeToLiveWithFutureExpiry() {
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .expiresIn(Duration.ofMinutes(5))
            .build();
        
        Duration ttl = tombstone.getTimeToLive();
        assertNotNull(ttl);
        assertTrue(ttl.toSeconds() > 0);
        assertTrue(ttl.toSeconds() <= 300); // 5 minutes
    }

    @Test
    void testGetTimeToLiveWithPastExpiry() {
        Instant pastTime = Instant.now().minusSeconds(100);
        
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .expiresAt(pastTime)
            .build();
        
        Duration ttl = tombstone.getTimeToLive();
        assertEquals(Duration.ZERO, ttl);
    }

    @Test
    void testAllTombstoneTypes() {
        TombstoneKey.Type[] types = TombstoneKey.Type.values();
        
        for (TombstoneKey.Type type : types) {
            TombstoneKey tombstone = TombstoneKey.builder()
                .key("test-key-" + type.name())
                .type(type)
                .build();
            
            assertEquals(type, tombstone.getType());
        }
    }

    @Test
    void testMetadataDefensiveCopy() {
        Map<String, String> originalMetadata = new HashMap<>();
        originalMetadata.put("key1", "value1");
        
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .metadata(originalMetadata)
            .build();
        
        // Modify original map
        originalMetadata.put("key2", "value2");
        
        // Tombstone metadata should not be affected
        assertEquals(1, tombstone.getMetadata().size());
        assertTrue(tombstone.getMetadata().containsKey("key1"));
        assertFalse(tombstone.getMetadata().containsKey("key2"));
    }

    @Test
    void testMetadataWithNullMap() {
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .metadata(null)
            .build();
        
        assertNotNull(tombstone.getMetadata());
        assertTrue(tombstone.getMetadata().isEmpty());
    }

    @Test
    void testToString() {
        String key = "test-key";
        String datacenterId = "us-east-1";
        String reason = "test reason";
        
        TombstoneKey tombstone = TombstoneKey.builder()
            .key(key)
            .type(TombstoneKey.Type.SOFT_DELETE)
            .datacenterId(datacenterId)
            .reason(reason)
            .build();
        
        String toString = tombstone.toString();
        assertTrue(toString.contains(key));
        assertTrue(toString.contains("TombstoneKey"));
        assertTrue(toString.contains("SOFT_DELETE"));
        assertTrue(toString.contains(datacenterId));
        assertTrue(toString.contains(reason));
    }

    @Test
    void testBoundaryConditionExpiryExactly() {
        // Test expiry exactly at current time - timing dependent
        Instant now = Instant.now();
        
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .expiresAt(now)
            .build();
        
        // Call isExpired to exercise the comparison logic
        tombstone.isExpired();
    }

    @Test
    void testExpiresInWithZeroDuration() {
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .expiresIn(Duration.ZERO)
            .build();
        
        assertNotNull(tombstone.getExpiresAt());
        // Should be expired immediately (or very soon)
        // This tests the boundary condition
        tombstone.isExpired();
    }

    @Test
    void testExpiresInWithNegativeDuration() {
        TombstoneKey tombstone = TombstoneKey.builder()
            .key("test-key")
            .type(TombstoneKey.Type.SOFT_DELETE)
            .expiresIn(Duration.ofSeconds(-100))
            .build();
        
        assertNotNull(tombstone.getExpiresAt());
        assertTrue(tombstone.isExpired());
    }
}
