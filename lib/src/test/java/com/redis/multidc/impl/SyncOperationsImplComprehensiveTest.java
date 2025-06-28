package com.redis.multidc.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import com.redis.multidc.observability.MetricsCollector;
import com.redis.multidc.routing.DatacenterRouter;
import com.redis.multidc.resilience.ResilienceManager;
import com.redis.multidc.config.ResilienceConfig;
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.*;

/**
 * Comprehensive test coverage for SyncOperationsImpl.
 * Tests all methods, error paths, and edge cases to achieve 100% coverage.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SyncOperationsImplComprehensiveTest {

    @Mock
    private DatacenterRouter router;
    
    @Mock
    private MetricsCollector metricsCollector;
    
    @Mock
    private StatefulRedisConnection<String, String> connection;
    
    @Mock
    private RedisCommands<String, String> syncCommands;
    
    @Mock
    private ResilienceManager resilienceManager;
    
    private Map<String, StatefulRedisConnection<String, String>> connections;
    private SyncOperationsImpl syncOperations;

    @BeforeEach
    void setUp() {
        connections = new HashMap<>();
        connections.put("us-east-1", connection);
        
        lenient().when(connection.sync()).thenReturn(syncCommands);
        lenient().when(router.selectDatacenterForRead(any())).thenReturn(Optional.of("us-east-1"));
        lenient().when(router.selectDatacenterForWrite(any())).thenReturn(Optional.of("us-east-1"));
        
        // Setup default successful command responses
        lenient().when(syncCommands.get(anyString())).thenReturn("value");
        lenient().when(syncCommands.set(anyString(), anyString())).thenReturn("OK");
        lenient().when(syncCommands.setex(anyString(), anyLong(), anyString())).thenReturn("OK");
        lenient().when(syncCommands.setnx(anyString(), anyString())).thenReturn(true);
        lenient().when(syncCommands.del(any(String[].class))).thenReturn(1L);
        lenient().when(syncCommands.exists(any(String[].class))).thenReturn(1L);
        lenient().when(syncCommands.expire(anyString(), anyLong())).thenReturn(true);
        lenient().when(syncCommands.ttl(anyString())).thenReturn(300L);
        lenient().when(syncCommands.hget(anyString(), anyString())).thenReturn("value");
        lenient().when(syncCommands.hset(anyString(), anyString(), anyString())).thenReturn(true);
        lenient().when(syncCommands.hmset(anyString(), anyMap())).thenReturn("OK");
        lenient().when(syncCommands.hdel(anyString(), any(String[].class))).thenReturn(1L);

        // Mock ResilienceManager to pass through operations unchanged
        when(resilienceManager.decorateSupplier(anyString(), any())).thenAnswer(invocation -> invocation.getArgument(1));

        syncOperations = new SyncOperationsImpl(router, connections, metricsCollector, resilienceManager);
    }

    // ===== String Operations Tests =====
    
    @Test
    void testGetSuccess() {
        when(syncCommands.get("key")).thenReturn("value");

        String result = syncOperations.get("key");

        assertEquals("value", result);
        verify(router).selectDatacenterForRead(DatacenterPreference.LOCAL_PREFERRED);
        verify(syncCommands).get("key");
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(true));
    }

    @Test
    void testGetWithPreference() {
        when(syncCommands.get("key")).thenReturn("value");

        String result = syncOperations.get("key", DatacenterPreference.ANY_AVAILABLE);

        assertEquals("value", result);
        verify(router).selectDatacenterForRead(DatacenterPreference.ANY_AVAILABLE);
    }

    @Test
    void testGetNoDatacenterAvailable() {
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.empty());

        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> syncOperations.get("key"));
        
        assertEquals("No available datacenter for read operation", exception.getMessage());
    }

    @Test
    void testGetConnectionFailure() {
        connections.clear(); // Remove all connections
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.of("us-east-1"));

        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> syncOperations.get("key"));
        
        assertTrue(exception.getMessage().contains("Redis operation failed") ||
                   exception.getCause().getMessage().contains("No connection available"));
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(false));
    }

    @Test
    void testSetSuccess() {
        syncOperations.set("key", "value");

        verify(syncCommands).set("key", "value");
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(true));
    }

    @Test
    void testSetWithPreference() {
        syncOperations.set("key", "value", DatacenterPreference.LOCAL_ONLY);

        verify(router).selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
        verify(syncCommands).set("key", "value");
    }

    @Test
    void testSetWithTtl() {
        syncOperations.set("key", "value", Duration.ofMinutes(5));

        verify(syncCommands).setex("key", 300, "value");
    }

    @Test
    void testSetWithTtlAndPreference() {
        syncOperations.set("key", "value", Duration.ofMinutes(5), DatacenterPreference.REMOTE_ONLY);

        verify(router).selectDatacenterForWrite(DatacenterPreference.REMOTE_ONLY);
        verify(syncCommands).setex("key", 300, "value");
    }

    @Test
    void testSetIfNotExistsSuccess() {
        when(syncCommands.setnx("key", "value")).thenReturn(true);

        boolean result = syncOperations.setIfNotExists("key", "value");

        assertTrue(result);
        verify(syncCommands).setnx("key", "value");
    }

    @Test
    void testSetIfNotExistsWithTtl() {
        when(syncCommands.set(eq("key"), eq("value"), any())).thenReturn("OK");

        boolean result = syncOperations.setIfNotExists("key", "value", Duration.ofMinutes(5));

        assertTrue(result);
        verify(syncCommands).set(eq("key"), eq("value"), any());
    }

    @Test
    void testSetIfNotExistsWithPreference() {
        when(syncCommands.setnx("key", "value")).thenReturn(false);

        boolean result = syncOperations.setIfNotExists("key", "value", DatacenterPreference.LOCAL_ONLY);

        assertFalse(result);
        verify(router).selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
    }

    @Test
    void testDeleteSuccess() {
        when(syncCommands.del("key")).thenReturn(1L);

        boolean result = syncOperations.delete("key");

        assertTrue(result);
        verify(syncCommands).del("key");
    }

    @Test
    void testDeleteMultipleKeys() {
        when(syncCommands.del("key1", "key2")).thenReturn(2L);

        long result = syncOperations.delete("key1", "key2");

        assertEquals(2L, result);
        verify(syncCommands).del("key1", "key2");
    }

    @Test
    void testExists() {
        when(syncCommands.exists("key")).thenReturn(1L);

        boolean result = syncOperations.exists("key");

        assertTrue(result);
        verify(syncCommands).exists("key");
    }

    @Test
    void testTtl() {
        when(syncCommands.ttl("key")).thenReturn(300L);

        Duration result = syncOperations.ttl("key");

        assertEquals(Duration.ofSeconds(300), result);
        verify(syncCommands).ttl("key");
    }

    @Test
    void testTtlNoExpiry() {
        when(syncCommands.ttl("key")).thenReturn(-1L);

        Duration result = syncOperations.ttl("key");

        assertNull(result);
    }

    @Test
    void testExpire() {
        when(syncCommands.expire("key", 300)).thenReturn(true);

        boolean result = syncOperations.expire("key", Duration.ofMinutes(5));

        assertTrue(result);
        verify(syncCommands).expire("key", 300);
    }

    // ===== Hash Operations Tests =====

    @Test
    void testHset() {
        when(syncCommands.hset("hash", "field", "value")).thenReturn(true);

        syncOperations.hset("hash", "field", "value");

        verify(syncCommands).hset("hash", "field", "value");
    }

    @Test
    void testHsetMap() {
        Map<String, String> fieldValues = Map.of("field1", "value1", "field2", "value2");
        when(syncCommands.hmset("hash", fieldValues)).thenReturn("OK");

        syncOperations.hset("hash", fieldValues);

        verify(syncCommands).hmset("hash", fieldValues);
    }

    @Test
    void testHget() {
        when(syncCommands.hget("hash", "field")).thenReturn("value");

        String result = syncOperations.hget("hash", "field");

        assertEquals("value", result);
        verify(syncCommands).hget("hash", "field");
    }

    @Test
    void testHgetAll() {
        Map<String, String> hash = Map.of("field1", "value1", "field2", "value2");
        when(syncCommands.hgetall("hash")).thenReturn(hash);

        Map<String, String> result = syncOperations.hgetAll("hash");

        assertEquals(hash, result);
        verify(syncCommands).hgetall("hash");
    }

    @Test
    void testHexists() {
        when(syncCommands.hexists("hash", "field")).thenReturn(true);

        boolean result = syncOperations.hexists("hash", "field");

        assertTrue(result);
        verify(syncCommands).hexists("hash", "field");
    }

    @Test
    void testHdel() {
        when(syncCommands.hdel("hash", "field")).thenReturn(1L);

        boolean result = syncOperations.hdel("hash", "field");

        assertTrue(result);
        verify(syncCommands).hdel("hash", "field");
    }

    @Test
    void testHdelMultipleFields() {
        when(syncCommands.hdel("hash", "field1", "field2")).thenReturn(2L);

        long result = syncOperations.hdel("hash", "field1", "field2");

        assertEquals(2L, result);
        verify(syncCommands).hdel("hash", "field1", "field2");
    }

    @Test
    void testHkeys() {
        List<String> keys = List.of("field1", "field2");
        when(syncCommands.hkeys("hash")).thenReturn(keys);

        Set<String> result = syncOperations.hkeys("hash");

        assertEquals(new HashSet<>(keys), result);
        verify(syncCommands).hkeys("hash");
    }

    // ===== List Operations Tests =====

    @Test
    void testLpush() {
        when(syncCommands.lpush("list", "value1", "value2")).thenReturn(2L);

        syncOperations.lpush("list", "value1", "value2");

        verify(syncCommands).lpush("list", "value1", "value2");
    }

    @Test
    void testLpushWithPreference() {
        when(syncCommands.lpush("list", "value1", "value2")).thenReturn(2L);

        syncOperations.lpush("list", DatacenterPreference.LOCAL_ONLY, "value1", "value2");

        verify(router).selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
        verify(syncCommands).lpush("list", "value1", "value2");
    }

    @Test
    void testRpush() {
        when(syncCommands.rpush("list", "value1", "value2")).thenReturn(2L);

        syncOperations.rpush("list", "value1", "value2");

        verify(syncCommands).rpush("list", "value1", "value2");
    }

    @Test
    void testLpop() {
        when(syncCommands.lpop("list")).thenReturn("value");

        String result = syncOperations.lpop("list");

        assertEquals("value", result);
        verify(syncCommands).lpop("list");
    }

    @Test
    void testRpop() {
        when(syncCommands.rpop("list")).thenReturn("value");

        String result = syncOperations.rpop("list");

        assertEquals("value", result);
        verify(syncCommands).rpop("list");
    }

    @Test
    void testLrange() {
        List<String> list = List.of("value1", "value2");
        when(syncCommands.lrange("list", 0, -1)).thenReturn(list);

        List<String> result = syncOperations.lrange("list", 0, -1);

        assertEquals(list, result);
        verify(syncCommands).lrange("list", 0, -1);
    }

    @Test
    void testLlen() {
        when(syncCommands.llen("list")).thenReturn(5L);

        long result = syncOperations.llen("list");

        assertEquals(5L, result);
        verify(syncCommands).llen("list");
    }

    // ===== Set Operations Tests =====

    @Test
    void testSadd() {
        when(syncCommands.sadd("set", "member1", "member2")).thenReturn(2L);

        boolean result = syncOperations.sadd("set", "member1", "member2");

        assertTrue(result);
        verify(syncCommands).sadd("set", "member1", "member2");
    }

    @Test
    void testSrem() {
        when(syncCommands.srem("set", "member")).thenReturn(1L);

        boolean result = syncOperations.srem("set", "member");

        assertTrue(result);
        verify(syncCommands).srem("set", "member");
    }

    @Test
    void testSismember() {
        when(syncCommands.sismember("set", "member")).thenReturn(true);

        boolean result = syncOperations.sismember("set", "member");

        assertTrue(result);
        verify(syncCommands).sismember("set", "member");
    }

    @Test
    void testSmembers() {
        Set<String> members = Set.of("member1", "member2");
        when(syncCommands.smembers("set")).thenReturn(members);

        Set<String> result = syncOperations.smembers("set");

        assertEquals(members, result);
        verify(syncCommands).smembers("set");
    }

    @Test
    void testScard() {
        when(syncCommands.scard("set")).thenReturn(3L);

        long result = syncOperations.scard("set");

        assertEquals(3L, result);
        verify(syncCommands).scard("set");
    }

    // ===== Sorted Set Operations Tests =====

    @Test
    void testZadd() {
        when(syncCommands.zadd("zset", 1.0, "member")).thenReturn(1L);

        boolean result = syncOperations.zadd("zset", 1.0, "member");

        assertTrue(result);
        verify(syncCommands).zadd("zset", 1.0, "member");
    }

    @Test
    void testZaddMap() {
        Map<String, Double> scoreMembers = Map.of("member1", 1.0, "member2", 2.0);
        when(syncCommands.zadd(eq("zset"), any(Object[].class))).thenReturn(2L);

        boolean result = syncOperations.zadd("zset", scoreMembers);

        assertTrue(result);
    }

    @Test
    void testZrem() {
        when(syncCommands.zrem("zset", "member")).thenReturn(1L);

        boolean result = syncOperations.zrem("zset", "member");

        assertTrue(result);
        verify(syncCommands).zrem("zset", "member");
    }

    @Test
    void testZrange() {
        List<String> range = List.of("member1", "member2");
        when(syncCommands.zrange("zset", 0, -1)).thenReturn(range);

        Set<String> result = syncOperations.zrange("zset", 0, -1);

        assertEquals(new HashSet<>(range), result);
        verify(syncCommands).zrange("zset", 0, -1);
    }

    @Test
    void testZscore() {
        when(syncCommands.zscore("zset", "member")).thenReturn(1.5);

        Double result = syncOperations.zscore("zset", "member");

        assertEquals(1.5, result);
        verify(syncCommands).zscore("zset", "member");
    }

    @Test
    void testZcard() {
        when(syncCommands.zcard("zset")).thenReturn(5L);

        long result = syncOperations.zcard("zset");

        assertEquals(5L, result);
        verify(syncCommands).zcard("zset");
    }

    // ===== Batch Operations Tests =====

    @Test
    void testMget() {
        List<KeyValue<String, String>> keyValues = List.of(
            KeyValue.just("key1", "value1"),
            KeyValue.just("key2", "value2")
        );
        when(syncCommands.mget("key1", "key2")).thenReturn(keyValues);

        List<String> result = syncOperations.mget("key1", "key2");

        List<String> expected = List.of("value1", "value2");
        assertEquals(expected, result);
        verify(syncCommands).mget("key1", "key2");
    }

    @Test
    void testMgetWithPreference() {
        List<KeyValue<String, String>> keyValues = List.of(
            KeyValue.just("key1", "value1")
        );
        when(syncCommands.mget("key1")).thenReturn(keyValues);

        syncOperations.mget(DatacenterPreference.REMOTE_ONLY, "key1");

        verify(router).selectDatacenterForRead(DatacenterPreference.REMOTE_ONLY);
    }

    @Test
    void testMset() {
        Map<String, String> keyValues = Map.of("key1", "value1", "key2", "value2");
        when(syncCommands.mset(keyValues)).thenReturn("OK");

        syncOperations.mset(keyValues);

        verify(syncCommands).mset(keyValues);
    }

    @Test
    void testMsetWithPreference() {
        Map<String, String> keyValues = Map.of("key1", "value1");
        when(syncCommands.mset(keyValues)).thenReturn("OK");

        syncOperations.mset(keyValues, DatacenterPreference.LOCAL_ONLY);

        verify(router).selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
    }

    // ===== Cross-Datacenter Operations Tests =====

    @Test
    void testCrossDatacenterGet() {
        connections.put("us-west-1", connection);
        when(syncCommands.get("key")).thenReturn("remote-value");

        String result = syncOperations.crossDatacenterGet("key", "us-west-1");

        assertEquals("remote-value", result);
        verify(metricsCollector).recordRequest(eq("us-west-1"), any(Duration.class), eq(true));
    }

    @Test
    void testCrossDatacenterGetInvalidDatacenter() {
        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> syncOperations.crossDatacenterGet("key", "invalid-dc"));
        
        assertTrue(exception.getMessage().contains("Redis operation failed") || 
                   exception.getCause().getMessage().contains("No connection available"));
        verify(metricsCollector).recordRequest(eq("invalid-dc"), any(Duration.class), eq(false));
    }

    @Test
    void testCrossDatacenterMultiGet() {
        connections.put("us-west-1", connection);
        List<String> keys = List.of("key1", "key2");
        List<KeyValue<String, String>> keyValues = List.of(
            KeyValue.just("key1", "value1"),
            KeyValue.just("key2", "value2")
        );
        when(syncCommands.mget("key1", "key2")).thenReturn(keyValues);

        Map<String, String> result = syncOperations.crossDatacenterMultiGet(keys, "us-west-1");

        Map<String, String> expected = Map.of("key1", "value1", "key2", "value2");
        assertEquals(expected, result);
    }

    // ===== Tombstone Operations Tests =====

    @Test
    void testCreateTombstone() {
        syncOperations.createTombstone("key", TombstoneKey.Type.SOFT_DELETE);

        // Note: Current implementation is a stub that only logs
        // No Redis operations are performed yet
    }

    @Test
    void testCreateTombstoneWithTtl() {
        syncOperations.createTombstone("key", TombstoneKey.Type.CACHE_INVALIDATION, Duration.ofMinutes(5));

        // Note: Current implementation is a stub that only logs
        // No Redis operations are performed yet
    }

    @Test
    void testIsTombstoned() {
        when(syncCommands.get(anyString())).thenReturn("tombstone-data");

        boolean result = syncOperations.isTombstoned("key");

        assertTrue(result);
    }

    @Test
    void testIsTombstonedNotFound() {
        when(syncCommands.get(anyString())).thenReturn(null);

        boolean result = syncOperations.isTombstoned("key");

        assertFalse(result);
    }

    @Test
    void testGetTombstone() {
        String tombstoneData = "{\"type\":\"SOFT_DELETE\",\"createdAt\":\"2023-01-01T00:00:00Z\",\"ttl\":null}";
        when(syncCommands.get(anyString())).thenReturn(tombstoneData);

        TombstoneKey result = syncOperations.getTombstone("key");

        assertNotNull(result);
        assertEquals(TombstoneKey.Type.SOFT_DELETE, result.getType());
    }

    @Test
    void testGetTombstoneNotFound() {
        when(syncCommands.get(anyString())).thenReturn(null);

        TombstoneKey result = syncOperations.getTombstone("key");

        assertNull(result);
    }

    @Test
    void testRemoveTombstone() {
        syncOperations.removeTombstone("key");

        // Note: Current implementation is a stub that only logs
        // No Redis operations are performed yet
    }

    // ===== Distributed Lock Operations Tests =====

    @Test
    void testAcquireLock() {
        lenient().when(syncCommands.set(anyString(), anyString(), any())).thenReturn("OK");

        boolean result = syncOperations.acquireLock("resource", "owner", Duration.ofMinutes(5));

        assertTrue(result);
    }

    @Test
    void testAcquireLockFailed() {
        when(syncCommands.set(anyString(), anyString(), any(io.lettuce.core.SetArgs.class))).thenReturn(null);

        boolean result = syncOperations.acquireLock("resource", "owner", Duration.ofMinutes(5));

        assertFalse(result);
    }

    @Test
    void testAcquireLockWithPreference() {
        lenient().when(syncCommands.set(anyString(), anyString(), any())).thenReturn("OK");

        boolean result = syncOperations.acquireLock("resource", "owner", Duration.ofMinutes(5), DatacenterPreference.LOCAL_ONLY);

        assertTrue(result);
        verify(router).selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
    }

    @Test
    void testReleaseLock() {
        when(syncCommands.eval(anyString(), any(io.lettuce.core.ScriptOutputType.class), any(String[].class), anyString())).thenReturn(1L);

        boolean result = syncOperations.releaseLock("resource", "owner");

        assertTrue(result);
    }

    @Test
    void testReleaseLockFailed() {
        lenient().when(syncCommands.eval(anyString(), any(io.lettuce.core.ScriptOutputType.class), any(String[].class), anyString())).thenReturn(0L);

        boolean result = syncOperations.releaseLock("resource", "owner");

        assertFalse(result);
    }

    @Test
    void testIsLocked() {
        lenient().when(syncCommands.get(anyString())).thenReturn("owner-value");

        boolean result = syncOperations.isLocked("resource");

        assertTrue(result);
    }

    @Test
    void testIsLockedNotFound() {
        when(syncCommands.exists(anyString())).thenReturn(0L);

        boolean result = syncOperations.isLocked("resource");

        assertFalse(result);
    }

    // ===== Utility Operations Tests =====

    @Test
    void testPing() {
        when(syncCommands.ping()).thenReturn("PONG");

        String result = syncOperations.ping();

        assertEquals("PONG", result);
        verify(syncCommands).ping();
    }

    @Test
    void testPingSpecificDatacenter() {
        connections.put("us-west-1", connection);
        when(syncCommands.ping()).thenReturn("PONG");

        String result = syncOperations.ping("us-west-1");

        assertEquals("PONG", result);
        verify(metricsCollector).recordRequest(eq("us-west-1"), any(Duration.class), eq(true));
    }

    @Test
    void testDbSize() {
        when(syncCommands.dbsize()).thenReturn(100L);

        long result = syncOperations.dbSize();

        assertEquals(100L, result);
        verify(syncCommands).dbsize();
    }

    @Test
    void testDbSizeSpecificDatacenter() {
        connections.put("us-west-1", connection);
        when(syncCommands.dbsize()).thenReturn(200L);

        long result = syncOperations.dbSize("us-west-1");

        assertEquals(200L, result);
    }

    @Test
    void testFlushAll() {
        when(syncCommands.flushall()).thenReturn("OK");

        syncOperations.flushAll();

        verify(syncCommands).flushall();
    }

    @Test
    void testFlushAllSpecificDatacenter() {
        connections.put("us-west-1", connection);
        when(syncCommands.flushall()).thenReturn("OK");

        syncOperations.flushAll("us-west-1");

        verify(syncCommands).flushall();
    }

    // ===== Error Handling Tests =====

    @Test
    void testOperationException() {
        when(syncCommands.get("key")).thenThrow(new RuntimeException("Redis error"));

        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> syncOperations.get("key"));
        
        assertTrue(exception.getMessage().contains("Redis operation failed"));
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(false));
    }

    @Test
    void testMetricsRecordingOnException() {
        when(syncCommands.set("key", "value")).thenThrow(new RuntimeException("Redis error"));

        assertThrows(RuntimeException.class,
            () -> syncOperations.set("key", "value"));
        
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(false));
    }

    @Test
    void testWriteOperationNoDatacenterAvailable() {
        when(router.selectDatacenterForWrite(any())).thenReturn(Optional.empty());

        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> syncOperations.set("key", "value"));
        
        assertEquals("No available datacenter for write operation", exception.getMessage());
    }
}
