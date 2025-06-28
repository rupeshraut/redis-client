package com.redis.multidc.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import com.redis.multidc.observability.MetricsCollector;
import com.redis.multidc.routing.DatacenterRouter;
import com.redis.multidc.resilience.ResilienceManager;
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.RedisFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Comprehensive test coverage for AsyncOperationsImpl.
 * Tests all methods, error paths, and edge cases to achieve 100% coverage.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AsyncOperationsImplComprehensiveTest {

    @Mock
    private DatacenterRouter router;
    
    @Mock
    private MetricsCollector metricsCollector;
    
    @Mock
    private StatefulRedisConnection<String, String> connection;
    
    @Mock
    private RedisAsyncCommands<String, String> asyncCommands;
    
    @Mock
    private RedisFuture<String> stringFuture;
    
    @Mock
    private RedisFuture<Boolean> booleanFuture;
    
    @Mock
    private RedisFuture<Long> longFuture;
    
    @Mock
    private RedisFuture<Double> doubleFuture;
    
    @Mock
    private RedisFuture<List<String>> listStringFuture;
    
    @Mock
    private RedisFuture<List<KeyValue<String, String>>> listKeyValueFuture;
    
    @Mock
    private RedisFuture<Set<String>> setStringFuture;
    
    @Mock
    private RedisFuture<Map<String, String>> mapStringFuture;
    
    @Mock
    private RedisFuture<Object> objectFuture;
    
    @Mock
    private ResilienceManager resilienceManager;
    
    private Map<String, StatefulRedisConnection<String, String>> connections;
    private AsyncOperationsImpl asyncOperations;

    @BeforeEach
    void setUp() {
        connections = new HashMap<>();
        connections.put("us-east-1", connection);
        
        lenient().when(connection.async()).thenReturn(asyncCommands);
        
        // Setup default router behavior with lenient stubs
        lenient().when(router.selectDatacenterForRead(any())).thenReturn(Optional.of("us-east-1"));
        lenient().when(router.selectDatacenterForWrite(any())).thenReturn(Optional.of("us-east-1"));
        
        // Setup default future behaviors with lenient stubs
        lenient().when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("value"));
        lenient().when(booleanFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));
        lenient().when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        
        // Setup default Redis command behaviors with lenient stubs
        lenient().when(asyncCommands.get(anyString())).thenReturn(stringFuture);
        lenient().when(asyncCommands.set(anyString(), anyString())).thenReturn(stringFuture);
        lenient().when(asyncCommands.setex(anyString(), anyLong(), anyString())).thenReturn(stringFuture);
        lenient().when(asyncCommands.del(any(String[].class))).thenReturn(longFuture);
        lenient().when(asyncCommands.exists(any(String[].class))).thenReturn(longFuture);
        lenient().when(asyncCommands.setnx(anyString(), anyString())).thenReturn(booleanFuture);
        lenient().when(asyncCommands.hset(anyString(), anyString(), anyString())).thenReturn(booleanFuture);
        lenient().when(asyncCommands.hmset(anyString(), anyMap())).thenReturn(stringFuture);

        // Mock ResilienceManager to pass through operations unchanged
        when(resilienceManager.decorateCompletionStage(anyString(), any())).thenAnswer(invocation -> invocation.getArgument(1));

        asyncOperations = new AsyncOperationsImpl(router, connections, metricsCollector, resilienceManager);
    }

    // ===== String Operations Tests =====
    
    @Test
    void testGetSuccess() {
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.of("us-east-1"));
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("value"));
        when(asyncCommands.get("key")).thenReturn(stringFuture);

        CompletableFuture<String> result = asyncOperations.get("key");

        assertEquals("value", result.join());
        verify(router).selectDatacenterForRead(DatacenterPreference.LOCAL_PREFERRED);
        verify(asyncCommands).get("key");
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(true));
    }

    @Test
    void testGetWithPreference() {
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.of("us-east-1"));
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("value"));
        when(asyncCommands.get("key")).thenReturn(stringFuture);

        CompletableFuture<String> result = asyncOperations.get("key", DatacenterPreference.ANY_AVAILABLE);

        assertEquals("value", result.join());
        verify(router).selectDatacenterForRead(DatacenterPreference.ANY_AVAILABLE);
    }

    @Test
    void testGetNoDatacenterAvailable() {
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.empty());

        CompletableFuture<String> result = asyncOperations.get("key");

        assertTrue(result.isCompletedExceptionally());
        assertThrows(CompletionException.class, result::join);
    }

    @Test
    void testGetConnectionFailure() {
        connections.clear(); // Remove all connections
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.of("us-east-1"));

        CompletableFuture<String> result = asyncOperations.get("key");

        assertTrue(result.isCompletedExceptionally());
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(false));
    }

    @Test
    void testSetSuccess() {
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(asyncCommands.set("key", "value")).thenReturn(stringFuture);

        CompletableFuture<Void> result = asyncOperations.set("key", "value");

        assertDoesNotThrow(result::join);
        verify(asyncCommands).set("key", "value");
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(true));
    }

    @Test
    void testSetWithPreference() {
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(asyncCommands.set("key", "value")).thenReturn(stringFuture);

        CompletableFuture<Void> result = asyncOperations.set("key", "value", DatacenterPreference.LOCAL_ONLY);

        assertDoesNotThrow(result::join);
        verify(router).selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
    }

    @Test
    void testSetWithTtl() {
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(asyncCommands.setex("key", 300, "value")).thenReturn(stringFuture);

        CompletableFuture<Void> result = asyncOperations.set("key", "value", Duration.ofMinutes(5));

        assertDoesNotThrow(result::join);
        verify(asyncCommands).setex("key", 300, "value");
    }

    @Test
    void testSetWithTtlAndPreference() {
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(asyncCommands.setex("key", 300, "value")).thenReturn(stringFuture);

        CompletableFuture<Void> result = asyncOperations.set("key", "value", Duration.ofMinutes(5), DatacenterPreference.REMOTE_ONLY);

        assertDoesNotThrow(result::join);
        verify(router).selectDatacenterForWrite(DatacenterPreference.REMOTE_ONLY);
        verify(asyncCommands).setex("key", 300, "value");
    }

    @Test
    void testSetIfNotExistsSuccess() {
        when(booleanFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));
        when(asyncCommands.setnx("key", "value")).thenReturn(booleanFuture);

        CompletableFuture<Boolean> result = asyncOperations.setIfNotExists("key", "value");

        assertTrue(result.join());
        verify(asyncCommands).setnx("key", "value");
    }

    @Test
    void testSetIfNotExistsWithTtl() {
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(asyncCommands.set(eq("key"), eq("value"), any())).thenReturn(stringFuture);

        CompletableFuture<Boolean> result = asyncOperations.setIfNotExists("key", "value", Duration.ofMinutes(5));

        assertTrue(result.join());
        verify(asyncCommands).set(eq("key"), eq("value"), any());
    }

    @Test
    void testDeleteSuccess() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        when(asyncCommands.del("key")).thenReturn(longFuture);

        CompletableFuture<Boolean> result = asyncOperations.delete("key");

        assertTrue(result.join());
        verify(asyncCommands).del("key");
    }

    @Test
    void testDeleteMultipleKeys() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(2L));
        when(asyncCommands.del("key1", "key2")).thenReturn(longFuture);

        CompletableFuture<Long> result = asyncOperations.delete("key1", "key2");

        assertEquals(2L, result.join());
        verify(asyncCommands).del("key1", "key2");
    }

    @Test
    void testExists() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        when(asyncCommands.exists("key")).thenReturn(longFuture);

        CompletableFuture<Boolean> result = asyncOperations.exists("key");

        assertTrue(result.join());
        verify(asyncCommands).exists("key");
    }

    @Test
    void testTtl() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(300L));
        when(asyncCommands.ttl("key")).thenReturn(longFuture);

        CompletableFuture<Duration> result = asyncOperations.ttl("key");

        assertEquals(Duration.ofSeconds(300), result.join());
        verify(asyncCommands).ttl("key");
    }

    @Test
    void testExpire() {
        when(booleanFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));
        when(asyncCommands.expire("key", 300)).thenReturn(booleanFuture);

        CompletableFuture<Boolean> result = asyncOperations.expire("key", Duration.ofMinutes(5));

        assertTrue(result.join());
        verify(asyncCommands).expire("key", 300);
    }

    // ===== Hash Operations Tests =====

    @Test
    void testHset() {
        when(booleanFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));
        when(asyncCommands.hset("hash", "field", "value")).thenReturn(booleanFuture);

        CompletableFuture<Void> result = asyncOperations.hset("hash", "field", "value");

        assertDoesNotThrow(result::join);
        verify(asyncCommands).hset("hash", "field", "value");
    }

    @Test
    void testHsetMap() {
        Map<String, String> fieldValues = Map.of("field1", "value1", "field2", "value2");
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(asyncCommands.hmset(eq("hash"), eq(fieldValues))).thenReturn(stringFuture);

        CompletableFuture<Void> result = asyncOperations.hset("hash", fieldValues);

        assertDoesNotThrow(result::join);
        verify(asyncCommands).hmset("hash", fieldValues);
    }

    @Test
    void testHget() {
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("value"));
        when(asyncCommands.hget("hash", "field")).thenReturn(stringFuture);

        CompletableFuture<String> result = asyncOperations.hget("hash", "field");

        assertEquals("value", result.join());
        verify(asyncCommands).hget("hash", "field");
    }

    @Test
    void testHgetAll() {
        Map<String, String> hash = Map.of("field1", "value1", "field2", "value2");
        when(mapStringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(hash));
        when(asyncCommands.hgetall("hash")).thenReturn(mapStringFuture);

        CompletableFuture<Map<String, String>> result = asyncOperations.hgetAll("hash");

        assertEquals(hash, result.join());
        verify(asyncCommands).hgetall("hash");
    }

    @Test
    void testHexists() {
        when(booleanFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));
        when(asyncCommands.hexists("hash", "field")).thenReturn(booleanFuture);

        CompletableFuture<Boolean> result = asyncOperations.hexists("hash", "field");

        assertTrue(result.join());
        verify(asyncCommands).hexists("hash", "field");
    }

    @Test
    void testHdel() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        when(asyncCommands.hdel("hash", "field")).thenReturn(longFuture);

        CompletableFuture<Boolean> result = asyncOperations.hdel("hash", "field");

        assertTrue(result.join());
        verify(asyncCommands).hdel("hash", "field");
    }

    @Test
    void testHdelMultipleFields() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(2L));
        when(asyncCommands.hdel("hash", "field1", "field2")).thenReturn(longFuture);

        CompletableFuture<Long> result = asyncOperations.hdel("hash", "field1", "field2");

        assertEquals(2L, result.join());
        verify(asyncCommands).hdel("hash", "field1", "field2");
    }

    @Test
    void testHkeys() {
        List<String> keys = List.of("field1", "field2");
        when(listStringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(keys));
        when(asyncCommands.hkeys("hash")).thenReturn(listStringFuture);

        CompletableFuture<Set<String>> result = asyncOperations.hkeys("hash");

        assertEquals(new HashSet<>(keys), result.join());
        verify(asyncCommands).hkeys("hash");
    }

    // ===== List Operations Tests =====

    @Test
    void testLpush() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(2L));
        when(asyncCommands.lpush("list", "value1", "value2")).thenReturn(longFuture);

        CompletableFuture<Void> result = asyncOperations.lpush("list", "value1", "value2");

        assertDoesNotThrow(result::join);
        verify(asyncCommands).lpush("list", "value1", "value2");
    }

    @Test
    void testLpushWithPreference() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(2L));
        when(asyncCommands.lpush("list", "value1", "value2")).thenReturn(longFuture);

        CompletableFuture<Void> result = asyncOperations.lpush("list", DatacenterPreference.LOCAL_ONLY, "value1", "value2");

        assertDoesNotThrow(result::join);
        verify(router).selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
        verify(asyncCommands).lpush("list", "value1", "value2");
    }

    @Test
    void testRpush() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(2L));
        when(asyncCommands.rpush("list", "value1", "value2")).thenReturn(longFuture);

        CompletableFuture<Void> result = asyncOperations.rpush("list", "value1", "value2");

        assertDoesNotThrow(result::join);
        verify(asyncCommands).rpush("list", "value1", "value2");
    }

    @Test
    void testLpop() {
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("value"));
        when(asyncCommands.lpop("list")).thenReturn(stringFuture);

        CompletableFuture<String> result = asyncOperations.lpop("list");

        assertEquals("value", result.join());
        verify(asyncCommands).lpop("list");
    }

    @Test
    void testRpop() {
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("value"));
        when(asyncCommands.rpop("list")).thenReturn(stringFuture);

        CompletableFuture<String> result = asyncOperations.rpop("list");

        assertEquals("value", result.join());
        verify(asyncCommands).rpop("list");
    }

    @Test
    void testLrange() {
        List<String> list = List.of("value1", "value2");
        when(listStringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(list));
        when(asyncCommands.lrange("list", 0, -1)).thenReturn(listStringFuture);

        CompletableFuture<List<String>> result = asyncOperations.lrange("list", 0, -1);

        assertEquals(list, result.join());
        verify(asyncCommands).lrange("list", 0, -1);
    }

    @Test
    void testLlen() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(5L));
        when(asyncCommands.llen("list")).thenReturn(longFuture);

        CompletableFuture<Long> result = asyncOperations.llen("list");

        assertEquals(5L, result.join());
        verify(asyncCommands).llen("list");
    }

    // ===== Set Operations Tests =====

    @Test
    void testSadd() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(2L));
        when(asyncCommands.sadd("set", "member1", "member2")).thenReturn(longFuture);

        CompletableFuture<Boolean> result = asyncOperations.sadd("set", "member1", "member2");

        assertTrue(result.join());
        verify(asyncCommands).sadd("set", "member1", "member2");
    }

    @Test
    void testSrem() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        when(asyncCommands.srem("set", "member")).thenReturn(longFuture);

        CompletableFuture<Boolean> result = asyncOperations.srem("set", "member");

        assertTrue(result.join());
        verify(asyncCommands).srem("set", "member");
    }

    @Test
    void testSismember() {
        when(booleanFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(true));
        when(asyncCommands.sismember("set", "member")).thenReturn(booleanFuture);

        CompletableFuture<Boolean> result = asyncOperations.sismember("set", "member");

        assertTrue(result.join());
        verify(asyncCommands).sismember("set", "member");
    }

    @Test
    void testSmembers() {
        Set<String> members = Set.of("member1", "member2");
        when(setStringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(members));
        when(asyncCommands.smembers("set")).thenReturn(setStringFuture);

        CompletableFuture<Set<String>> result = asyncOperations.smembers("set");

        assertEquals(members, result.join());
        verify(asyncCommands).smembers("set");
    }

    @Test
    void testScard() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(3L));
        when(asyncCommands.scard("set")).thenReturn(longFuture);

        CompletableFuture<Long> result = asyncOperations.scard("set");

        assertEquals(3L, result.join());
        verify(asyncCommands).scard("set");
    }

    // ===== Sorted Set Operations Tests =====

    @Test
    void testZadd() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        when(asyncCommands.zadd("zset", 1.0, "member")).thenReturn(longFuture);

        CompletableFuture<Boolean> result = asyncOperations.zadd("zset", 1.0, "member");

        assertTrue(result.join());
        verify(asyncCommands).zadd("zset", 1.0, "member");
    }

    @Test
    void testZaddMap() {
        Map<String, Double> scoreMembers = Map.of("member1", 1.0, "member2", 2.0);
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(2L));
        when(asyncCommands.zadd(eq("zset"), any(Object[].class))).thenReturn(longFuture);

        CompletableFuture<Boolean> result = asyncOperations.zadd("zset", scoreMembers);

        assertTrue(result.join());
    }

    @Test
    void testZrem() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        when(asyncCommands.zrem("zset", "member")).thenReturn(longFuture);

        CompletableFuture<Boolean> result = asyncOperations.zrem("zset", "member");

        assertTrue(result.join());
        verify(asyncCommands).zrem("zset", "member");
    }

    @Test
    void testZrange() {
        List<String> range = List.of("member1", "member2");
        when(listStringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(range));
        when(asyncCommands.zrange("zset", 0, -1)).thenReturn(listStringFuture);

        CompletableFuture<Set<String>> result = asyncOperations.zrange("zset", 0, -1);

        assertEquals(new HashSet<>(range), result.join());
        verify(asyncCommands).zrange("zset", 0, -1);
    }

    @Test
    void testZscore() {
        when(doubleFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1.5));
        when(asyncCommands.zscore("zset", "member")).thenReturn(doubleFuture);

        CompletableFuture<Double> result = asyncOperations.zscore("zset", "member");

        assertEquals(1.5, result.join());
        verify(asyncCommands).zscore("zset", "member");
    }

    @Test
    void testZcard() {
        when(longFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(5L));
        when(asyncCommands.zcard("zset")).thenReturn(longFuture);

        CompletableFuture<Long> result = asyncOperations.zcard("zset");

        assertEquals(5L, result.join());
        verify(asyncCommands).zcard("zset");
    }

    // ===== Batch Operations Tests =====

    @Test
    void testMget() {
        List<KeyValue<String, String>> keyValues = List.of(
            KeyValue.just("key1", "value1"),
            KeyValue.just("key2", "value2")
        );
        when(listKeyValueFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(keyValues));
        when(asyncCommands.mget("key1", "key2")).thenReturn(listKeyValueFuture);

        CompletableFuture<List<String>> result = asyncOperations.mget("key1", "key2");

        List<String> expected = List.of("value1", "value2");
        assertEquals(expected, result.join());
        verify(asyncCommands).mget("key1", "key2");
    }

    @Test
    void testMgetWithPreference() {
        List<KeyValue<String, String>> keyValues = List.of(
            KeyValue.just("key1", "value1")
        );
        when(listKeyValueFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(keyValues));
        when(asyncCommands.mget("key1")).thenReturn(listKeyValueFuture);

        asyncOperations.mget(DatacenterPreference.REMOTE_ONLY, "key1");

        verify(router).selectDatacenterForRead(DatacenterPreference.REMOTE_ONLY);
    }

    @Test
    void testMset() {
        Map<String, String> keyValues = Map.of("key1", "value1", "key2", "value2");
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(asyncCommands.mset(keyValues)).thenReturn(stringFuture);

        CompletableFuture<Void> result = asyncOperations.mset(keyValues);

        assertDoesNotThrow(result::join);
        verify(asyncCommands).mset(keyValues);
    }

    @Test
    void testMsetWithPreference() {
        Map<String, String> keyValues = Map.of("key1", "value1");
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        when(asyncCommands.mset(keyValues)).thenReturn(stringFuture);

        asyncOperations.mset(keyValues, DatacenterPreference.LOCAL_ONLY);

        verify(router).selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
    }

    // ===== Cross-Datacenter Operations Tests =====

    @Test
    void testCrossDatacenterGet() {
        connections.put("us-west-1", connection);
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("remote-value"));
        when(asyncCommands.get("key")).thenReturn(stringFuture);

        CompletableFuture<String> result = asyncOperations.crossDatacenterGet("key", "us-west-1");

        assertEquals("remote-value", result.join());
        verify(metricsCollector).recordRequest(eq("us-west-1"), any(Duration.class), eq(true));
    }

    @Test
    void testCrossDatacenterGetInvalidDatacenter() {
        CompletableFuture<String> result = asyncOperations.crossDatacenterGet("key", "invalid-dc");

        assertTrue(result.isCompletedExceptionally());
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
        when(listKeyValueFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(keyValues));
        when(asyncCommands.mget("key1", "key2")).thenReturn(listKeyValueFuture);

        CompletableFuture<Map<String, String>> result = asyncOperations.crossDatacenterMultiGet(keys, "us-west-1");

        Map<String, String> expected = Map.of("key1", "value1", "key2", "value2");
        assertEquals(expected, result.join());
    }

    // ===== Tombstone Operations Tests =====

    @Test
    void testCreateTombstone() {
        CompletableFuture<Void> result = asyncOperations.createTombstone("key", TombstoneKey.Type.SOFT_DELETE);

        assertDoesNotThrow(result::join);
    }

    @Test
    void testCreateTombstoneWithTtl() {
        CompletableFuture<Void> result = asyncOperations.createTombstone("key", TombstoneKey.Type.CACHE_INVALIDATION, Duration.ofMinutes(5));

        assertDoesNotThrow(result::join);
    }

    @Test
    void testIsTombstoned() {
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("tombstone-data"));
        when(asyncCommands.get(anyString())).thenReturn(stringFuture);

        CompletableFuture<Boolean> result = asyncOperations.isTombstoned("key");

        assertTrue(result.join());
    }

    @Test
    void testGetTombstone() {
        String tombstoneData = "{\"type\":\"SOFT_DELETE\",\"createdAt\":\"2023-01-01T00:00:00Z\",\"ttl\":null}";
        when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(tombstoneData));
        when(asyncCommands.get(anyString())).thenReturn(stringFuture);

        CompletableFuture<TombstoneKey> result = asyncOperations.getTombstone("key");

        TombstoneKey tombstone = result.join();
        assertNotNull(tombstone);
        assertEquals(TombstoneKey.Type.SOFT_DELETE, tombstone.getType());
    }

    @Test
    void testRemoveTombstone() {
        CompletableFuture<Void> result = asyncOperations.removeTombstone("key");

        assertDoesNotThrow(result::join);
    }

    // ===== Distributed Lock Operations Tests =====

    @Test
    void testAcquireLock() {
        lenient().when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        lenient().when(asyncCommands.set(anyString(), anyString(), any())).thenReturn(stringFuture);

        CompletableFuture<Boolean> result = asyncOperations.acquireLock("resource", "owner", Duration.ofMinutes(5));

        assertTrue(result.join());
    }

    @Test
    void testAcquireLockWithPreference() {
        lenient().when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("OK"));
        lenient().when(asyncCommands.set(anyString(), anyString(), any())).thenReturn(stringFuture);

        CompletableFuture<Boolean> result = asyncOperations.acquireLock("resource", "owner", Duration.ofMinutes(5), DatacenterPreference.LOCAL_ONLY);

        assertTrue(result.join());
        verify(router).selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
    }

    @Test
    void testReleaseLock() {
        when(objectFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture(1L));
        when(asyncCommands.eval(anyString(), any(io.lettuce.core.ScriptOutputType.class), any(String[].class), anyString())).thenReturn(objectFuture);

        CompletableFuture<Boolean> result = asyncOperations.releaseLock("resource", "owner");

        assertTrue(result.join());
    }

    @Test
    void testIsLocked() {
        lenient().when(stringFuture.toCompletableFuture()).thenReturn(CompletableFuture.completedFuture("owner-value"));
        lenient().when(asyncCommands.get(anyString())).thenReturn(stringFuture);

        CompletableFuture<Boolean> result = asyncOperations.isLocked("resource");

        assertTrue(result.join());
    }

    // ===== Error Handling Tests =====

    @Test
    void testAsyncOperationException() {
        when(asyncCommands.get("key")).thenThrow(new RuntimeException("Redis error"));

        CompletableFuture<String> result = asyncOperations.get("key");

        assertTrue(result.isCompletedExceptionally());
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(false));
    }

    @Test
    void testMetricsRecordingOnException() {
        CompletableFuture<String> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Operation failed"));
        
        when(stringFuture.toCompletableFuture()).thenReturn(failedFuture);
        when(asyncCommands.get("key")).thenReturn(stringFuture);

        CompletableFuture<String> result = asyncOperations.get("key");

        assertTrue(result.isCompletedExceptionally());
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(false));
    }
}
