package com.redis.multidc.operations;

import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous Redis operations with multi-datacenter support.
 * All operations return CompletableFuture for non-blocking execution.
 */
public interface AsyncOperations {
    
    // String operations
    CompletableFuture<String> get(String key);
    CompletableFuture<String> get(String key, DatacenterPreference preference);
    
    CompletableFuture<Void> set(String key, String value);
    CompletableFuture<Void> set(String key, String value, DatacenterPreference preference);
    CompletableFuture<Void> set(String key, String value, Duration ttl);
    CompletableFuture<Void> set(String key, String value, Duration ttl, DatacenterPreference preference);
    
    CompletableFuture<Boolean> setIfNotExists(String key, String value);
    CompletableFuture<Boolean> setIfNotExists(String key, String value, Duration ttl);
    CompletableFuture<Boolean> setIfNotExists(String key, String value, DatacenterPreference preference);
    
    CompletableFuture<Boolean> delete(String key);
    CompletableFuture<Boolean> delete(String key, DatacenterPreference preference);
    CompletableFuture<Long> delete(String... keys);
    
    CompletableFuture<Boolean> exists(String key);
    CompletableFuture<Boolean> exists(String key, DatacenterPreference preference);
    
    CompletableFuture<Duration> ttl(String key);
    CompletableFuture<Duration> ttl(String key, DatacenterPreference preference);
    
    CompletableFuture<Boolean> expire(String key, Duration ttl);
    CompletableFuture<Boolean> expire(String key, Duration ttl, DatacenterPreference preference);
    
    // Hash operations
    CompletableFuture<String> hget(String key, String field);
    CompletableFuture<String> hget(String key, String field, DatacenterPreference preference);
    
    CompletableFuture<Map<String, String>> hgetAll(String key);
    CompletableFuture<Map<String, String>> hgetAll(String key, DatacenterPreference preference);
    
    CompletableFuture<Void> hset(String key, String field, String value);
    CompletableFuture<Void> hset(String key, String field, String value, DatacenterPreference preference);
    CompletableFuture<Void> hset(String key, Map<String, String> fields);
    CompletableFuture<Void> hset(String key, Map<String, String> fields, DatacenterPreference preference);
    
    CompletableFuture<Boolean> hdel(String key, String field);
    CompletableFuture<Boolean> hdel(String key, String field, DatacenterPreference preference);
    CompletableFuture<Long> hdel(String key, String... fields);
    
    CompletableFuture<Boolean> hexists(String key, String field);
    CompletableFuture<Boolean> hexists(String key, String field, DatacenterPreference preference);
    
    CompletableFuture<Set<String>> hkeys(String key);
    CompletableFuture<Set<String>> hkeys(String key, DatacenterPreference preference);
    
    // List operations
    CompletableFuture<String> lpop(String key);
    CompletableFuture<String> lpop(String key, DatacenterPreference preference);
    
    CompletableFuture<String> rpop(String key);
    CompletableFuture<String> rpop(String key, DatacenterPreference preference);
    
    CompletableFuture<Void> lpush(String key, String... values);
    CompletableFuture<Void> lpush(String key, DatacenterPreference preference, String... values);
    
    CompletableFuture<Void> rpush(String key, String... values);
    CompletableFuture<Void> rpush(String key, DatacenterPreference preference, String... values);
    
    CompletableFuture<List<String>> lrange(String key, long start, long stop);
    CompletableFuture<List<String>> lrange(String key, long start, long stop, DatacenterPreference preference);
    
    CompletableFuture<Long> llen(String key);
    CompletableFuture<Long> llen(String key, DatacenterPreference preference);
    
    // Set operations
    CompletableFuture<Boolean> sadd(String key, String... members);
    CompletableFuture<Boolean> sadd(String key, DatacenterPreference preference, String... members);
    
    CompletableFuture<Boolean> srem(String key, String... members);
    CompletableFuture<Boolean> srem(String key, DatacenterPreference preference, String... members);
    
    CompletableFuture<Set<String>> smembers(String key);
    CompletableFuture<Set<String>> smembers(String key, DatacenterPreference preference);
    
    CompletableFuture<Boolean> sismember(String key, String member);
    CompletableFuture<Boolean> sismember(String key, String member, DatacenterPreference preference);
    
    CompletableFuture<Long> scard(String key);
    CompletableFuture<Long> scard(String key, DatacenterPreference preference);
    
    // Sorted set operations
    CompletableFuture<Boolean> zadd(String key, double score, String member);
    CompletableFuture<Boolean> zadd(String key, double score, String member, DatacenterPreference preference);
    CompletableFuture<Boolean> zadd(String key, Map<String, Double> scoreMembers);
    CompletableFuture<Boolean> zadd(String key, Map<String, Double> scoreMembers, DatacenterPreference preference);
    
    CompletableFuture<Boolean> zrem(String key, String... members);
    CompletableFuture<Boolean> zrem(String key, DatacenterPreference preference, String... members);
    
    CompletableFuture<Set<String>> zrange(String key, long start, long stop);
    CompletableFuture<Set<String>> zrange(String key, long start, long stop, DatacenterPreference preference);
    
    CompletableFuture<Double> zscore(String key, String member);
    CompletableFuture<Double> zscore(String key, String member, DatacenterPreference preference);
    
    CompletableFuture<Long> zcard(String key);
    CompletableFuture<Long> zcard(String key, DatacenterPreference preference);
    
    // Cross-datacenter lookup operations
    CompletableFuture<String> crossDatacenterGet(String key, String datacenterKey);
    CompletableFuture<Map<String, String>> crossDatacenterMultiGet(List<String> keys, String datacenterKey);
    
    // Tombstone key operations
    CompletableFuture<Void> createTombstone(String key, TombstoneKey.Type type);
    CompletableFuture<Void> createTombstone(String key, TombstoneKey.Type type, Duration ttl);
    CompletableFuture<Boolean> isTombstoned(String key);
    CompletableFuture<TombstoneKey> getTombstone(String key);
    CompletableFuture<Void> removeTombstone(String key);
    
    // Distributed lock operations using tombstone keys
    CompletableFuture<Boolean> acquireLock(String lockKey, String lockValue, Duration timeout);
    CompletableFuture<Boolean> acquireLock(String lockKey, String lockValue, Duration timeout, DatacenterPreference preference);
    CompletableFuture<Boolean> releaseLock(String lockKey, String lockValue);
    CompletableFuture<Boolean> releaseLock(String lockKey, String lockValue, DatacenterPreference preference);
    CompletableFuture<Boolean> isLocked(String lockKey);
    
    // Batch operations
    CompletableFuture<List<String>> mget(String... keys);
    CompletableFuture<List<String>> mget(DatacenterPreference preference, String... keys);
    CompletableFuture<Void> mset(Map<String, String> keyValues);
    CompletableFuture<Void> mset(Map<String, String> keyValues, DatacenterPreference preference);
    
    // Ping and health check
    CompletableFuture<String> ping();
    CompletableFuture<String> ping(String datacenterId);
    
    // Utility operations
    CompletableFuture<Void> flushAll();
    CompletableFuture<Void> flushAll(String datacenterId);
    CompletableFuture<Long> dbSize();
    CompletableFuture<Long> dbSize(String datacenterId);
}
