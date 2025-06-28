package com.redis.multidc.operations;

import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reactive Redis operations with multi-datacenter support.
 * All operations return Mono or Flux for reactive stream processing.
 */
public interface ReactiveOperations {
    
    // String operations
    Mono<String> get(String key);
    Mono<String> get(String key, DatacenterPreference preference);
    
    Mono<Void> set(String key, String value);
    Mono<Void> set(String key, String value, DatacenterPreference preference);
    Mono<Void> set(String key, String value, Duration ttl);
    Mono<Void> set(String key, String value, Duration ttl, DatacenterPreference preference);
    
    Mono<Boolean> setIfNotExists(String key, String value);
    Mono<Boolean> setIfNotExists(String key, String value, Duration ttl);
    Mono<Boolean> setIfNotExists(String key, String value, DatacenterPreference preference);
    
    Mono<Boolean> delete(String key);
    Mono<Boolean> delete(String key, DatacenterPreference preference);
    Mono<Long> delete(String... keys);
    
    Mono<Boolean> exists(String key);
    Mono<Boolean> exists(String key, DatacenterPreference preference);
    
    Mono<Duration> ttl(String key);
    Mono<Duration> ttl(String key, DatacenterPreference preference);
    
    Mono<Boolean> expire(String key, Duration ttl);
    Mono<Boolean> expire(String key, Duration ttl, DatacenterPreference preference);
    
    // Hash operations
    Mono<String> hget(String key, String field);
    Mono<String> hget(String key, String field, DatacenterPreference preference);
    
    Mono<Map<String, String>> hgetAll(String key);
    Mono<Map<String, String>> hgetAll(String key, DatacenterPreference preference);
    
    Mono<Void> hset(String key, String field, String value);
    Mono<Void> hset(String key, String field, String value, DatacenterPreference preference);
    Mono<Void> hset(String key, Map<String, String> fields);
    Mono<Void> hset(String key, Map<String, String> fields, DatacenterPreference preference);
    
    Mono<Boolean> hdel(String key, String field);
    Mono<Boolean> hdel(String key, String field, DatacenterPreference preference);
    Mono<Long> hdel(String key, String... fields);
    
    Mono<Boolean> hexists(String key, String field);
    Mono<Boolean> hexists(String key, String field, DatacenterPreference preference);
    
    Mono<Set<String>> hkeys(String key);
    Mono<Set<String>> hkeys(String key, DatacenterPreference preference);
    
    // List operations
    Mono<String> lpop(String key);
    Mono<String> lpop(String key, DatacenterPreference preference);
    
    Mono<String> rpop(String key);
    Mono<String> rpop(String key, DatacenterPreference preference);
    
    Mono<Void> lpush(String key, String... values);
    Mono<Void> lpush(String key, DatacenterPreference preference, String... values);
    
    Mono<Void> rpush(String key, String... values);
    Mono<Void> rpush(String key, DatacenterPreference preference, String... values);
    
    Flux<String> lrange(String key, long start, long stop);
    Flux<String> lrange(String key, long start, long stop, DatacenterPreference preference);
    
    Mono<Long> llen(String key);
    Mono<Long> llen(String key, DatacenterPreference preference);
    
    // Set operations
    Mono<Boolean> sadd(String key, String... members);
    Mono<Boolean> sadd(String key, DatacenterPreference preference, String... members);
    
    Mono<Boolean> srem(String key, String... members);
    Mono<Boolean> srem(String key, DatacenterPreference preference, String... members);
    
    Flux<String> smembers(String key);
    Flux<String> smembers(String key, DatacenterPreference preference);
    
    Mono<Boolean> sismember(String key, String member);
    Mono<Boolean> sismember(String key, String member, DatacenterPreference preference);
    
    Mono<Long> scard(String key);
    Mono<Long> scard(String key, DatacenterPreference preference);
    
    // Sorted set operations
    Mono<Boolean> zadd(String key, double score, String member);
    Mono<Boolean> zadd(String key, double score, String member, DatacenterPreference preference);
    Mono<Boolean> zadd(String key, Map<String, Double> scoreMembers);
    Mono<Boolean> zadd(String key, Map<String, Double> scoreMembers, DatacenterPreference preference);
    
    Mono<Boolean> zrem(String key, String... members);
    Mono<Boolean> zrem(String key, DatacenterPreference preference, String... members);
    
    Flux<String> zrange(String key, long start, long stop);
    Flux<String> zrange(String key, long start, long stop, DatacenterPreference preference);
    
    Mono<Double> zscore(String key, String member);
    Mono<Double> zscore(String key, String member, DatacenterPreference preference);
    
    Mono<Long> zcard(String key);
    Mono<Long> zcard(String key, DatacenterPreference preference);
    
    // Cross-datacenter lookup operations
    Mono<String> crossDatacenterGet(String key, String datacenterKey);
    Mono<Map<String, String>> crossDatacenterMultiGet(List<String> keys, String datacenterKey);
    
    // Tombstone key operations
    Mono<Void> createTombstone(String key, TombstoneKey.Type type);
    Mono<Void> createTombstone(String key, TombstoneKey.Type type, Duration ttl);
    Mono<Boolean> isTombstoned(String key);
    Mono<TombstoneKey> getTombstone(String key);
    Mono<Void> removeTombstone(String key);
    
    // Distributed lock operations using tombstone keys
    Mono<Boolean> acquireLock(String lockKey, String lockValue, Duration timeout);
    Mono<Boolean> acquireLock(String lockKey, String lockValue, Duration timeout, DatacenterPreference preference);
    Mono<Boolean> releaseLock(String lockKey, String lockValue);
    Mono<Boolean> releaseLock(String lockKey, String lockValue, DatacenterPreference preference);
    Mono<Boolean> isLocked(String lockKey);
    
    // Batch operations
    Flux<String> mget(String... keys);
    Flux<String> mget(DatacenterPreference preference, String... keys);
    Mono<Void> mset(Map<String, String> keyValues);
    Mono<Void> mset(Map<String, String> keyValues, DatacenterPreference preference);
    
    // Streaming operations
    Flux<String> scan(String pattern);
    Flux<String> scan(String pattern, DatacenterPreference preference);
    Flux<Map.Entry<String, String>> hscan(String key, String pattern);
    Flux<Map.Entry<String, String>> hscan(String key, String pattern, DatacenterPreference preference);
    
    // Ping and health check
    Mono<String> ping();
    Mono<String> ping(String datacenterId);
    
    // Utility operations
    Mono<Void> flushAll();
    Mono<Void> flushAll(String datacenterId);
    Mono<Long> dbSize();
    Mono<Long> dbSize(String datacenterId);
}
