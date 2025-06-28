package com.redis.multidc.operations;

import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Synchronous Redis operations with multi-datacenter support.
 * All operations block until completion and throw exceptions on failure.
 */
public interface SyncOperations {
    
    // String operations
    String get(String key);
    String get(String key, DatacenterPreference preference);
    
    void set(String key, String value);
    void set(String key, String value, DatacenterPreference preference);
    void set(String key, String value, Duration ttl);
    void set(String key, String value, Duration ttl, DatacenterPreference preference);
    
    boolean setIfNotExists(String key, String value);
    boolean setIfNotExists(String key, String value, Duration ttl);
    boolean setIfNotExists(String key, String value, DatacenterPreference preference);
    
    boolean delete(String key);
    boolean delete(String key, DatacenterPreference preference);
    long delete(String... keys);
    
    boolean exists(String key);
    boolean exists(String key, DatacenterPreference preference);
    
    Duration ttl(String key);
    Duration ttl(String key, DatacenterPreference preference);
    
    boolean expire(String key, Duration ttl);
    boolean expire(String key, Duration ttl, DatacenterPreference preference);
    
    // Hash operations
    String hget(String key, String field);
    String hget(String key, String field, DatacenterPreference preference);
    
    Map<String, String> hgetAll(String key);
    Map<String, String> hgetAll(String key, DatacenterPreference preference);
    
    void hset(String key, String field, String value);
    void hset(String key, String field, String value, DatacenterPreference preference);
    void hset(String key, Map<String, String> fields);
    void hset(String key, Map<String, String> fields, DatacenterPreference preference);
    
    boolean hdel(String key, String field);
    boolean hdel(String key, String field, DatacenterPreference preference);
    long hdel(String key, String... fields);
    
    boolean hexists(String key, String field);
    boolean hexists(String key, String field, DatacenterPreference preference);
    
    Set<String> hkeys(String key);
    Set<String> hkeys(String key, DatacenterPreference preference);
    
    // List operations
    String lpop(String key);
    String lpop(String key, DatacenterPreference preference);
    
    String rpop(String key);
    String rpop(String key, DatacenterPreference preference);
    
    void lpush(String key, String... values);
    void lpush(String key, DatacenterPreference preference, String... values);
    
    void rpush(String key, String... values);
    void rpush(String key, DatacenterPreference preference, String... values);
    
    List<String> lrange(String key, long start, long stop);
    List<String> lrange(String key, long start, long stop, DatacenterPreference preference);
    
    long llen(String key);
    long llen(String key, DatacenterPreference preference);
    
    // Set operations
    boolean sadd(String key, String... members);
    boolean sadd(String key, DatacenterPreference preference, String... members);
    
    boolean srem(String key, String... members);
    boolean srem(String key, DatacenterPreference preference, String... members);
    
    Set<String> smembers(String key);
    Set<String> smembers(String key, DatacenterPreference preference);
    
    boolean sismember(String key, String member);
    boolean sismember(String key, String member, DatacenterPreference preference);
    
    long scard(String key);
    long scard(String key, DatacenterPreference preference);
    
    // Sorted set operations
    boolean zadd(String key, double score, String member);
    boolean zadd(String key, double score, String member, DatacenterPreference preference);
    boolean zadd(String key, Map<String, Double> scoreMembers);
    boolean zadd(String key, Map<String, Double> scoreMembers, DatacenterPreference preference);
    
    boolean zrem(String key, String... members);
    boolean zrem(String key, DatacenterPreference preference, String... members);
    
    Set<String> zrange(String key, long start, long stop);
    Set<String> zrange(String key, long start, long stop, DatacenterPreference preference);
    
    Double zscore(String key, String member);
    Double zscore(String key, String member, DatacenterPreference preference);
    
    long zcard(String key);
    long zcard(String key, DatacenterPreference preference);
    
    // Cross-datacenter lookup operations
    String crossDatacenterGet(String key, String datacenterKey);
    Map<String, String> crossDatacenterMultiGet(List<String> keys, String datacenterKey);
    
    // Tombstone key operations
    void createTombstone(String key, TombstoneKey.Type type);
    void createTombstone(String key, TombstoneKey.Type type, Duration ttl);
    boolean isTombstoned(String key);
    TombstoneKey getTombstone(String key);
    void removeTombstone(String key);
    
    // Distributed lock operations using tombstone keys
    boolean acquireLock(String lockKey, String lockValue, Duration timeout);
    boolean acquireLock(String lockKey, String lockValue, Duration timeout, DatacenterPreference preference);
    boolean releaseLock(String lockKey, String lockValue);
    boolean releaseLock(String lockKey, String lockValue, DatacenterPreference preference);
    boolean isLocked(String lockKey);
    
    // Batch operations
    List<String> mget(String... keys);
    List<String> mget(DatacenterPreference preference, String... keys);
    void mset(Map<String, String> keyValues);
    void mset(Map<String, String> keyValues, DatacenterPreference preference);
    
    // Ping and health check
    String ping();
    String ping(String datacenterId);
    
    // Utility operations
    void flushAll();
    void flushAll(String datacenterId);
    long dbSize();
    long dbSize(String datacenterId);
}
