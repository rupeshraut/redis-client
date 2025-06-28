package com.redis.multidc.impl;

import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import com.redis.multidc.observability.MetricsCollector;
import com.redis.multidc.operations.SyncOperations;
import com.redis.multidc.routing.DatacenterRouter;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Synchronous operations implementation for multi-datacenter Redis client.
 */
public class SyncOperationsImpl implements SyncOperations {
    
    private static final Logger logger = LoggerFactory.getLogger(SyncOperationsImpl.class);
    
    private final DatacenterRouter router;
    private final Map<String, StatefulRedisConnection<String, String>> connections;
    private final MetricsCollector metricsCollector;
    
    public SyncOperationsImpl(DatacenterRouter router, 
                             Map<String, StatefulRedisConnection<String, String>> connections,
                             MetricsCollector metricsCollector) {
        this.router = router;
        this.connections = connections;
        this.metricsCollector = metricsCollector;
    }
    
    @Override
    public String get(String key) {
        return get(key, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public String get(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "GET", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.get(key);
        });
    }
    
    @Override
    public void set(String key, String value) {
        set(key, value, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public void set(String key, String value, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        executeWithMetrics(datacenterId.get(), "SET", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            commands.set(key, value);
            return null;
        });
    }
    
    @Override
    public void set(String key, String value, Duration ttl) {
        set(key, value, ttl, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public void set(String key, String value, Duration ttl, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        executeWithMetrics(datacenterId.get(), "SETEX", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            commands.setex(key, ttl.getSeconds(), value);
            return null;
        });
    }
    
    @Override
    public boolean setIfNotExists(String key, String value) {
        return setIfNotExists(key, value, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public boolean setIfNotExists(String key, String value, Duration ttl) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "SET_NX_EX", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            // Use SET command with NX and EX options for atomic operation
            String result = commands.set(key, value, io.lettuce.core.SetArgs.Builder.nx().ex(ttl.getSeconds()));
            return "OK".equals(result);
        });
    }
    
    @Override
    public boolean setIfNotExists(String key, String value, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "SETNX", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.setnx(key, value);
        });
    }
    
    @Override
    public boolean delete(String key) {
        return delete(key, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public boolean delete(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "DEL", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.del(key) > 0;
        });
    }
    
    @Override
    public long delete(String... keys) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "DEL", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.del(keys);
        });
    }
    
    @Override
    public boolean exists(String key) {
        return exists(key, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public boolean exists(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "EXISTS", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.exists(key) > 0;
        });
    }
    
    @Override
    public Duration ttl(String key) {
        return ttl(key, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public Duration ttl(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "TTL", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            long ttlSeconds = commands.ttl(key);
            return ttlSeconds > 0 ? Duration.ofSeconds(ttlSeconds) : null;
        });
    }
    
    @Override
    public boolean expire(String key, Duration ttl) {
        return expire(key, ttl, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public boolean expire(String key, Duration ttl, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "EXPIRE", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.expire(key, ttl.getSeconds());
        });
    }
    
    // Hash operations - implementing a few key ones
    @Override
    public String hget(String key, String field) {
        return hget(key, field, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public String hget(String key, String field, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "HGET", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.hget(key, field);
        });
    }
    
    @Override
    public Map<String, String> hgetAll(String key) {
        return hgetAll(key, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public Map<String, String> hgetAll(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "HGETALL", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.hgetall(key);
        });
    }
    
    // Helper method to execute operations with metrics
    private <T> T executeWithMetrics(String datacenterId, String operation, OperationSupplier<T> supplier) {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        
        try {
            T result = supplier.get();
            success = true;
            return result;
        } catch (Exception e) {
            logger.error("Operation {} failed for datacenter {}: {}", operation, datacenterId, e.getMessage());
            throw new RuntimeException("Redis operation failed", e);
        } finally {
            Duration latency = Duration.ofMillis(System.currentTimeMillis() - startTime);
            metricsCollector.recordRequest(datacenterId, latency, success);
        }
    }
    
    private RedisCommands<String, String> getCommands(String datacenterId) {
        StatefulRedisConnection<String, String> connection = connections.get(datacenterId);
        if (connection == null) {
            throw new RuntimeException("No connection available for datacenter: " + datacenterId);
        }
        return connection.sync();
    }
    
    @FunctionalInterface
    private interface OperationSupplier<T> {
        T get() throws Exception;
    }
    
    // Hash operations - implementing remaining ones
    @Override
    public void hset(String key, String field, String value) {
        hset(key, field, value, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public void hset(String key, String field, String value, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        executeWithMetrics(datacenterId.get(), "HSET", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            commands.hset(key, field, value);
            return null;
        });
    }

    @Override
    public void hset(String key, Map<String, String> fields) {
        hset(key, fields, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public void hset(String key, Map<String, String> fields, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        executeWithMetrics(datacenterId.get(), "HMSET", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            commands.hmset(key, fields);
            return null;
        });
    }

    @Override
    public boolean hdel(String key, String field) {
        return hdel(key, field, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public boolean hdel(String key, String field, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "HDEL", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.hdel(key, field) > 0;
        });
    }

    @Override
    public long hdel(String key, String... fields) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "HDEL", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.hdel(key, fields);
        });
    }

    @Override
    public boolean hexists(String key, String field) {
        return hexists(key, field, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public boolean hexists(String key, String field, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "HEXISTS", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.hexists(key, field);
        });
    }

    @Override
    public Set<String> hkeys(String key) {
        return hkeys(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Set<String> hkeys(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "HKEYS", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            List<String> keys = commands.hkeys(key);
            return Set.copyOf(keys);
        });
    }
    // List operations
    @Override
    public String lpop(String key) { 
        return lpop(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public String lpop(String key, DatacenterPreference preference) { 
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "LPOP", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.lpop(key);
        });
    }

    @Override
    public String rpop(String key) { 
        return rpop(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public String rpop(String key, DatacenterPreference preference) { 
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "RPOP", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.rpop(key);
        });
    }

    @Override
    public void lpush(String key, String... values) { 
        lpush(key, DatacenterPreference.LOCAL_PREFERRED, values);
    }

    @Override
    public void lpush(String key, DatacenterPreference preference, String... values) { 
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        executeWithMetrics(datacenterId.get(), "LPUSH", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            commands.lpush(key, values);
            return null;
        });
    }

    @Override
    public void rpush(String key, String... values) { 
        rpush(key, DatacenterPreference.LOCAL_PREFERRED, values);
    }

    @Override
    public void rpush(String key, DatacenterPreference preference, String... values) { 
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        executeWithMetrics(datacenterId.get(), "RPUSH", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            commands.rpush(key, values);
            return null;
        });
    }

    @Override
    public List<String> lrange(String key, long start, long stop) { 
        return lrange(key, start, stop, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public List<String> lrange(String key, long start, long stop, DatacenterPreference preference) { 
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "LRANGE", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.lrange(key, start, stop);
        });
    }

    @Override
    public long llen(String key) { 
        return llen(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public long llen(String key, DatacenterPreference preference) { 
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "LLEN", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.llen(key);
        });
    }
    
    // Set operations
    @Override
    public boolean sadd(String key, String... members) {
        return sadd(key, DatacenterPreference.LOCAL_PREFERRED, members);
    }

    @Override
    public boolean sadd(String key, DatacenterPreference preference, String... members) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "SADD", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.sadd(key, members) > 0;
        });
    }

    @Override
    public boolean srem(String key, String... members) {
        return srem(key, DatacenterPreference.LOCAL_PREFERRED, members);
    }

    @Override
    public boolean srem(String key, DatacenterPreference preference, String... members) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "SREM", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.srem(key, members) > 0;
        });
    }

    @Override
    public Set<String> smembers(String key) {
        return smembers(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Set<String> smembers(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "SMEMBERS", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.smembers(key);
        });
    }

    @Override
    public boolean sismember(String key, String member) {
        return sismember(key, member, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public boolean sismember(String key, String member, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "SISMEMBER", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.sismember(key, member);
        });
    }

    @Override
    public long scard(String key) {
        return scard(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public long scard(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "SCARD", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.scard(key);
        });
    }
    
    // Sorted set operations
    @Override
    public boolean zadd(String key, double score, String member) {
        return zadd(key, score, member, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public boolean zadd(String key, double score, String member, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "ZADD", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.zadd(key, score, member) > 0;
        });
    }

    @Override
    public boolean zadd(String key, Map<String, Double> scoreMembers) {
        return zadd(key, scoreMembers, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public boolean zadd(String key, Map<String, Double> scoreMembers, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "ZADD", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            // Convert to Lettuce format
            Object[] args = new Object[scoreMembers.size() * 2];
            int i = 0;
            for (Map.Entry<String, Double> entry : scoreMembers.entrySet()) {
                args[i++] = entry.getValue();
                args[i++] = entry.getKey();
            }
            return commands.zadd(key, args) > 0;
        });
    }

    @Override
    public boolean zrem(String key, String... members) {
        return zrem(key, DatacenterPreference.LOCAL_PREFERRED, members);
    }

    @Override
    public boolean zrem(String key, DatacenterPreference preference, String... members) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "ZREM", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.zrem(key, members) > 0;
        });
    }

    @Override
    public Set<String> zrange(String key, long start, long stop) {
        return zrange(key, start, stop, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Set<String> zrange(String key, long start, long stop, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "ZRANGE", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            List<String> result = commands.zrange(key, start, stop);
            return Set.copyOf(result);
        });
    }

    @Override
    public Double zscore(String key, String member) {
        return zscore(key, member, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Double zscore(String key, String member, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "ZSCORE", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.zscore(key, member);
        });
    }

    @Override
    public long zcard(String key) {
        return zcard(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public long zcard(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "ZCARD", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.zcard(key);
        });
    }
    
    // Cross-datacenter operations
    @Override
    public String crossDatacenterGet(String key, String datacenterKey) {
        return executeWithMetrics(datacenterKey, "CROSS_GET", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterKey);
            return commands.get(key);
        });
    }

    @Override
    public Map<String, String> crossDatacenterMultiGet(List<String> keys, String datacenterKey) {
        return executeWithMetrics(datacenterKey, "CROSS_MGET", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterKey);
            List<io.lettuce.core.KeyValue<String, String>> keyValues = commands.mget(keys.toArray(new String[0]));
            Map<String, String> result = new java.util.HashMap<>();
            for (int i = 0; i < keys.size() && i < keyValues.size(); i++) {
                var keyValue = keyValues.get(i);
                if (keyValue != null && keyValue.hasValue()) {
                    result.put(keys.get(i), keyValue.getValue());
                }
            }
            return result;
        });
    }

    // Tombstone operations - simplified implementations
    @Override
    public void createTombstone(String key, TombstoneKey.Type type) {
        // Simplified implementation - could be enhanced with actual tombstone logic
        logger.info("Creating tombstone for key: {} with type: {}", key, type);
    }

    @Override
    public void createTombstone(String key, TombstoneKey.Type type, Duration ttl) {
        // Simplified implementation - could be enhanced with actual tombstone logic
        logger.info("Creating tombstone for key: {} with type: {} and ttl: {}", key, type, ttl);
    }

    @Override
    public boolean isTombstoned(String key) {
        String data = get(key);
        return data != null;
    }

    @Override
    public TombstoneKey getTombstone(String key) {
        String data = get(key);
        if (data == null) {
            return null;
        }
        try {
            // Parse the JSON data to create TombstoneKey
            // For now, create a simple SOFT_DELETE tombstone with current time
            return new TombstoneKey(
                key,
                TombstoneKey.Type.SOFT_DELETE,
                Instant.now(),
                null, // no expiration
                "us-east-1", // default datacenter
                "Retrieved tombstone",
                Map.of()
            );
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void removeTombstone(String key) {
        // Simplified implementation - could remove tombstone marker
        logger.info("Removing tombstone for key: {}", key);
    }

    // Distributed lock operations
    @Override
    public boolean acquireLock(String lockKey, String lockValue, Duration timeout) {
        return acquireLock(lockKey, lockValue, timeout, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public boolean acquireLock(String lockKey, String lockValue, Duration timeout, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return false;
        }
        
        return executeWithMetrics(datacenterId.get(), "SET_NX_EX", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            // Use SET command with NX and EX options for atomic lock acquisition
            String result = commands.set(lockKey, lockValue, io.lettuce.core.SetArgs.Builder.nx().ex(timeout.getSeconds()));
            return "OK".equals(result);
        });
    }

    @Override
    public boolean releaseLock(String lockKey, String lockValue) {
        return releaseLock(lockKey, lockValue, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public boolean releaseLock(String lockKey, String lockValue, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return false;
        }
        
        StatefulRedisConnection<String, String> connection = connections.get(datacenterId.get());
        if (connection == null) {
            return false;
        }
        
        RedisCommands<String, String> syncCommands = connection.sync();
        
        // Lua script to atomically check value and delete if it matches
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        
        try {
            Object result = syncCommands.eval(script, io.lettuce.core.ScriptOutputType.INTEGER, new String[]{lockKey}, lockValue);
            long deleteCount = (Long) result;
            return deleteCount > 0;
        } catch (Exception e) {
            logger.warn("Failed to release lock for key: {}", lockKey, e);
            return false;
        }
    }

    @Override
    public boolean isLocked(String lockKey) {
        return exists(lockKey);
    }

    // Batch operations
    @Override
    public List<String> mget(String... keys) {
        return mget(DatacenterPreference.LOCAL_PREFERRED, keys);
    }

    @Override
    public List<String> mget(DatacenterPreference preference, String... keys) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for read operation");
        }
        
        return executeWithMetrics(datacenterId.get(), "MGET", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            List<io.lettuce.core.KeyValue<String, String>> keyValues = commands.mget(keys);
            return keyValues.stream()
                .map(kv -> kv != null && kv.hasValue() ? kv.getValue() : null)
                .collect(java.util.stream.Collectors.toList());
        });
    }

    @Override
    public void mset(Map<String, String> keyValues) {
        mset(keyValues, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public void mset(Map<String, String> keyValues, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            throw new RuntimeException("No available datacenter for write operation");
        }
        
        executeWithMetrics(datacenterId.get(), "MSET", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            commands.mset(keyValues);
            return null;
        });
    }

    // Utility operations
    @Override
    public String ping() {
        Optional<String> datacenterId = router.selectDatacenterForRead(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            return "No datacenter available";
        }
        
        return executeWithMetrics(datacenterId.get(), "PING", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId.get());
            return commands.ping();
        });
    }

    @Override
    public String ping(String datacenterId) {
        return executeWithMetrics(datacenterId, "PING", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId);
            return commands.ping();
        });
    }

    @Override
    public void flushAll() {
        // Execute on all datacenters
        for (String datacenterId : connections.keySet()) {
            flushAll(datacenterId);
        }
    }

    @Override
    public void flushAll(String datacenterId) {
        executeWithMetrics(datacenterId, "FLUSHALL", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId);
            commands.flushall();
            return null;
        });
    }

    @Override
    public long dbSize() {
        Optional<String> datacenterId = router.selectDatacenterForRead(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            return 0L;
        }
        
        return dbSize(datacenterId.get());
    }

    @Override
    public long dbSize(String datacenterId) {
        return executeWithMetrics(datacenterId, "DBSIZE", () -> {
            RedisCommands<String, String> commands = getCommands(datacenterId);
            return commands.dbsize();
        });
    }
}
