package com.redis.multidc.impl;

import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import com.redis.multidc.observability.MetricsCollector;
import com.redis.multidc.operations.AsyncOperations;
import com.redis.multidc.routing.DatacenterRouter;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous operations implementation for multi-datacenter Redis client.
 */
public class AsyncOperationsImpl implements AsyncOperations {
    
    private static final Logger logger = LoggerFactory.getLogger(AsyncOperationsImpl.class);
    
    private final DatacenterRouter router;
    private final Map<String, StatefulRedisConnection<String, String>> connections;
    private final MetricsCollector metricsCollector;
    
    public AsyncOperationsImpl(DatacenterRouter router, 
                              Map<String, StatefulRedisConnection<String, String>> connections,
                              MetricsCollector metricsCollector) {
        this.router = router;
        this.connections = connections;
        this.metricsCollector = metricsCollector;
    }
    
    @Override
    public CompletableFuture<String> get(String key) {
        return get(key, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public CompletableFuture<String> get(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "GET", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.get(key).toCompletableFuture();
        });
    }
    
    @Override
    public CompletableFuture<Void> set(String key, String value) {
        return set(key, value, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public CompletableFuture<Void> set(String key, String value, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "SET", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.set(key, value).toCompletableFuture().thenApply(result -> null);
        });
    }
    
    @Override
    public CompletableFuture<Void> set(String key, String value, Duration ttl) {
        return set(key, value, ttl, DatacenterPreference.LOCAL_PREFERRED);
    }
    
    @Override
    public CompletableFuture<Void> set(String key, String value, Duration ttl, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "SETEX", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.setex(key, ttl.getSeconds(), value).toCompletableFuture().thenApply(result -> null);
        });
    }

    // Helper method to execute operations with metrics
    private <T> CompletableFuture<T> executeWithMetrics(String datacenterId, String operation, 
                                                       AsyncOperationSupplier<T> supplier) {
        long startTime = System.currentTimeMillis();
        
        try {
            return supplier.get()
                .whenComplete((result, throwable) -> {
                    Duration latency = Duration.ofMillis(System.currentTimeMillis() - startTime);
                    boolean success = throwable == null;
                    metricsCollector.recordRequest(datacenterId, latency, success);
                    
                    if (throwable != null) {
                        logger.error("Async operation {} failed for datacenter {}: {}", 
                            operation, datacenterId, throwable.getMessage());
                    }
                });
        } catch (Exception e) {
            Duration latency = Duration.ofMillis(System.currentTimeMillis() - startTime);
            metricsCollector.recordRequest(datacenterId, latency, false);
            logger.error("Async operation {} failed for datacenter {}: {}", operation, datacenterId, e.getMessage());
            return CompletableFuture.failedFuture(new RuntimeException("Redis async operation failed", e));
        }
    }
    
    private RedisAsyncCommands<String, String> getAsyncCommands(String datacenterId) {
        StatefulRedisConnection<String, String> connection = connections.get(datacenterId);
        if (connection == null) {
            throw new RuntimeException("No connection available for datacenter: " + datacenterId);
        }
        return connection.async();
    }
    
    @FunctionalInterface
    private interface AsyncOperationSupplier<T> {
        CompletableFuture<T> get() throws Exception;
    }
    
    // String operations
    @Override
    public CompletableFuture<Boolean> setIfNotExists(String key, String value) {
        return setIfNotExists(key, value, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> setIfNotExists(String key, String value, Duration ttl) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "SET_NX_EX", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            // Use SET command with NX and EX options for atomic operation
            return commands.set(key, value, io.lettuce.core.SetArgs.Builder.nx().ex(ttl.getSeconds()))
                .toCompletableFuture()
                .thenApply(result -> "OK".equals(result));
        });
    }

    @Override
    public CompletableFuture<Boolean> setIfNotExists(String key, String value, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "SETNX", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.setnx(key, value).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Boolean> delete(String key) {
        return delete(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> delete(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "DEL", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.del(key).toCompletableFuture().thenApply(count -> count > 0);
        });
    }

    @Override
    public CompletableFuture<Long> delete(String... keys) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "DEL", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.del(keys).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Boolean> exists(String key) {
        return exists(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> exists(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "EXISTS", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.exists(key).toCompletableFuture().thenApply(count -> count > 0);
        });
    }

    @Override
    public CompletableFuture<Duration> ttl(String key) {
        return ttl(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Duration> ttl(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "TTL", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.ttl(key).toCompletableFuture().thenApply(seconds -> Duration.ofSeconds(seconds));
        });
    }

    @Override
    public CompletableFuture<Boolean> expire(String key, Duration ttl) {
        return expire(key, ttl, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> expire(String key, Duration ttl, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "EXPIRE", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.expire(key, ttl.getSeconds()).toCompletableFuture();
        });
    }
    // Hash operations
    @Override
    public CompletableFuture<String> hget(String key, String field) {
        return hget(key, field, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<String> hget(String key, String field, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "HGET", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.hget(key, field).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Map<String, String>> hgetAll(String key) {
        return hgetAll(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Map<String, String>> hgetAll(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "HGETALL", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.hgetall(key).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Void> hset(String key, String field, String value) {
        return hset(key, field, value, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Void> hset(String key, String field, String value, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "HSET", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.hset(key, field, value).toCompletableFuture().thenApply(result -> null);
        });
    }

    @Override
    public CompletableFuture<Void> hset(String key, Map<String, String> fields) {
        return hset(key, fields, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Void> hset(String key, Map<String, String> fields, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "HMSET", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.hmset(key, fields).toCompletableFuture().thenApply(result -> null);
        });
    }

    @Override
    public CompletableFuture<Boolean> hdel(String key, String field) {
        return hdel(key, field, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> hdel(String key, String field, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "HDEL", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.hdel(key, field).toCompletableFuture().thenApply(count -> count > 0);
        });
    }

    @Override
    public CompletableFuture<Long> hdel(String key, String... fields) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "HDEL", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.hdel(key, fields).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Boolean> hexists(String key, String field) {
        return hexists(key, field, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> hexists(String key, String field, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "HEXISTS", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.hexists(key, field).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Set<String>> hkeys(String key) {
        return hkeys(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Set<String>> hkeys(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "HKEYS", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.hkeys(key).toCompletableFuture().thenApply(list -> Set.copyOf(list));
        });
    }
    // List operations
    @Override
    public CompletableFuture<String> lpop(String key) {
        return lpop(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<String> lpop(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "LPOP", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.lpop(key).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<String> rpop(String key) {
        return rpop(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<String> rpop(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "RPOP", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.rpop(key).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Void> lpush(String key, String... values) {
        return lpush(key, DatacenterPreference.LOCAL_PREFERRED, values);
    }

    @Override
    public CompletableFuture<Void> lpush(String key, DatacenterPreference preference, String... values) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "LPUSH", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.lpush(key, values).toCompletableFuture().thenApply(result -> null);
        });
    }

    @Override
    public CompletableFuture<Void> rpush(String key, String... values) {
        return rpush(key, DatacenterPreference.LOCAL_PREFERRED, values);
    }

    @Override
    public CompletableFuture<Void> rpush(String key, DatacenterPreference preference, String... values) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "RPUSH", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.rpush(key, values).toCompletableFuture().thenApply(result -> null);
        });
    }

    @Override
    public CompletableFuture<List<String>> lrange(String key, long start, long stop) {
        return lrange(key, start, stop, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<List<String>> lrange(String key, long start, long stop, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "LRANGE", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.lrange(key, start, stop).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Long> llen(String key) {
        return llen(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Long> llen(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "LLEN", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.llen(key).toCompletableFuture();
        });
    }
    // Set operations
    @Override
    public CompletableFuture<Boolean> sadd(String key, String... members) {
        return sadd(key, DatacenterPreference.LOCAL_PREFERRED, members);
    }

    @Override
    public CompletableFuture<Boolean> sadd(String key, DatacenterPreference preference, String... members) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "SADD", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.sadd(key, members).toCompletableFuture().thenApply(count -> count > 0);
        });
    }

    @Override
    public CompletableFuture<Boolean> srem(String key, String... members) {
        return srem(key, DatacenterPreference.LOCAL_PREFERRED, members);
    }

    @Override
    public CompletableFuture<Boolean> srem(String key, DatacenterPreference preference, String... members) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "SREM", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.srem(key, members).toCompletableFuture().thenApply(count -> count > 0);
        });
    }

    @Override
    public CompletableFuture<Set<String>> smembers(String key) {
        return smembers(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Set<String>> smembers(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "SMEMBERS", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.smembers(key).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Boolean> sismember(String key, String member) {
        return sismember(key, member, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> sismember(String key, String member, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "SISMEMBER", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.sismember(key, member).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Long> scard(String key) {
        return scard(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Long> scard(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "SCARD", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.scard(key).toCompletableFuture();
        });
    }

    // Sorted set operations
    @Override
    public CompletableFuture<Boolean> zadd(String key, double score, String member) {
        return zadd(key, score, member, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> zadd(String key, double score, String member, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "ZADD", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.zadd(key, score, member).toCompletableFuture().thenApply(count -> count > 0);
        });
    }

    @Override
    public CompletableFuture<Boolean> zadd(String key, Map<String, Double> scoreMembers) {
        return zadd(key, scoreMembers, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> zadd(String key, Map<String, Double> scoreMembers, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "ZADD", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            // Convert to Lettuce format
            Object[] args = new Object[scoreMembers.size() * 2];
            int i = 0;
            for (Map.Entry<String, Double> entry : scoreMembers.entrySet()) {
                args[i++] = entry.getValue();
                args[i++] = entry.getKey();
            }
            return commands.zadd(key, args).toCompletableFuture().thenApply(count -> count > 0);
        });
    }

    @Override
    public CompletableFuture<Boolean> zrem(String key, String... members) {
        return zrem(key, DatacenterPreference.LOCAL_PREFERRED, members);
    }

    @Override
    public CompletableFuture<Boolean> zrem(String key, DatacenterPreference preference, String... members) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "ZREM", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.zrem(key, members).toCompletableFuture().thenApply(count -> count > 0);
        });
    }

    @Override
    public CompletableFuture<Set<String>> zrange(String key, long start, long stop) {
        return zrange(key, start, stop, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Set<String>> zrange(String key, long start, long stop, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "ZRANGE", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.zrange(key, start, stop).toCompletableFuture().thenApply(list -> Set.copyOf(list));
        });
    }

    @Override
    public CompletableFuture<Double> zscore(String key, String member) {
        return zscore(key, member, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Double> zscore(String key, String member, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "ZSCORE", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.zscore(key, member).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Long> zcard(String key) {
        return zcard(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Long> zcard(String key, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "ZCARD", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.zcard(key).toCompletableFuture();
        });
    }
    // Cross-datacenter operations
    @Override
    public CompletableFuture<String> crossDatacenterGet(String key, String datacenterKey) {
        return executeWithMetrics(datacenterKey, "CROSS_GET", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterKey);
            return commands.get(key).toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Map<String, String>> crossDatacenterMultiGet(List<String> keys, String datacenterKey) {
        return executeWithMetrics(datacenterKey, "CROSS_MGET", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterKey);
            return commands.mget(keys.toArray(new String[0])).toCompletableFuture()
                .thenApply(keyValues -> {
                    Map<String, String> result = new java.util.HashMap<>();
                    for (int i = 0; i < keys.size() && i < keyValues.size(); i++) {
                        var keyValue = keyValues.get(i);
                        if (keyValue != null && keyValue.hasValue()) {
                            result.put(keys.get(i), keyValue.getValue());
                        }
                    }
                    return result;
                });
        });
    }

    // Tombstone operations - simplified implementations
    @Override
    public CompletableFuture<Void> createTombstone(String key, TombstoneKey.Type type) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createTombstone(String key, TombstoneKey.Type type, Duration ttl) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> isTombstoned(String key) {
        return get(key).thenApply(data -> data != null);
    }

    @Override
    public CompletableFuture<TombstoneKey> getTombstone(String key) {
        return get(key).thenApply(data -> {
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
        });
    }

    @Override
    public CompletableFuture<Void> removeTombstone(String key) {
        return CompletableFuture.completedFuture(null);
    }

    // Distributed lock operations
    @Override
    public CompletableFuture<Boolean> acquireLock(String lockKey, String lockValue, Duration timeout) {
        return acquireLock(lockKey, lockValue, timeout, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> acquireLock(String lockKey, String lockValue, Duration timeout, DatacenterPreference preference) {
        return set(lockKey, lockValue, timeout, preference)
            .thenApply(v -> true)
            .exceptionally(ex -> false);
    }

    @Override
    public CompletableFuture<Boolean> releaseLock(String lockKey, String lockValue) {
        return releaseLock(lockKey, lockValue, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Boolean> releaseLock(String lockKey, String lockValue, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        
        StatefulRedisConnection<String, String> connection = connections.get(datacenterId.get());
        if (connection == null) {
            return CompletableFuture.completedFuture(false);
        }
        
        RedisAsyncCommands<String, String> asyncCommands = connection.async();
        
        // Lua script to atomically check value and delete if it matches
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        
        return asyncCommands.eval(script, io.lettuce.core.ScriptOutputType.INTEGER, new String[]{lockKey}, lockValue)
            .toCompletableFuture()
            .thenApply(result -> {
                long deleteCount = (Long) result;
                return deleteCount > 0;
            });
    }

    @Override
    public CompletableFuture<Boolean> isLocked(String lockKey) {
        return exists(lockKey);
    }

    // Batch operations
    @Override
    public CompletableFuture<List<String>> mget(String... keys) {
        return mget(DatacenterPreference.LOCAL_PREFERRED, keys);
    }

    @Override
    public CompletableFuture<List<String>> mget(DatacenterPreference preference, String... keys) {
        Optional<String> datacenterId = router.selectDatacenterForRead(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for read operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "MGET", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.mget(keys).toCompletableFuture()
                .thenApply(keyValues -> 
                    keyValues.stream()
                        .map(kv -> kv != null && kv.hasValue() ? kv.getValue() : null)
                        .collect(java.util.stream.Collectors.toList())
                );
        });
    }

    @Override
    public CompletableFuture<Void> mset(Map<String, String> keyValues) {
        return mset(keyValues, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public CompletableFuture<Void> mset(Map<String, String> keyValues, DatacenterPreference preference) {
        Optional<String> datacenterId = router.selectDatacenterForWrite(preference);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.failedFuture(
                new RuntimeException("No available datacenter for write operation"));
        }
        
        return executeWithMetrics(datacenterId.get(), "MSET", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.mset(keyValues).toCompletableFuture().thenApply(result -> null);
        });
    }

    // Utility operations
    @Override
    public CompletableFuture<String> ping() {
        Optional<String> datacenterId = router.selectDatacenterForRead(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.completedFuture("No datacenter available");
        }
        
        return executeWithMetrics(datacenterId.get(), "PING", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId.get());
            return commands.ping().toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<String> ping(String datacenterId) {
        return executeWithMetrics(datacenterId, "PING", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId);
            return commands.ping().toCompletableFuture();
        });
    }

    @Override
    public CompletableFuture<Void> flushAll() {
        // Execute on all datacenters
        List<CompletableFuture<Void>> futures = connections.keySet().stream()
            .<CompletableFuture<Void>>map(datacenterId -> executeWithMetrics(datacenterId, "FLUSHALL", () -> {
                RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId);
                return commands.flushall().toCompletableFuture().thenApply(result -> null);
            }))
            .collect(java.util.stream.Collectors.toList());
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    @Override
    public CompletableFuture<Void> flushAll(String datacenterId) {
        return executeWithMetrics(datacenterId, "FLUSHALL", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId);
            return commands.flushall().toCompletableFuture().thenApply(result -> null);
        });
    }

    @Override
    public CompletableFuture<Long> dbSize() {
        Optional<String> datacenterId = router.selectDatacenterForRead(DatacenterPreference.LOCAL_PREFERRED);
        if (datacenterId.isEmpty()) {
            return CompletableFuture.completedFuture(0L);
        }
        
        return dbSize(datacenterId.get());
    }

    @Override
    public CompletableFuture<Long> dbSize(String datacenterId) {
        return executeWithMetrics(datacenterId, "DBSIZE", () -> {
            RedisAsyncCommands<String, String> commands = getAsyncCommands(datacenterId);
            return commands.dbsize().toCompletableFuture();
        });
    }
}
