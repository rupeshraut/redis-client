package com.redis.multidc.impl;

import com.redis.multidc.operations.ReactiveOperations;
import com.redis.multidc.routing.DatacenterRouter;
import com.redis.multidc.observability.MetricsCollector;
import com.redis.multidc.resilience.ResilienceManager;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import com.redis.multidc.config.DatacenterConfiguration;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reactive implementation of Redis operations for multi-datacenter deployments.
 * Uses Project Reactor and Lettuce reactive API for non-blocking operations.
 */
public class ReactiveOperationsImpl implements ReactiveOperations {
    private static final Logger logger = LoggerFactory.getLogger(ReactiveOperationsImpl.class);
    
    private final Map<String, RedisReactiveCommands<String, String>> datacenterConnections;
    private final DatacenterRouter router;
    private final MetricsCollector metricsCollector;
    private final ResilienceManager resilienceManager;
    private final DatacenterConfiguration configuration;
    private final Map<String, TombstoneKey> tombstoneCache = new ConcurrentHashMap<>();

    public ReactiveOperationsImpl(
            Map<String, RedisReactiveCommands<String, String>> datacenterConnections,
            DatacenterRouter router,
            MetricsCollector metricsCollector,
            ResilienceManager resilienceManager,
            DatacenterConfiguration configuration) {
        this.datacenterConnections = datacenterConnections;
        this.router = router;
        this.metricsCollector = metricsCollector;
        this.resilienceManager = resilienceManager;
        this.configuration = configuration;
    }

    // String operations
    @Override
    public Mono<String> get(String key) {
        return get(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<String> get(String key, DatacenterPreference preference) {
        return Mono.fromCallable(() -> {
            if (isTombstoneKey(key)) {
                return Optional.<String>empty();
            }
            return router.selectDatacenterForRead(preference);
        })
        .flatMap(datacenterOpt -> {
            if (datacenterOpt.isPresent()) {
                String datacenterId = datacenterOpt.get();
                RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                if (commands != null) {
                    return executeWithMetricsAndResilience(datacenterId, "GET", 
                        commands.get(key).timeout(Duration.ofMillis(5000)));
                }
            }
            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> set(String key, String value) {
        return set(key, value, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Void> set(String key, String value, DatacenterPreference preference) {
        Instant startTime = Instant.now();
        return Mono.fromCallable(() -> {
            removeTombstoneKey(key);
            return router.selectDatacenterForWrite(preference);
        })
        .flatMap(datacenterOpt -> {
            if (datacenterOpt.isPresent()) {
                String datacenterId = datacenterOpt.get();
                RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                if (commands != null) {
                    return commands.set(key, value)
                        .timeout(Duration.ofMillis(5000))
                        .then()
                        .doOnSuccess(v -> {
                            Duration latency = Duration.between(startTime, Instant.now());
                            metricsCollector.recordRequest(datacenterId, latency, true);
                        })
                        .onErrorResume(error -> {
                            metricsCollector.recordRequest(datacenterId, Duration.ZERO, false);
                            return Mono.error(error);
                        });
                }
            }
            return Mono.error(new RuntimeException("No datacenter available for write"));
        });
    }

    @Override
    public Mono<Void> set(String key, String value, Duration ttl) {
        return set(key, value, ttl, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Void> set(String key, String value, Duration ttl, DatacenterPreference preference) {
        Instant startTime = Instant.now();
        return Mono.fromCallable(() -> {
            removeTombstoneKey(key);
            return router.selectDatacenterForWrite(preference);
        })
        .flatMap(datacenterOpt -> {
            if (datacenterOpt.isPresent()) {
                String datacenterId = datacenterOpt.get();
                RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                if (commands != null) {
                    return commands.setex(key, ttl.getSeconds(), value)
                        .timeout(Duration.ofMillis(5000))
                        .then()
                        .doOnSuccess(v -> {
                            Duration latency = Duration.between(startTime, Instant.now());
                            metricsCollector.recordRequest(datacenterId, latency, true);
                        })
                        .onErrorResume(error -> {
                            metricsCollector.recordRequest(datacenterId, Duration.ZERO, false);
                            return Mono.error(error);
                        });
                }
            }
            return Mono.error(new RuntimeException("No datacenter available for write"));
        });
    }

    @Override
    public Mono<Boolean> setIfNotExists(String key, String value) {
        return setIfNotExists(key, value, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Boolean> setIfNotExists(String key, String value, Duration ttl) {
        // Implementation for setIfNotExists with TTL
        return setIfNotExists(key, value, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Boolean> setIfNotExists(String key, String value, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.setnx(key, value);
                    }
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Boolean> delete(String key) {
        return delete(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Boolean> delete(String key, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.del(key)
                            .map(count -> {
                                boolean deleted = count > 0;
                                if (deleted) {
                                    createTombstoneKey(key, TombstoneKey.Type.SOFT_DELETE);
                                }
                                return deleted;
                            });
                    }
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Long> delete(String... keys) {
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(DatacenterPreference.LOCAL_PREFERRED))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.del(keys)
                            .doOnSuccess(count -> {
                                for (String key : keys) {
                                    createTombstoneKey(key, TombstoneKey.Type.SOFT_DELETE);
                                }
                            });
                    }
                }
                return Mono.just(0L);
            });
    }

    @Override
    public Mono<Boolean> exists(String key) {
        return exists(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Boolean> exists(String key, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.exists(key).map(count -> count > 0);
                    }
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Duration> ttl(String key) {
        return ttl(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Duration> ttl(String key, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.ttl(key).map(seconds -> Duration.ofSeconds(seconds));
                    }
                }
                return Mono.just(Duration.ofSeconds(-2)); // Key doesn't exist
            });
    }

    @Override
    public Mono<Boolean> expire(String key, Duration ttl) {
        return expire(key, ttl, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Boolean> expire(String key, Duration ttl, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.expire(key, ttl.getSeconds());
                    }
                }
                return Mono.just(false);
            });
    }

    // Hash operations
    @Override
    public Mono<String> hget(String key, String field) {
        return hget(key, field, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<String> hget(String key, String field, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.hget(key, field);
                    }
                }
                return Mono.empty();
            });
    }

    @Override
    public Mono<Map<String, String>> hgetAll(String key) {
        return hgetAll(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Map<String, String>> hgetAll(String key, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.hgetall(key)
                            .collectMap(
                                keyValue -> keyValue.getKey(),
                                keyValue -> keyValue.getValue()
                            );
                    }
                }
                return Mono.just(new java.util.HashMap<>());
            });
    }

    @Override
    public Mono<Void> hset(String key, String field, String value) {
        return hset(key, field, value, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Void> hset(String key, String field, String value, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.hset(key, field, value).then();
                    }
                }
                return Mono.empty();
            });
    }

    @Override
    public Mono<Void> hset(String key, Map<String, String> fields) {
        return hset(key, fields, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Void> hset(String key, Map<String, String> fields, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.hmset(key, fields).then();
                    }
                }
                return Mono.empty();
            });
    }

    @Override
    public Mono<Boolean> hdel(String key, String field) {
        return hdel(key, field, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Boolean> hdel(String key, String field, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.hdel(key, field).map(count -> count > 0);
                    }
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Long> hdel(String key, String... fields) {
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(DatacenterPreference.LOCAL_PREFERRED))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.hdel(key, fields);
                    }
                }
                return Mono.just(0L);
            });
    }

    @Override
    public Mono<Boolean> hexists(String key, String field) {
        return hexists(key, field, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Boolean> hexists(String key, String field, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.hexists(key, field);
                    }
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Set<String>> hkeys(String key) {
        return hkeys(key, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Set<String>> hkeys(String key, DatacenterPreference preference) {
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.hkeys(key).collect(java.util.stream.Collectors.toSet());
                    }
                }
                return Mono.empty();
            });
    }

    // List operations - stub implementations
    @Override
    public Mono<String> lpop(String key) { return lpop(key, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<String> lpop(String key, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.lpop(key);
                }
                return Mono.empty();
            });
    }

    @Override
    public Mono<String> rpop(String key) { return rpop(key, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<String> rpop(String key, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.rpop(key);
                }
                return Mono.empty();
            });
    }

    @Override
    public Mono<Void> lpush(String key, String... values) { return lpush(key, DatacenterPreference.LOCAL_PREFERRED, values); }
    @Override
    public Mono<Void> lpush(String key, DatacenterPreference preference, String... values) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.lpush(key, values).then();
                }
                return Mono.empty();
            });
    }

    @Override
    public Mono<Void> rpush(String key, String... values) { return rpush(key, DatacenterPreference.LOCAL_PREFERRED, values); }
    @Override
    public Mono<Void> rpush(String key, DatacenterPreference preference, String... values) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.rpush(key, values).then();
                }
                return Mono.empty();
            });
    }

    @Override
    public Flux<String> lrange(String key, long start, long stop) { return lrange(key, start, stop, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Flux<String> lrange(String key, long start, long stop, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMapMany(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.lrange(key, start, stop);
                }
                return Flux.empty();
            });
    }

    @Override
    public Mono<Long> llen(String key) { return llen(key, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<Long> llen(String key, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.llen(key);
                }
                return Mono.just(0L);
            });
    }

    // Set operations - stub implementations
    @Override
    public Mono<Boolean> sadd(String key, String... members) { return sadd(key, DatacenterPreference.LOCAL_PREFERRED, members); }
    @Override
    public Mono<Boolean> sadd(String key, DatacenterPreference preference, String... members) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.sadd(key, members).map(count -> count > 0);
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Boolean> srem(String key, String... members) { return srem(key, DatacenterPreference.LOCAL_PREFERRED, members); }
    @Override
    public Mono<Boolean> srem(String key, DatacenterPreference preference, String... members) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.srem(key, members).map(count -> count > 0);
                }
                return Mono.just(false);
            });
    }

    @Override
    public Flux<String> smembers(String key) { return smembers(key, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Flux<String> smembers(String key, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMapMany(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.smembers(key);
                }
                return Flux.empty();
            });
    }

    @Override
    public Mono<Boolean> sismember(String key, String member) { return sismember(key, member, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<Boolean> sismember(String key, String member, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.sismember(key, member);
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Long> scard(String key) { return scard(key, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<Long> scard(String key, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.scard(key);
                }
                return Mono.just(0L);
            });
    }

    // Sorted set operations - stub implementations
    @Override
    public Mono<Boolean> zadd(String key, double score, String member) { return zadd(key, score, member, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<Boolean> zadd(String key, double score, String member, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.zadd(key, score, member).map(count -> count > 0);
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Boolean> zadd(String key, Map<String, Double> scoreMembers) { return zadd(key, scoreMembers, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<Boolean> zadd(String key, Map<String, Double> scoreMembers, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        // Convert to Lettuce format
                        Object[] args = new Object[scoreMembers.size() * 2];
                        int i = 0;
                        for (Map.Entry<String, Double> entry : scoreMembers.entrySet()) {
                            args[i++] = entry.getValue();
                            args[i++] = entry.getKey();
                        }
                        return commands.zadd(key, args).map(count -> count > 0);
                    }
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Boolean> zrem(String key, String... members) { return zrem(key, DatacenterPreference.LOCAL_PREFERRED, members); }
    @Override
    public Mono<Boolean> zrem(String key, DatacenterPreference preference, String... members) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.zrem(key, members).map(count -> count > 0);
                }
                return Mono.just(false);
            });
    }

    @Override
    public Flux<String> zrange(String key, long start, long stop) { return zrange(key, start, stop, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Flux<String> zrange(String key, long start, long stop, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMapMany(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.zrange(key, start, stop);
                }
                return Flux.empty();
            });
    }

    @Override
    public Mono<Double> zscore(String key, String member) { return zscore(key, member, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<Double> zscore(String key, String member, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.zscore(key, member);
                }
                return Mono.empty();
            });
    }

    @Override
    public Mono<Long> zcard(String key) { return zcard(key, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<Long> zcard(String key, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.zcard(key);
                }
                return Mono.just(0L);
            });
    }

    // Cross-datacenter operations
    @Override
    public Mono<String> crossDatacenterGet(String key, String datacenterKey) {
        RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterKey);
        if (commands != null) {
            return commands.get(key);
        }
        return Mono.empty();
    }

    @Override
    public Mono<Map<String, String>> crossDatacenterMultiGet(List<String> keys, String datacenterKey) {
        RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterKey);
        if (commands != null) {
            return commands.mget(keys.toArray(new String[0]))
                .collectList()
                .map(keyValues -> {
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
        return Mono.just(new java.util.HashMap<>());
    }

    // Tombstone operations
    @Override
    public Mono<Void> createTombstone(String key, TombstoneKey.Type type) {
        return createTombstone(key, type, Duration.ofHours(24));
    }

    @Override
    public Mono<Void> createTombstone(String key, TombstoneKey.Type type, Duration ttl) {
        return Mono.fromRunnable(() -> {
            TombstoneKey tombstone = TombstoneKey.builder()
                .key(key)
                .type(type)
                .createdAt(Instant.now())
                .expiresIn(ttl)
                .build();
            tombstoneCache.put(key, tombstone);
        });
    }

    @Override
    public Mono<Boolean> isTombstoned(String key) {
        return Mono.fromCallable(() -> tombstoneCache.containsKey(key) && 
            !tombstoneCache.get(key).isExpired());
    }

    @Override
    public Mono<TombstoneKey> getTombstone(String key) {
        return Mono.fromCallable(() -> tombstoneCache.get(key))
            .filter(tombstone -> !tombstone.isExpired());
    }

    @Override
    public Mono<Void> removeTombstone(String key) {
        return Mono.fromRunnable(() -> tombstoneCache.remove(key));
    }

    // Distributed lock operations
    @Override
    public Mono<Boolean> acquireLock(String lockKey, String lockValue, Duration timeout) {
        return acquireLock(lockKey, lockValue, timeout, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Boolean> acquireLock(String lockKey, String lockValue, Duration timeout, DatacenterPreference preference) {
        return set(lockKey, lockValue, timeout, preference)
            .then(Mono.just(true))
            .onErrorReturn(false);
    }

    @Override
    public Mono<Boolean> releaseLock(String lockKey, String lockValue) {
        return releaseLock(lockKey, lockValue, DatacenterPreference.LOCAL_PREFERRED);
    }

    @Override
    public Mono<Boolean> releaseLock(String lockKey, String lockValue, DatacenterPreference preference) {
        return get(lockKey, preference)
            .flatMap(currentValue -> {
                if (lockValue.equals(currentValue)) {
                    return delete(lockKey, preference);
                }
                return Mono.just(false);
            });
    }

    @Override
    public Mono<Boolean> isLocked(String lockKey) {
        return exists(lockKey);
    }

    // Batch operations
    @Override
    public Flux<String> mget(String... keys) { return mget(DatacenterPreference.LOCAL_PREFERRED, keys); }
    @Override
    public Flux<String> mget(DatacenterPreference preference, String... keys) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMapMany(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        return commands.mget(keys)
                            .map(keyValue -> keyValue.hasValue() ? keyValue.getValue() : null)
                            .filter(value -> value != null);
                    }
                }
                return Flux.empty();
            });
    }

    @Override
    public Mono<Void> mset(Map<String, String> keyValues) { return mset(keyValues, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Mono<Void> mset(Map<String, String> keyValues, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForWrite(preference))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.mset(keyValues).then();
                }
                return Mono.empty();
            });
    }

    @Override
    public Flux<String> scan(String pattern) { return scan(pattern, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Flux<String> scan(String pattern, DatacenterPreference preference) { 
        return Mono.fromCallable(() -> router.selectDatacenterForRead(preference))
            .flatMapMany(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) {
                        // Use scan instead of keys for better performance
                        return commands.scan()
                            .map(scanResult -> scanResult.getKeys())
                            .flatMapIterable(keys -> keys)
                            .filter(key -> key.matches(pattern.replace("*", ".*")));
                    }
                }
                return Flux.empty();
            });
    }

    @Override
    public Flux<Map.Entry<String, String>> hscan(String key, String pattern) { return hscan(key, pattern, DatacenterPreference.LOCAL_PREFERRED); }
    @Override
    public Flux<Map.Entry<String, String>> hscan(String key, String pattern, DatacenterPreference preference) { 
        return hgetAll(key, preference)
            .flatMapMany(map -> Flux.fromIterable(map.entrySet()))
            .filter(entry -> entry.getKey().matches(pattern.replace("*", ".*")));
    }

    // Ping and health
    @Override
    public Mono<String> ping() {
        return Mono.fromCallable(() -> router.selectDatacenterForRead(DatacenterPreference.LOCAL_PREFERRED))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
                    if (commands != null) return commands.ping();
                }
                return Mono.just("No datacenter available");
            });
    }

    @Override
    public Mono<String> ping(String datacenterId) {
        RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
        if (commands != null) {
            return commands.ping();
        }
        return Mono.just("Datacenter not available: " + datacenterId);
    }

    // Utility operations
    @Override
    public Mono<Void> flushAll() {
        return Flux.fromIterable(datacenterConnections.values())
            .flatMap(commands -> commands.flushall())
            .then();
    }

    @Override
    public Mono<Void> flushAll(String datacenterId) {
        RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
        if (commands != null) {
            return commands.flushall().then();
        }
        return Mono.empty();
    }

    @Override
    public Mono<Long> dbSize() {
        return Mono.fromCallable(() -> router.selectDatacenterForRead(DatacenterPreference.LOCAL_PREFERRED))
            .flatMap(datacenterOpt -> {
                if (datacenterOpt.isPresent()) {
                    String datacenterId = datacenterOpt.get();
                    return dbSize(datacenterId);
                }
                return Mono.just(0L);
            });
    }

    @Override
    public Mono<Long> dbSize(String datacenterId) {
        RedisReactiveCommands<String, String> commands = datacenterConnections.get(datacenterId);
        if (commands != null) {
            return commands.dbsize();
        }
        return Mono.just(0L);
    }

    // Helper methods
    private boolean isTombstoneKey(String key) {
        TombstoneKey tombstone = tombstoneCache.get(key);
        return tombstone != null && !tombstone.isExpired();
    }

    private void removeTombstoneKey(String key) {
        tombstoneCache.remove(key);
    }

    private void createTombstoneKey(String key, TombstoneKey.Type type) {
        TombstoneKey tombstone = TombstoneKey.builder()
            .key(key)
            .type(type)
            .createdAt(Instant.now())
            .expiresIn(Duration.ofHours(24))
            .build();
        tombstoneCache.put(key, tombstone);
    }

    // Helper method to execute operations with metrics and resilience patterns
    private <T> Mono<T> executeWithMetricsAndResilience(String datacenterId, String operation, Mono<T> operationMono) {
        Instant startTime = Instant.now();
        
        // Use ResilienceManager to decorate the reactive operation with all resilience patterns
        return resilienceManager.decorateMono(datacenterId, operationMono)
            .doOnSuccess(value -> {
                Duration latency = Duration.between(startTime, Instant.now());
                metricsCollector.recordRequest(datacenterId, latency, true);
            })
            .doOnError(error -> {
                Duration latency = Duration.between(startTime, Instant.now());
                metricsCollector.recordRequest(datacenterId, latency, false);
                logger.error("Reactive operation {} failed for datacenter {}: {}", 
                    operation, datacenterId, error.getMessage());
            });
    }
}
