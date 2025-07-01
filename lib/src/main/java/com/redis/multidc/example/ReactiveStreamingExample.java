package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.operations.ReactiveOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Comprehensive example demonstrating reactive streaming operations
 * with the Redis Multi-Datacenter Client.
 * 
 * Features demonstrated:
 * - Reactive operations with Mono and Flux
 * - Streaming operations (scan, hscan)
 * - Backpressure handling
 * - Error handling with reactive streams
 * - Parallel processing across datacenters
 * - Real-time data processing pipelines
 */
public class ReactiveStreamingExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ReactiveStreamingExample.class);
    
    public static void main(String[] args) {
        ReactiveStreamingExample example = new ReactiveStreamingExample();
        example.runExample();
    }
    
    public void runExample() {
        logger.info("Starting Reactive Streaming Example");
        
        // Configure datacenters
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .host("localhost")
                    .port(6379)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("us-west-2")
                    .host("localhost")
                    .port(6380)
                    .build()
            ))
            .build();
        
        // Create client
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            
            ReactiveOperations reactive = client.reactive();
            
            // Setup test data
            setupTestData(reactive);
            
            // Demonstrate various reactive streaming patterns
            demonstrateBasicReactiveOperations(reactive);
            demonstrateStreamingOperations(reactive);
            demonstrateBatchProcessing(reactive);
            demonstrateParallelProcessing(reactive);
            demonstrateErrorHandling(reactive);
            demonstrateBackpressureHandling(reactive);
            demonstrateRealTimeDataPipeline(reactive);
            
            logger.info("Reactive Streaming Example completed successfully");
            
        } catch (Exception e) {
            logger.error("Error in reactive streaming example", e);
        }
    }
    
    private void setupTestData(ReactiveOperations reactive) {
        logger.info("Setting up test data...");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        // Setup data in parallel using reactive streams
        Flux.range(1, 100)
            .flatMap(i -> reactive.set("key:" + i, "value:" + i))
            .then(Flux.range(1, 50)
                .flatMap(i -> {
                    Map<String, String> hash = new HashMap<>();
                    hash.put("field1", "value1_" + i);
                    hash.put("field2", "value2_" + i);
                    hash.put("timestamp", String.valueOf(System.currentTimeMillis()));
                    return reactive.hset("hash:" + i, hash);
                })
                .then())
            .then(Flux.range(1, 30)
                .flatMap(i -> reactive.sadd("set:" + i, "member1_" + i, "member2_" + i, "member3_" + i))
                .then())
            .doOnTerminate(() -> {
                logger.info("Test data setup completed");
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Error setting up test data", error);
                latch.countDown();
            })
            .subscribe();
        
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while setting up test data");
        }
    }
    
    private void demonstrateBasicReactiveOperations(ReactiveOperations reactive) {
        logger.info("=== Basic Reactive Operations ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        // Chain reactive operations
        reactive.set("reactive:test", "initial_value")
            .then(reactive.get("reactive:test"))
            .doOnNext(value -> logger.info("Retrieved value: {}", value))
            .then(reactive.set("reactive:test", "updated_value", Duration.ofMinutes(5)))
            .then(reactive.ttl("reactive:test"))
            .doOnNext(ttl -> logger.info("TTL: {} seconds", ttl.getSeconds()))
            .then(reactive.exists("reactive:test"))
            .doOnNext(exists -> logger.info("Key exists: {}", exists))
            .doOnSuccess(v -> {
                logger.info("Basic reactive operations completed");
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Error in basic reactive operations", error);
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void demonstrateStreamingOperations(ReactiveOperations reactive) {
        logger.info("=== Streaming Operations ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        // Stream all keys matching a pattern
        reactive.scan("key:*")
            .take(10) // Limit to first 10 keys
            .flatMap(key -> reactive.get(key)
                .map(value -> Map.entry(key, value)))
            .doOnNext(entry -> logger.info("Scanned key: {} = {}", entry.getKey(), entry.getValue()))
            .count()
            .doOnNext(count -> logger.info("Scanned {} keys", count))
            .then(
                // Stream hash fields
                reactive.hscan("hash:1", "*")
                    .take(5)
                    .doOnNext(entry -> logger.info("Hash field: {} = {}", entry.getKey(), entry.getValue()))
                    .count()
                    .doOnNext(count -> logger.info("Scanned {} hash fields", count))
            )
            .doOnSuccess(v -> {
                logger.info("Streaming operations completed");
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Error in streaming operations", error);
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void demonstrateBatchProcessing(ReactiveOperations reactive) {
        logger.info("=== Batch Processing ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        // Process multiple keys in batches
        String[] keys = {"key:1", "key:2", "key:3", "key:4", "key:5"};
        
        reactive.mget(keys)
            .collectList()
            .doOnNext(values -> {
                for (int i = 0; i < keys.length && i < values.size(); i++) {
                    logger.info("Batch get: {} = {}", keys[i], values.get(i));
                }
            })
            .then(
                // Batch set operations
                Mono.fromCallable(() -> {
                    Map<String, String> batchData = new HashMap<>();
                    batchData.put("batch:1", "batch_value_1");
                    batchData.put("batch:2", "batch_value_2");
                    batchData.put("batch:3", "batch_value_3");
                    return batchData;
                })
                .flatMap(reactive::mset)
            )
            .then(reactive.mget("batch:1", "batch:2", "batch:3").collectList())
            .doOnNext(values -> logger.info("Batch set result: {}", values))
            .doOnSuccess(v -> {
                logger.info("Batch processing completed");
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Error in batch processing", error);
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void demonstrateParallelProcessing(ReactiveOperations reactive) {
        logger.info("=== Parallel Processing ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        // Process operations in parallel across datacenters
        Mono<String> localOp = reactive.get("key:1", DatacenterPreference.LOCAL_ONLY)
            .doOnNext(value -> logger.info("Local datacenter result: {}", value));
        
        Mono<String> remoteOp = reactive.get("key:1", DatacenterPreference.REMOTE_ONLY)
            .doOnNext(value -> logger.info("Remote datacenter result: {}", value));
        
        Mono<String> anyOp = reactive.get("key:1", DatacenterPreference.ANY_AVAILABLE)
            .doOnNext(value -> logger.info("Any datacenter result: {}", value));
        
        // Execute all operations in parallel
        Mono.zip(localOp, remoteOp, anyOp)
            .doOnNext(tuple -> logger.info("Parallel operations completed: local={}, remote={}, any={}", 
                tuple.getT1(), tuple.getT2(), tuple.getT3()))
            .then(
                // Parallel processing of multiple keys
                Flux.range(1, 20)
                    .parallel(4) // Use 4 parallel rails
                    .runOn(Schedulers.parallel())
                    .map(i -> "parallel:key:" + i)
                    .flatMap(key -> reactive.set(key, "parallel_value_" + System.currentTimeMillis()))
                    .sequential()
                    .count()
                    .doOnNext(count -> logger.info("Processed {} keys in parallel", count))
            )
            .doOnSuccess(v -> {
                logger.info("Parallel processing completed");
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Error in parallel processing", error);
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void demonstrateErrorHandling(ReactiveOperations reactive) {
        logger.info("=== Error Handling ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        // Demonstrate various error handling strategies
        reactive.get("nonexistent:key")
            .switchIfEmpty(Mono.just("default_value"))
            .doOnNext(value -> logger.info("Value with fallback: {}", value))
            .then(
                // Retry on failure
                reactive.get("another:nonexistent:key")
                    .switchIfEmpty(Mono.error(new RuntimeException("Key not found")))
                    .retry(3)
                    .onErrorReturn("fallback_after_retry")
                    .doOnNext(value -> logger.info("Value after retry: {}", value))
            )
            .then(
                // Error handling with timeout
                reactive.get("key:1")
                    .timeout(Duration.ofSeconds(5))
                    .onErrorResume(throwable -> {
                        logger.warn("Operation timed out, using fallback", throwable);
                        return Mono.just("timeout_fallback");
                    })
                    .doOnNext(value -> logger.info("Value with timeout handling: {}", value))
            )
            .doOnSuccess(v -> {
                logger.info("Error handling demonstration completed");
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Unexpected error in error handling demo", error);
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void demonstrateBackpressureHandling(ReactiveOperations reactive) {
        logger.info("=== Backpressure Handling ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        // Generate a large stream and handle backpressure
        Flux.range(1, 1000)
            .map(i -> "stream:key:" + i)
            .buffer(50) // Buffer in chunks of 50
            .flatMap(keyBatch -> 
                Flux.fromIterable(keyBatch)
                    .flatMap(key -> reactive.set(key, "stream_value_" + System.currentTimeMillis()))
                    .count(), 
                2) // Limit concurrent batches
            .reduce(0L, Long::sum)
            .doOnNext(total -> logger.info("Processed {} operations with backpressure control", total))
            .then(
                // Demonstrate rate limiting
                Flux.interval(Duration.ofMillis(100))
                    .take(20)
                    .flatMap(i -> reactive.set("rate:limited:" + i, "value_" + i))
                    .count()
                    .doOnNext(count -> logger.info("Rate limited operations: {}", count))
            )
            .doOnSuccess(v -> {
                logger.info("Backpressure handling completed");
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Error in backpressure handling", error);
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void demonstrateRealTimeDataPipeline(ReactiveOperations reactive) {
        logger.info("=== Real-time Data Pipeline ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        // Simulate a real-time data processing pipeline
        Flux.interval(Duration.ofMillis(200))
            .take(25)
            .map(i -> {
                Map<String, String> data = new HashMap<>();
                data.put("id", String.valueOf(i));
                data.put("timestamp", String.valueOf(System.currentTimeMillis()));
                data.put("value", String.valueOf(Math.random() * 100));
                return data;
            })
            .flatMap(data -> {
                String key = "pipeline:data:" + data.get("id");
                // Store the data
                return reactive.hset(key, data)
                    .then(reactive.expire(key, Duration.ofMinutes(5)))
                    .thenReturn(data);
            })
            .buffer(Duration.ofSeconds(2)) // Aggregate data every 2 seconds
            .doOnNext(batch -> logger.info("Processed batch of {} items", batch.size()))
            .flatMap(batch -> {
                // Process aggregated batch
                double avgValue = batch.stream()
                    .mapToDouble(data -> Double.parseDouble(data.get("value")))
                    .average()
                    .orElse(0.0);
                
                return reactive.set("pipeline:avg:" + System.currentTimeMillis(), 
                    String.valueOf(avgValue))
                    .thenReturn(avgValue);
            })
            .doOnNext(avg -> logger.info("Calculated average: {}", avg))
            .count()
            .doOnNext(batches -> logger.info("Processed {} batches in pipeline", batches))
            .doOnSuccess(v -> {
                logger.info("Real-time data pipeline completed");
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Error in real-time data pipeline", error);
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void await(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for operation completion");
        }
    }
}
