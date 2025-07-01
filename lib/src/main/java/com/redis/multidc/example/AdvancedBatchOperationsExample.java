package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.operations.AsyncOperations;
import com.redis.multidc.operations.ReactiveOperations;
import com.redis.multidc.operations.SyncOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Comprehensive example demonstrating advanced batch operations and
 * cross-datacenter operations with the Redis Multi-Datacenter Client.
 * 
 * Features demonstrated:
 * - Large-scale batch operations
 * - Cross-datacenter data operations
 * - Optimized bulk data transfer
 * - Parallel batch processing
 * - Data consistency across datacenters
 * - Performance optimization techniques
 * - Error handling in batch operations
 */
public class AdvancedBatchOperationsExample {
    
    private static final Logger logger = LoggerFactory.getLogger(AdvancedBatchOperationsExample.class);
    
    public static void main(String[] args) {
        AdvancedBatchOperationsExample example = new AdvancedBatchOperationsExample();
        example.runExample();
    }
    
    public void runExample() {
        logger.info("Starting Advanced Batch Operations Example");
        
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
                    .build(),
                DatacenterEndpoint.builder()
                    .id("eu-west-1")
                    .host("localhost")
                    .port(6381)
                    .build()
            ))
            .build();
        
        // Create client
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            
            // Demonstrate various batch operation patterns
            demonstrateSyncBatchOperations(client.sync());
            demonstrateAsyncBatchOperations(client.async());
            demonstrateReactiveBatchOperations(client.reactive());
            demonstrateCrossDatacenterOperations(client);
            demonstrateOptimizedBulkTransfer(client);
            demonstrateParallelBatchProcessing(client);
            demonstrateDataConsistencyOperations(client);
            demonstrateBatchErrorHandling(client);
            
            logger.info("Advanced Batch Operations Example completed successfully");
            
        } catch (Exception e) {
            logger.error("Error in advanced batch operations example", e);
        }
    }
    
    private void demonstrateSyncBatchOperations(SyncOperations sync) {
        logger.info("=== Synchronous Batch Operations ===");
        
        // Large batch set operation
        Map<String, String> largeBatch = IntStream.range(1, 1001)
            .boxed()
            .collect(Collectors.toMap(
                i -> "batch:sync:" + i,
                i -> "value_" + i + "_" + System.currentTimeMillis()
            ));
        
        long startTime = System.currentTimeMillis();
        sync.mset(largeBatch);
        long setDuration = System.currentTimeMillis() - startTime;
        logger.info("Sync batch set: {} keys in {}ms", largeBatch.size(), setDuration);
        
        // Large batch get operation
        String[] keys = largeBatch.keySet().toArray(new String[0]);
        startTime = System.currentTimeMillis();
        List<String> values = sync.mget(keys);
        long getDuration = System.currentTimeMillis() - startTime;
        logger.info("Sync batch get: {} values in {}ms", values.size(), getDuration);
        
        // Batch hash operations
        Map<String, String> hashBatch = new HashMap<>();
        hashBatch.put("field1", "hash_value_1");
        hashBatch.put("field2", "hash_value_2");
        hashBatch.put("field3", "hash_value_3");
        hashBatch.put("timestamp", String.valueOf(System.currentTimeMillis()));
        
        IntStream.range(1, 101).forEach(i -> {
            String hashKey = "batch:hash:" + i;
            sync.hset(hashKey, hashBatch);
        });
        logger.info("Created {} hash objects with {} fields each", 100, hashBatch.size());
        
        // Batch existence checks
        startTime = System.currentTimeMillis();
        int existingKeys = 0;
        for (int i = 1; i <= 100; i++) {
            if (sync.exists("batch:sync:" + i)) {
                existingKeys++;
            }
        }
        long existsDuration = System.currentTimeMillis() - startTime;
        logger.info("Sync existence check: {}/{} keys exist (checked in {}ms)", existingKeys, 100, existsDuration);
        
        // Cleanup
        sync.delete(keys);
        logger.info("Cleaned up {} keys", keys.length);
    }
    
    private void demonstrateAsyncBatchOperations(AsyncOperations async) {
        logger.info("=== Asynchronous Batch Operations ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        // Prepare large dataset
        Map<String, String> asyncBatch = IntStream.range(1, 501)
            .boxed()
            .collect(Collectors.toMap(
                i -> "batch:async:" + i,
                i -> "async_value_" + i + "_" + System.currentTimeMillis()
            ));
        
        long startTime = System.currentTimeMillis();
        
        // Chain async operations
        async.mset(asyncBatch)
            .thenCompose(v -> {
                long setDuration = System.currentTimeMillis() - startTime;
                logger.info("Async batch set: {} keys in {}ms", asyncBatch.size(), setDuration);
                
                // Async batch get
                String[] keys = asyncBatch.keySet().toArray(new String[0]);
                return async.mget(keys);
            })
            .thenCompose(values -> {
                long getDuration = System.currentTimeMillis() - startTime;
                logger.info("Async batch get: {} values in total duration {}ms", values.size(), getDuration);
                
                // Parallel async operations
                List<CompletableFuture<Boolean>> futures = new ArrayList<>();
                for (int i = 1; i <= 50; i++) {
                    String key = "batch:async:parallel:" + i;
                    String value = "parallel_value_" + i;
                    futures.add(async.setIfNotExists(key, value));
                }
                
                return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> futures.stream()
                        .mapToLong(f -> f.join() ? 1 : 0)
                        .sum());
            })
            .thenCompose(successCount -> {
                logger.info("Async parallel operations: {} successful", successCount);
                
                // Async batch TTL operations
                List<CompletableFuture<Boolean>> ttlFutures = new ArrayList<>();
                for (int i = 1; i <= 50; i++) {
                    String key = "batch:async:" + i;
                    ttlFutures.add(async.expire(key, Duration.ofMinutes(10)));
                }
                
                return CompletableFuture.allOf(ttlFutures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> ttlFutures.stream()
                        .mapToLong(f -> f.join() ? 1 : 0)
                        .sum());
            })
            .thenAccept(ttlCount -> {
                long totalDuration = System.currentTimeMillis() - startTime;
                logger.info("Async TTL operations: {} successful, total duration: {}ms", ttlCount, totalDuration);
                latch.countDown();
            })
            .exceptionally(throwable -> {
                logger.error("Error in async batch operations", throwable);
                latch.countDown();
                return null;
            });
        
        await(latch);
    }
    
    private void demonstrateReactiveBatchOperations(ReactiveOperations reactive) {
        logger.info("=== Reactive Batch Operations ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        long startTime = System.currentTimeMillis();
        
        // Reactive batch processing with backpressure control
        Flux.range(1, 2000)
            .buffer(100) // Process in chunks of 100
            .flatMap(batch -> {
                // Create batch data
                Map<String, String> batchData = batch.stream()
                    .collect(Collectors.toMap(
                        i -> "batch:reactive:" + i,
                        i -> "reactive_value_" + i + "_" + System.currentTimeMillis()
                    ));
                
                return reactive.mset(batchData)
                    .thenReturn(batchData.size());
            }, 4) // Limit concurrency to 4 batches
            .reduce(0, Integer::sum)
            .doOnNext(totalKeys -> {
                long duration = System.currentTimeMillis() - startTime;
                logger.info("Reactive batch processing: {} total keys in {}ms", totalKeys, duration);
            })
            .then(
                // Reactive batch validation
                Flux.range(1, 200)
                    .flatMap(i -> reactive.exists("batch:reactive:" + i))
                    .filter(exists -> exists)
                    .count()
                    .doOnNext(existingCount -> logger.info("Reactive validation: {} keys exist", existingCount))
            )
            .then(
                // Reactive batch transformation
                Flux.range(1, 100)
                    .flatMap(i -> {
                        String key = "batch:reactive:" + i;
                        return reactive.get(key)
                            .map(value -> value.toUpperCase())
                            .flatMap(upperValue -> reactive.set(key + ":upper", upperValue));
                    })
                    .count()
                    .doOnNext(transformedCount -> logger.info("Reactive transformation: {} keys transformed", transformedCount))
            )
            .then(
                // Reactive streaming aggregation
                Flux.range(1, 500)
                    .flatMap(i -> reactive.get("batch:reactive:" + i))
                    .filter(Objects::nonNull)
                    .buffer(50)
                    .map(valuesBatch -> {
                        // Simulate aggregation
                        return "aggregated_" + valuesBatch.size() + "_" + System.currentTimeMillis();
                    })
                    .flatMap(aggregatedValue -> reactive.set("batch:reactive:aggregated:" + System.currentTimeMillis(), aggregatedValue))
                    .count()
                    .doOnNext(aggregatedBatches -> logger.info("Reactive aggregation: {} batches aggregated", aggregatedBatches))
            )
            .doOnSuccess(v -> {
                long totalDuration = System.currentTimeMillis() - startTime;
                logger.info("Reactive batch operations completed in {}ms", totalDuration);
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Error in reactive batch operations", error);
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void demonstrateCrossDatacenterOperations(MultiDatacenterRedisClient client) {
        logger.info("=== Cross-Datacenter Operations ===");
        
        SyncOperations sync = client.sync();
        
        // Set data in local datacenter
        Map<String, String> localData = new HashMap<>();
        localData.put("cross:local:1", "local_value_1");
        localData.put("cross:local:2", "local_value_2");
        localData.put("cross:local:3", "local_value_3");
        
        sync.mset(localData, DatacenterPreference.LOCAL_ONLY);
        logger.info("Set {} keys in LOCAL datacenter", localData.size());
        
        // Set data in remote datacenter
        Map<String, String> remoteData = new HashMap<>();
        remoteData.put("cross:remote:1", "remote_value_1");
        remoteData.put("cross:remote:2", "remote_value_2");
        remoteData.put("cross:remote:3", "remote_value_3");
        
        sync.mset(remoteData, DatacenterPreference.REMOTE_ONLY);
        logger.info("Set {} keys in REMOTE datacenter", remoteData.size());
        
        // Cross-datacenter lookup
        List<String> localValues = sync.mget(DatacenterPreference.LOCAL_ONLY, 
            "cross:local:1", "cross:local:2", "cross:local:3");
        logger.info("Retrieved from LOCAL: {}", localValues);
        
        List<String> remoteValues = sync.mget(DatacenterPreference.REMOTE_ONLY, 
            "cross:remote:1", "cross:remote:2", "cross:remote:3");
        logger.info("Retrieved from REMOTE: {}", remoteValues);
        
        // Cross-datacenter data comparison
        String key = "cross:comparison:test";
        sync.set(key, "version_1", DatacenterPreference.LOCAL_ONLY);
        sync.set(key, "version_2", DatacenterPreference.REMOTE_ONLY);
        
        String localVersion = sync.get(key, DatacenterPreference.LOCAL_ONLY);
        String remoteVersion = sync.get(key, DatacenterPreference.REMOTE_ONLY);
        
        logger.info("Cross-datacenter comparison - Local: '{}', Remote: '{}'", localVersion, remoteVersion);
        
        // Data synchronization simulation
        if (!Objects.equals(localVersion, remoteVersion)) {
            logger.info("Detected data inconsistency, synchronizing...");
            String latestVersion = "synchronized_version_" + System.currentTimeMillis();
            sync.set(key, latestVersion, DatacenterPreference.LOCAL_ONLY);
            sync.set(key, latestVersion, DatacenterPreference.REMOTE_ONLY);
            logger.info("Data synchronized to: '{}'", latestVersion);
        }
    }
    
    private void demonstrateOptimizedBulkTransfer(MultiDatacenterRedisClient client) {
        logger.info("=== Optimized Bulk Transfer ===");
        
        ReactiveOperations reactive = client.reactive();
        CountDownLatch latch = new CountDownLatch(1);
        
        // Simulate bulk data transfer with optimization
        long startTime = System.currentTimeMillis();
        
        // Generate large dataset
        int totalRecords = 5000;
        int batchSize = 200;
        
        Flux.range(0, (totalRecords + batchSize - 1) / batchSize) // Calculate number of batches
            .flatMap(batchIndex -> {
                int startIdx = batchIndex * batchSize;
                int endIdx = Math.min(startIdx + batchSize, totalRecords);
                
                Map<String, String> batch = IntStream.range(startIdx, endIdx)
                    .boxed()
                    .collect(Collectors.toMap(
                        i -> "bulk:transfer:" + i,
                        i -> generateLargeValue(i)
                    ));
                
                return reactive.mset(batch, DatacenterPreference.LOCAL_ONLY)
                    .doOnSuccess(v -> logger.debug("Transferred batch {}: {} records", batchIndex, batch.size()))
                    .thenReturn(batch.size());
            }, 3) // Limit concurrent batches for optimization
            .reduce(0, Integer::sum)
            .doOnNext(totalTransferred -> {
                long duration = System.currentTimeMillis() - startTime;
                long throughput = (totalTransferred * 1000L) / duration; // records per second
                logger.info("Bulk transfer completed: {} records in {}ms ({} records/sec)", 
                    totalTransferred, duration, throughput);
            })
            .then(
                // Verify transfer integrity
                Flux.range(0, 100) // Sample verification
                    .flatMap(i -> reactive.exists("bulk:transfer:" + i))
                    .filter(exists -> exists)
                    .count()
                    .doOnNext(verifiedCount -> logger.info("Transfer verification: {}/100 samples verified", verifiedCount))
            )
            .doOnSuccess(v -> {
                long totalDuration = System.currentTimeMillis() - startTime;
                logger.info("Optimized bulk transfer completed in {}ms", totalDuration);
                latch.countDown();
            })
            .doOnError(error -> {
                logger.error("Error in bulk transfer", error);
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void demonstrateParallelBatchProcessing(MultiDatacenterRedisClient client) {
        logger.info("=== Parallel Batch Processing ===");
        
        AsyncOperations async = client.async();
        CountDownLatch latch = new CountDownLatch(1);
        
        long startTime = System.currentTimeMillis();
        
        // Process multiple batches in parallel across datacenters
        List<CompletableFuture<Integer>> parallelBatches = new ArrayList<>();
        
        // Local datacenter batches
        for (int batch = 1; batch <= 5; batch++) {
            final int batchId = batch;
            CompletableFuture<Integer> batchFuture = CompletableFuture.supplyAsync(() -> {
                Map<String, String> batchData = IntStream.range(1, 101)
                    .boxed()
                    .collect(Collectors.toMap(
                        i -> "parallel:local:" + batchId + ":" + i,
                        i -> "local_batch_" + batchId + "_value_" + i
                    ));
                
                return async.mset(batchData, DatacenterPreference.LOCAL_ONLY)
                    .thenApply(v -> batchData.size())
                    .join();
            });
            parallelBatches.add(batchFuture);
        }
        
        // Remote datacenter batches
        for (int batch = 1; batch <= 3; batch++) {
            final int batchId = batch;
            CompletableFuture<Integer> batchFuture = CompletableFuture.supplyAsync(() -> {
                Map<String, String> batchData = IntStream.range(1, 151)
                    .boxed()
                    .collect(Collectors.toMap(
                        i -> "parallel:remote:" + batchId + ":" + i,
                        i -> "remote_batch_" + batchId + "_value_" + i
                    ));
                
                return async.mset(batchData, DatacenterPreference.REMOTE_ONLY)
                    .thenApply(v -> batchData.size())
                    .join();
            });
            parallelBatches.add(batchFuture);
        }
        
        // Wait for all parallel batches to complete
        CompletableFuture.allOf(parallelBatches.toArray(new CompletableFuture[0]))
            .thenApply(v -> parallelBatches.stream()
                .mapToInt(CompletableFuture::join)
                .sum())
            .thenCompose(totalProcessed -> {
                long duration = System.currentTimeMillis() - startTime;
                logger.info("Parallel processing completed: {} records in {}ms", totalProcessed, duration);
                
                // Parallel verification
                List<CompletableFuture<Boolean>> verificationFutures = new ArrayList<>();
                for (int i = 1; i <= 10; i++) {
                    verificationFutures.add(async.exists("parallel:local:1:" + i));
                    verificationFutures.add(async.exists("parallel:remote:1:" + i));
                }
                
                return CompletableFuture.allOf(verificationFutures.toArray(new CompletableFuture[0]))
                    .thenApply(vv -> verificationFutures.stream()
                        .mapToLong(f -> f.join() ? 1 : 0)
                        .sum());
            })
            .thenAccept(verifiedCount -> {
                long totalDuration = System.currentTimeMillis() - startTime;
                logger.info("Parallel verification: {}/20 samples verified, total duration: {}ms", 
                    verifiedCount, totalDuration);
                latch.countDown();
            })
            .exceptionally(throwable -> {
                logger.error("Error in parallel batch processing", throwable);
                latch.countDown();
                return null;
            });
        
        await(latch);
    }
    
    private void demonstrateDataConsistencyOperations(MultiDatacenterRedisClient client) {
        logger.info("=== Data Consistency Operations ===");
        
        SyncOperations sync = client.sync();
        
        // Demonstrate eventual consistency scenario
        String consistencyKey = "consistency:test";
        Map<String, String> dataSet = new HashMap<>();
        dataSet.put(consistencyKey + ":1", "consistent_value_1");
        dataSet.put(consistencyKey + ":2", "consistent_value_2");
        dataSet.put(consistencyKey + ":3", "consistent_value_3");
        
        // Write to all datacenters
        sync.mset(dataSet, DatacenterPreference.LOCAL_ONLY);
        sync.mset(dataSet, DatacenterPreference.REMOTE_ONLY);
        logger.info("Data written to all datacenters");
        
        // Read from different datacenters to verify consistency
        List<String> localValues = sync.mget(DatacenterPreference.LOCAL_ONLY, 
            consistencyKey + ":1", consistencyKey + ":2", consistencyKey + ":3");
        List<String> remoteValues = sync.mget(DatacenterPreference.REMOTE_ONLY, 
            consistencyKey + ":1", consistencyKey + ":2", consistencyKey + ":3");
        
        boolean isConsistent = Objects.equals(localValues, remoteValues);
        logger.info("Data consistency check: {} (local: {}, remote: {})", 
            isConsistent ? "CONSISTENT" : "INCONSISTENT", localValues, remoteValues);
        
        // Demonstrate conflict resolution
        String conflictKey = "consistency:conflict";
        sync.set(conflictKey, "local_version", DatacenterPreference.LOCAL_ONLY);
        sync.set(conflictKey, "remote_version", DatacenterPreference.REMOTE_ONLY);
        
        String localVersion = sync.get(conflictKey, DatacenterPreference.LOCAL_ONLY);
        String remoteVersion = sync.get(conflictKey, DatacenterPreference.REMOTE_ONLY);
        
        if (!Objects.equals(localVersion, remoteVersion)) {
            logger.info("Conflict detected - Local: '{}', Remote: '{}'", localVersion, remoteVersion);
            
            // Resolve conflict using timestamp-based resolution
            String resolvedVersion = "resolved_" + System.currentTimeMillis();
            sync.set(conflictKey, resolvedVersion, DatacenterPreference.LOCAL_ONLY);
            sync.set(conflictKey, resolvedVersion, DatacenterPreference.REMOTE_ONLY);
            logger.info("Conflict resolved with version: '{}'", resolvedVersion);
        }
    }
    
    private void demonstrateBatchErrorHandling(MultiDatacenterRedisClient client) {
        logger.info("=== Batch Error Handling ===");
        
        AsyncOperations async = client.async();
        CountDownLatch latch = new CountDownLatch(1);
        
        // Simulate batch operations with potential failures
        List<CompletableFuture<String>> operationFutures = new ArrayList<>();
        
        for (int i = 1; i <= 20; i++) {
            final int id = i;
            CompletableFuture<String> future = async.set("error:test:" + id, "value_" + id)
                .thenCompose(v -> async.get("error:test:" + id))
                .handle((result, throwable) -> {
                    if (throwable != null) {
                        logger.warn("Operation {} failed: {}", id, throwable.getMessage());
                        return "FAILED";
                    } else {
                        return result != null ? result : "NULL";
                    }
                });
            operationFutures.add(future);
        }
        
        // Collect results and handle errors gracefully
        CompletableFuture.allOf(operationFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<String> results = operationFutures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());
                
                long successCount = results.stream().filter(r -> !r.equals("FAILED") && !r.equals("NULL")).count();
                long failureCount = results.stream().filter(r -> r.equals("FAILED")).count();
                
                logger.info("Batch error handling results: {} successful, {} failed", successCount, failureCount);
                return results;
            })
            .thenCompose(results -> {
                // Retry failed operations
                if (results.contains("FAILED")) {
                    logger.info("Retrying failed operations...");
                    
                    List<CompletableFuture<Void>> retryFutures = new ArrayList<>();
                    for (int i = 0; i < results.size(); i++) {
                        if (results.get(i).equals("FAILED")) {
                            final int id = i + 1;
                            retryFutures.add(async.set("error:retry:" + id, "retry_value_" + id));
                        }
                    }
                    
                    return CompletableFuture.allOf(retryFutures.toArray(new CompletableFuture[0]))
                        .thenApply(vv -> retryFutures.size());
                } else {
                    return CompletableFuture.completedFuture(0);
                }
            })
            .thenAccept(retryCount -> {
                logger.info("Batch error handling completed, {} operations retried", retryCount);
                latch.countDown();
            })
            .exceptionally(throwable -> {
                logger.error("Error in batch error handling demonstration", throwable);
                latch.countDown();
                return null;
            });
        
        await(latch);
    }
    
    private String generateLargeValue(int index) {
        StringBuilder sb = new StringBuilder();
        sb.append("large_value_").append(index).append("_");
        for (int i = 0; i < 100; i++) {
            sb.append("data_").append(i).append("_");
        }
        sb.append(System.currentTimeMillis());
        return sb.toString();
    }
    
    private void await(CountDownLatch latch) {
        try {
            latch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for operation completion");
        }
    }
}
