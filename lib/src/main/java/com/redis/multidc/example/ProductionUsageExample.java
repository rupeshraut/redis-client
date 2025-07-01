package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.ResilienceConfig;
import com.redis.multidc.model.DatacenterPreference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Advanced example demonstrating production-ready patterns for the Redis Multi-Datacenter Client.
 * Shows resilience patterns, async operations, and monitoring integration.
 */
public class ProductionUsageExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ProductionUsageExample.class);
    
    public static void main(String[] args) throws Exception {
        
        logger.info("Starting Redis Multi-Datacenter Client Production Example");
        
        // 1. Create production-grade configuration
        DatacenterConfiguration config = createProductionConfiguration();
        
        // 2. Create multi-datacenter client
        MultiDatacenterRedisClient client = createProductionClient(config);
        
        // 3. Create thread pool for async operations
        ExecutorService executor = Executors.newFixedThreadPool(10);
        
        try {
            // 4. Demonstrate production patterns
            demonstrateAsyncOperations(client, executor);
            demonstrateResiliencePatterns(client);
            demonstrateMonitoring(client);
            
        } finally {
            // 5. Cleanup
            executor.shutdown();
            client.close();
        }
        
        logger.info("Production example completed successfully");
    }
    
    /**
     * Creates a production-grade datacenter configuration.
     */
    private static DatacenterConfiguration createProductionConfiguration() {
        return DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(Arrays.asList(
                // Primary datacenter
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .region("us-east")
                    .host("redis-primary.us-east-1.company.com")
                    .port(6380)
                    .ssl(true)
                    .password("${REDIS_PASSWORD}")
                    .priority(1)
                    .weight(1.0)
                    .build(),
                    
                // Secondary datacenter
                DatacenterEndpoint.builder()
                    .id("us-west-1")
                    .region("us-west")
                    .host("redis-secondary.us-west-1.company.com")
                    .port(6380)
                    .ssl(true)
                    .password("${REDIS_PASSWORD}")
                    .priority(2)
                    .weight(0.8)
                    .build(),
                    
                // Read replica
                DatacenterEndpoint.builder()
                    .id("us-east-1-replica")
                    .region("us-east")
                    .host("redis-replica.us-east-1.company.com")
                    .port(6380)
                    .ssl(true)
                    .password("${REDIS_PASSWORD}")
                    .priority(1)
                    .weight(0.6)
                    .readOnly(true)
                    .build()
            ))
            .connectionTimeout(Duration.ofSeconds(10))
            .requestTimeout(Duration.ofSeconds(5))
            .healthCheckInterval(Duration.ofSeconds(30))
            .resilienceConfig(ResilienceConfig.defaultConfig())
            .build();
    }
    
    /**
     * Creates the production multi-datacenter client.
     */
    private static MultiDatacenterRedisClient createProductionClient(DatacenterConfiguration config) {
        return MultiDatacenterRedisClientBuilder.create(config);
    }
    
    /**
     * Demonstrates asynchronous operations for high-throughput scenarios.
     */
    private static void demonstrateAsyncOperations(MultiDatacenterRedisClient client, ExecutorService executor) {
        
        logger.info("=== Async Operations Demonstration ===");
        
        // 1. Parallel writes to multiple datacenters
        logger.info("1. Testing parallel async writes...");
        
        CompletableFuture<Void> asyncWrites = CompletableFuture.allOf(
            client.async().set("user:1", "user-data-1", DatacenterPreference.LOCAL_PREFERRED),
            client.async().set("user:2", "user-data-2", DatacenterPreference.REMOTE_ONLY),
            client.async().set("user:3", "user-data-3", DatacenterPreference.LOCAL_PREFERRED)
        );
        
        asyncWrites.thenRun(() -> {
            logger.info("All async writes completed successfully");
        }).exceptionally(throwable -> {
            logger.error("Async write failed", throwable);
            return null;
        });
        
        try {
            asyncWrites.get(); // Wait for completion
        } catch (Exception e) {
            logger.error("Async operation failed", e);
        }
        
        // 2. Async batch operations
        logger.info("2. Testing async batch operations...");
        
        CompletableFuture<java.util.List<String>> batchGet = 
            client.async().mget(DatacenterPreference.LOCAL_PREFERRED, "user:1", "user:2", "user:3");
        
        batchGet.thenAccept(results -> {
            logger.info("Batch get completed: {} results", results.size());
            for (int i = 0; i < results.size(); i++) {
                logger.info("  Result {}: {}", i, results.get(i));
            }
        }).exceptionally(throwable -> {
            logger.error("Batch get failed", throwable);
            return null;
        });
        
        try {
            batchGet.get(); // Wait for completion
        } catch (Exception e) {
            logger.error("Batch operation failed", e);
        }
        
        // 3. Pipeline operations
        logger.info("3. Testing pipeline operations...");
        
        long startTime = System.currentTimeMillis();
        
        CompletableFuture<?>[] pipelinedOps = new CompletableFuture[50];
        for (int i = 0; i < 50; i++) {
            String key = "pipeline:test:" + i;
            pipelinedOps[i] = client.async().set(key, "value-" + i, DatacenterPreference.LOCAL_PREFERRED);
        }
        
        CompletableFuture<Void> allPipelined = CompletableFuture.allOf(pipelinedOps);
        
        try {
            allPipelined.get();
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Pipelined 50 operations in {}ms (avg: {:.2f}ms per op)", 
                duration, duration / 50.0);
        } catch (Exception e) {
            logger.error("Pipeline operations failed", e);
        }
        
        logger.info("=== Async operations completed ===");
    }
    
    /**
     * Demonstrates resilience patterns and failure handling.
     */
    private static void demonstrateResiliencePatterns(MultiDatacenterRedisClient client) {
        
        logger.info("=== Resilience Patterns Demonstration ===");
        
        // 1. Circuit breaker behavior
        logger.info("1. Testing circuit breaker patterns...");
        
        // Normal operations should work
        try {
            client.sync().set("resilience:test", "test-value", DatacenterPreference.LOCAL_PREFERRED);
            String value = client.sync().get("resilience:test", DatacenterPreference.LOCAL_PREFERRED);
            logger.info("Normal operation successful: {}", value);
        } catch (Exception e) {
            logger.warn("Operation failed (expected with test setup): {}", e.getMessage());
        }
        
        // 2. Retry behavior
        logger.info("2. Testing retry behavior...");
        
        try {
            // This might fail and retry depending on datacenter availability
            client.sync().set("retry:test", "retry-value", DatacenterPreference.REMOTE_ONLY);
            logger.info("Retry operation completed");
        } catch (Exception e) {
            logger.warn("Retry operation failed (expected with test setup): {}", e.getMessage());
        }
        
        // 3. Fallback behavior
        logger.info("3. Testing fallback behavior...");
        
        try {
            // Try LOCAL_ONLY first, then fall back to available datacenters
            String value = client.sync().get("fallback:test", DatacenterPreference.LOCAL_PREFERRED);
            logger.info("Fallback operation result: {}", value);
        } catch (Exception e) {
            logger.warn("Fallback operation failed: {}", e.getMessage());
        }
        
        logger.info("=== Resilience patterns completed ===");
    }
    
    /**
     * Demonstrates monitoring and observability patterns.
     */
    private static void demonstrateMonitoring(MultiDatacenterRedisClient client) {
        
        logger.info("=== Monitoring and Observability Demonstration ===");
        
        // 1. Health monitoring
        logger.info("1. Checking datacenter health...");
        
        java.util.List<com.redis.multidc.model.DatacenterInfo> datacenters = client.getDatacenters();
        for (com.redis.multidc.model.DatacenterInfo dc : datacenters) {
            logger.info("Datacenter: {} - Region: {} - Status: {} - Host: {}:{}",
                dc.getId(),
                dc.getRegion(),
                dc.isHealthy() ? "Healthy" : "Unhealthy",
                dc.getHost(),
                dc.getPort());
                
            logger.info("  Latency: {}ms", dc.getLatencyMs());
            logger.info("  Circuit Breaker: {}", dc.getCircuitBreakerState());
            logger.info("  Available: {}", dc.isAvailable());
            logger.info("  Active Connections: {}", dc.getActiveConnections());
        }
        
        // 2. Local datacenter info
        logger.info("2. Local datacenter information...");
        
        com.redis.multidc.model.DatacenterInfo localDc = client.getLocalDatacenter();
        logger.info("Local datacenter: {} in region {}", localDc.getId(), localDc.getRegion());
        
        // 3. Performance monitoring
        logger.info("3. Running performance monitoring test...");
        
        long startTime = System.currentTimeMillis();
        int operations = 10;
        
        for (int i = 0; i < operations; i++) {
            try {
                client.sync().set("perf:monitor:" + i, "monitor-value-" + i, DatacenterPreference.LOCAL_PREFERRED);
            } catch (Exception e) {
                logger.warn("Performance test operation {} failed: {}", i, e.getMessage());
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Completed {} operations in {}ms (avg: {:.2f}ms per operation)", 
            operations, duration, duration / (double) operations);
        
        // 4. Error rate monitoring
        logger.info("4. Testing error handling and monitoring...");
        
        int successCount = 0;
        int errorCount = 0;
        
        for (int i = 0; i < 5; i++) {
            try {
                client.sync().get("non-existent-key-" + i, DatacenterPreference.LOCAL_PREFERRED);
                successCount++;
            } catch (Exception e) {
                errorCount++;
                logger.debug("Expected error for non-existent key: {}", e.getMessage());
            }
        }
        
        logger.info("Error monitoring results - Success: {}, Errors: {}, Success rate: {:.1f}%",
            successCount, errorCount, (successCount / 5.0) * 100);
        
        logger.info("=== Monitoring demonstration completed ===");
    }
}
