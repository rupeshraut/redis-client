package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.RoutingStrategy;
import com.redis.multidc.model.DatacenterPreference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Example demonstrating the multi-datacenter Redis client usage.
 */
public class MultiDatacenterExample {
    
    private static final Logger logger = LoggerFactory.getLogger(MultiDatacenterExample.class);
    
    public static void main(String[] args) {
        // Configure multiple datacenters
        DatacenterConfiguration configuration = DatacenterConfiguration.builder()
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .region("us-east")
                    .host("redis-us-east.example.com")
                    .port(6379)
                    .priority(1)
                    .weight(1.0)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("us-west-1")
                    .region("us-west")
                    .host("redis-us-west.example.com")
                    .port(6379)
                    .priority(2)
                    .weight(0.8)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("eu-central-1")
                    .region("eu-central")
                    .host("redis-eu-central.example.com")
                    .port(6379)
                    .priority(3)
                    .weight(0.6)
                    .build()
            ))
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .healthCheckInterval(Duration.ofSeconds(30))
            .connectionTimeout(Duration.ofSeconds(5))
            .requestTimeout(Duration.ofSeconds(10))
            .maxRetries(3)
            .enableCircuitBreaker(true)
            .build();
        
        // Create the client
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(configuration)) {
            
            logger.info("Multi-datacenter Redis client created successfully");
            
            // Demonstrate sync operations
            demonstrateSyncOperations(client);
            
            // Demonstrate async operations
            demonstrateAsyncOperations(client);
            
            // Demonstrate reactive operations
            demonstrateReactiveOperations(client);
            
            // Demonstrate datacenter-specific operations
            demonstrateDatacenterOperations(client);
            
        } catch (Exception e) {
            logger.error("Error during example execution", e);
        }
    }
    
    private static void demonstrateSyncOperations(MultiDatacenterRedisClient client) {
        logger.info("=== Synchronous Operations Demo ===");
        
        // Basic string operations with local preference
        client.sync().set("user:123", "John Doe", DatacenterPreference.LOCAL_PREFERRED);
        String value = client.sync().get("user:123", DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Retrieved value: {}", value);
        
        // Operations with TTL
        client.sync().set("session:abc", "active", Duration.ofMinutes(30), DatacenterPreference.LOCAL_ONLY);
        
        // Hash operations
        client.sync().hset("user:123:profile", "name", "John Doe", DatacenterPreference.LOCAL_PREFERRED);
        client.sync().hset("user:123:profile", "email", "john@example.com", DatacenterPreference.LOCAL_PREFERRED);
        
        // Check if key exists
        boolean exists = client.sync().exists("user:123", DatacenterPreference.ANY_AVAILABLE);
        logger.info("Key exists: {}", exists);
        
        logger.info("Sync operations completed successfully");
    }
    
    private static void demonstrateAsyncOperations(MultiDatacenterRedisClient client) {
        logger.info("=== Asynchronous Operations Demo ===");
        
        // Async operations return CompletableFuture
        client.async().set("async:key", "async value")
            .thenCompose(v -> client.async().get("async:key"))
            .thenAccept(value -> logger.info("Async retrieved value: {}", value))
            .exceptionally(throwable -> {
                logger.error("Async operation failed", throwable);
                return null;
            })
            .join(); // Wait for completion in this example
        
        logger.info("Async operations completed successfully");
    }
    
    private static void demonstrateReactiveOperations(MultiDatacenterRedisClient client) {
        logger.info("=== Reactive Operations Demo ===");
        
        // Reactive operations return Mono/Flux
        client.reactive().set("reactive:key", "reactive value")
            .then(client.reactive().get("reactive:key"))
            .doOnNext(value -> logger.info("Reactive retrieved value: {}", value))
            .doOnError(error -> logger.error("Reactive operation failed", error))
            .block(); // Block for completion in this example
        
        logger.info("Reactive operations completed successfully");
    }
    
    private static void demonstrateDatacenterOperations(MultiDatacenterRedisClient client) {
        logger.info("=== Datacenter-Specific Operations Demo ===");
        
        // Get datacenter information
        var datacenters = client.getDatacenters();
        logger.info("Available datacenters: {}", datacenters.size());
        
        for (var datacenter : datacenters) {
            logger.info("Datacenter: {}", datacenter);
        }
        
        // Get local datacenter
        var localDatacenter = client.getLocalDatacenter();
        logger.info("Local datacenter: {}", localDatacenter);
        
        // Health check
        client.checkDatacenterHealth()
            .thenAccept(healthInfo -> {
                logger.info("Health check completed for {} datacenters", healthInfo.size());
                for (var info : healthInfo) {
                    logger.info("Datacenter {} health: {}", info.getId(), info.isHealthy());
                }
            })
            .join();
        
        // Subscribe to health changes
        var subscription = client.subscribeToHealthChanges((datacenter, healthy) -> 
            logger.info("Health change: {} is now {}", datacenter.getId(), healthy ? "healthy" : "unhealthy")
        );
        
        // Clean up subscription
        subscription.dispose();
        
        logger.info("Datacenter operations completed successfully");
    }
}
