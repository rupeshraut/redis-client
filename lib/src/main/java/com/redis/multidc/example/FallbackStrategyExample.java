package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.*;
import com.redis.multidc.model.DatacenterPreference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Comprehensive example demonstrating production-ready fallback strategies
 * for multi-datacenter Redis deployments. Shows various fallback behaviors
 * for handling datacenter failures and ensuring high availability.
 */
public class FallbackStrategyExample {
    
    private static final Logger logger = LoggerFactory.getLogger(FallbackStrategyExample.class);
    
    public static void main(String[] args) {
        // Configure multiple datacenters for demonstration
        List<DatacenterEndpoint> datacenters = List.of(
            DatacenterEndpoint.builder()
                .id("us-east-1")
                .region("us-east")
                .host("redis-us-east.example.com")
                .port(6379)
                .priority(1)
                .weight(0.6)
                .build(),
            DatacenterEndpoint.builder()
                .id("us-west-2")
                .region("us-west")
                .host("redis-us-west.example.com")
                .port(6379)
                .priority(2)
                .weight(0.3)
                .build(),
            DatacenterEndpoint.builder()
                .id("eu-central-1")
                .region("eu-central")
                .host("redis-eu.example.com")
                .port(6379)
                .priority(3)
                .weight(0.1)
                .build()
        );
        
        // Demonstrate different fallback strategies
        demonstrateNextAvailableFallback(datacenters);
        demonstrateTryAllFallback(datacenters);
        demonstrateLocalOnlyFallback(datacenters);
        demonstrateBestEffortFallback(datacenters);
        demonstrateQueueAndRetryFallback(datacenters);
        demonstrateCustomFallbackLogic(datacenters);
        demonstrateFailFastStrategy(datacenters);
    }
    
    /**
     * NEXT_AVAILABLE: Most common production strategy.
     * Falls back to the next available datacenter based on priority/latency.
     */
    private static void demonstrateNextAvailableFallback(List<DatacenterEndpoint> datacenters) {
        logger.info("=== NEXT_AVAILABLE Fallback Strategy ===");
        
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.NEXT_AVAILABLE)
            .fallbackTimeout(Duration.ofSeconds(5))
            .maxFallbackAttempts(3)
            .fallbackRetryDelay(Duration.ofMillis(100))
            .build();
        
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .datacenters(datacenters)
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .fallbackConfiguration(fallbackConfig)
            .healthCheckInterval(Duration.ofSeconds(30))
            .resilienceConfig(ResilienceConfig.defaultConfig())
            .build();
        
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            // Normal operation - will try local first, then fallback to next available
            client.sync().set("user:123", "John Doe", DatacenterPreference.LOCAL_PREFERRED);
            String value = client.sync().get("user:123", DatacenterPreference.LOCAL_PREFERRED);
            logger.info("Retrieved value with NEXT_AVAILABLE fallback: {}", value);
            
            // When local datacenter fails, automatically falls back to us-west-2 or eu-central-1
            try {
                String fallbackValue = client.sync().get("user:123", DatacenterPreference.LOCAL_ONLY);
                logger.info("Fallback successful: {}", fallbackValue);
            } catch (Exception e) {
                logger.info("Local-only failed as expected, fallback would activate: {}", e.getMessage());
            }
            
        } catch (Exception e) {
            logger.error("Error in NEXT_AVAILABLE example", e);
        }
    }
    
    /**
     * TRY_ALL: More resilient strategy that tries all datacenters.
     * Higher latency but better availability.
     */
    private static void demonstrateTryAllFallback(List<DatacenterEndpoint> datacenters) {
        logger.info("=== TRY_ALL Fallback Strategy ===");
        
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.TRY_ALL)
            .fallbackTimeout(Duration.ofSeconds(10))
            .maxFallbackAttempts(5)
            .fallbackRetryDelay(Duration.ofMillis(200))
            .fallbackDatacenterOrder(List.of("us-west-2", "eu-central-1", "us-east-1"))
            .build();
        
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .datacenters(datacenters)
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.PRIORITY_BASED)
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            // Will try all datacenters in the specified order until one succeeds
            client.sync().set("critical:data", "Mission Critical Information");
            
            // Even with failures, will keep trying other datacenters
            String criticalData = client.sync().get("critical:data", DatacenterPreference.ANY_AVAILABLE);
            logger.info("Retrieved critical data with TRY_ALL strategy: {}", criticalData);
            
        } catch (Exception e) {
            logger.error("Error in TRY_ALL example", e);
        }
    }
    
    /**
     * LOCAL_ONLY: Fallback to local datacenter only, ensuring data locality.
     */
    private static void demonstrateLocalOnlyFallback(List<DatacenterEndpoint> datacenters) {
        logger.info("=== LOCAL_ONLY Fallback Strategy ===");
        
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.LOCAL_ONLY)
            .fallbackTimeout(Duration.ofSeconds(3))
            .maxFallbackAttempts(2)
            .build();
        
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .datacenters(datacenters)
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            // Will only use local datacenter, even if remote ones are available
            client.sync().set("local:data", "Data that must stay local");
            
            String localData = client.sync().get("local:data", DatacenterPreference.ANY_AVAILABLE);
            logger.info("Retrieved local data with LOCAL_ONLY fallback: {}", localData);
            
        } catch (Exception e) {
            logger.error("Error in LOCAL_ONLY example", e);
        }
    }
    
    /**
     * BEST_EFFORT: Prioritizes availability over consistency.
     * May use stale data when necessary.
     */
    private static void demonstrateBestEffortFallback(List<DatacenterEndpoint> datacenters) {
        logger.info("=== BEST_EFFORT Fallback Strategy ===");
        
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.BEST_EFFORT)
            .fallbackTimeout(Duration.ofSeconds(2))
            .enableStaleReads(true)
            .staleReadTolerance(Duration.ofMinutes(5))
            .maxFallbackAttempts(1) // Quick fallback
            .build();
        
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .datacenters(datacenters)
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            // Set data that might become stale
            client.sync().set("cache:user:456", "User Profile Data");
            
            // Best effort read - will return stale data if fresh data unavailable
            String userProfile = client.sync().get("cache:user:456", DatacenterPreference.LOWEST_LATENCY);
            logger.info("Retrieved user profile with BEST_EFFORT strategy: {}", userProfile);
            
        } catch (Exception e) {
            logger.error("Error in BEST_EFFORT example", e);
        }
    }
    
    /**
     * QUEUE_AND_RETRY: Queues operations and retries when datacenters recover.
     */
    private static void demonstrateQueueAndRetryFallback(List<DatacenterEndpoint> datacenters) {
        logger.info("=== QUEUE_AND_RETRY Fallback Strategy ===");
        
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.QUEUE_AND_RETRY)
            .fallbackTimeout(Duration.ofSeconds(30))
            .maxFallbackAttempts(5)
            .fallbackRetryDelay(Duration.ofSeconds(2))
            .queueSize(1000)
            .queueTimeout(Duration.ofSeconds(5))
            .build();
        
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .datacenters(datacenters)
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.PRIORITY_BASED)
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            // Operations will be queued if datacenters are temporarily unavailable
            client.sync().set("queued:operation", "Will be retried when datacenter recovers");
            
            // Async operations work well with queue and retry
            CompletableFuture<String> futureResult = client.async().get("queued:operation");
            futureResult.thenAccept(result -> 
                logger.info("Queued operation completed: {}", result));
            
            // Wait a bit for async completion
            Thread.sleep(1000);
            
        } catch (Exception e) {
            logger.error("Error in QUEUE_AND_RETRY example", e);
        }
    }
    
    /**
     * CUSTOM: Demonstrates custom fallback logic implementation.
     */
    private static void demonstrateCustomFallbackLogic(List<DatacenterEndpoint> datacenters) {
        logger.info("=== CUSTOM Fallback Strategy ===");
        
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.CUSTOM)
            .fallbackTimeout(Duration.ofSeconds(5))
            .customFallbackLogic(() -> {
                // Custom logic: prefer EU datacenter during US business hours
                java.time.LocalTime now = java.time.LocalTime.now();
                if (now.getHour() >= 9 && now.getHour() <= 17) {
                    logger.info("Business hours detected, preferring EU datacenter");
                    return "eu-central-1";
                } else {
                    logger.info("Off hours, using US West datacenter");
                    return "us-west-2";
                }
            })
            .build();
        
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .datacenters(datacenters)
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            // Custom fallback logic will be applied when primary routing fails
            client.sync().set("custom:routed", "Data routed by custom logic");
            String customData = client.sync().get("custom:routed", DatacenterPreference.ANY_AVAILABLE);
            logger.info("Retrieved data with CUSTOM fallback: {}", customData);
            
        } catch (Exception e) {
            logger.error("Error in CUSTOM example", e);
        }
    }
    
    /**
     * FAIL_FAST: No fallback - fails immediately if preferred datacenter unavailable.
     */
    private static void demonstrateFailFastStrategy(List<DatacenterEndpoint> datacenters) {
        logger.info("=== FAIL_FAST Strategy ===");
        
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.FAIL_FAST)
            .build();
        
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .datacenters(datacenters)
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.LOCAL_ONLY)
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            // Will succeed if local datacenter is available
            client.sync().set("fail:fast", "No fallback allowed");
            
            try {
                // This might fail fast if local datacenter is unavailable
                String value = client.sync().get("fail:fast", DatacenterPreference.LOCAL_ONLY);
                logger.info("FAIL_FAST succeeded: {}", value);
            } catch (Exception e) {
                logger.info("FAIL_FAST failed as expected: {}", e.getMessage());
            }
            
        } catch (Exception e) {
            logger.error("Error in FAIL_FAST example", e);
        }
    }
}
