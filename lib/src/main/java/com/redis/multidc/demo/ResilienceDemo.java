package com.redis.multidc.demo;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.ResilienceConfig;
import com.redis.multidc.impl.DefaultMultiDatacenterRedisClient;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.pool.ConnectionPoolConfig;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Demonstration of the Redis Multi-Datacenter Client with Resilience4j integration.
 * This demo shows how the client handles failures gracefully using circuit breakers,
 * retries, rate limiting, and other resilience patterns.
 */
public class ResilienceDemo {
    
    public static void main(String[] args) {
        System.out.println("🚀 Redis Multi-Datacenter Client - Resilience4j Integration Demo");
        System.out.println("================================================================\n");
        
        demonstrateResilienceConfiguration();
    }
    
    private static void demonstrateResilienceConfiguration() {
        System.out.println("📋 Configuring Multi-Datacenter Redis Client with Resilience4j");
        System.out.println("----------------------------------------------------------------");
        
        // Create a resilience configuration with circuit breaker and retry
        ResilienceConfig resilienceConfig = ResilienceConfig.builder()
                .enableBasicPatterns() // Circuit breaker + retry
                .circuitBreakerConfig(
                        50.0f, // 50% failure rate threshold
                        Duration.ofSeconds(30), // Wait 30 seconds in open state
                        10, // Sliding window size
                        5 // Minimum number of calls
                )
                .retryConfig(
                        3, // Max 3 retry attempts
                        Duration.ofMillis(500) // 500ms wait between retries
                )
                .build();
        
        // Configure datacenters (using non-existent endpoints to demonstrate resilience)
        DatacenterConfiguration config = DatacenterConfiguration.builder()
                .datacenters(List.of(
                        DatacenterEndpoint.builder()
                                .id("primary-dc")
                                .region("us-east")
                                .host("non-existent-redis-1.example.com") // Will fail to connect
                                .port(6379)
                                .priority(1)
                                .connectionPoolSize(10) // Use connection pooling
                                .build(),
                        DatacenterEndpoint.builder()
                                .id("backup-dc")
                                .region("us-west")
                                .host("non-existent-redis-2.example.com") // Will fail to connect
                                .port(6379)
                                .priority(2)
                                .poolConfig(ConnectionPoolConfig.builder()
                                    .resourceConstrained() // Use minimal resources for backup
                                    .build())
                                .build()
                ))
                .localDatacenter("primary-dc")
                .resilienceConfig(resilienceConfig)
                .connectionTimeout(Duration.ofSeconds(2))
                .requestTimeout(Duration.ofSeconds(1))
                .build();
        
        System.out.println("✅ Configuration created with:");
        System.out.println("   • Circuit Breaker: " + (resilienceConfig.isCircuitBreakerEnabled() ? "ENABLED" : "DISABLED"));
        System.out.println("   • Retry: " + (resilienceConfig.isRetryEnabled() ? "ENABLED" : "DISABLED"));
        System.out.println("   • Rate Limiter: " + (resilienceConfig.isRateLimiterEnabled() ? "ENABLED" : "DISABLED"));
        System.out.println("   • Bulkhead: " + (resilienceConfig.isBulkheadEnabled() ? "ENABLED" : "DISABLED"));
        System.out.println("   • Time Limiter: " + (resilienceConfig.isTimeLimiterEnabled() ? "ENABLED" : "DISABLED"));
        System.out.println("   • Connection Pooling: ENABLED (10 connections per datacenter)");
        System.out.println();
        
        try {
            System.out.println("🔧 Creating client with resilience configuration...");
            var client = new DefaultMultiDatacenterRedisClient(config);
            System.out.println("✅ Client created successfully!");
            
            System.out.println("\n🧪 Testing resilience patterns (connections will fail gracefully):");
            System.out.println("--------------------------------------------------------------------");
            
            demonstrateOperationResilience(client);
            demonstrateConnectionPoolMetrics(client);
            
            // Clean up
            client.close();
            System.out.println("\n✅ Demo completed successfully!");
            
        } catch (Exception e) {
            System.out.println("❌ Demo failed: " + e.getMessage());
            System.out.println("   This is expected when Redis endpoints are not available.");
            System.out.println("   The resilience patterns are configured and will work when endpoints are available.");
        }
    }
    
    private static void demonstrateOperationResilience(DefaultMultiDatacenterRedisClient client) {
        var sync = client.sync();
        var async = client.async();
        
        System.out.println("1. Synchronous operations with resilience:");
        try {
            // This will trigger circuit breaker and retries
            String result = sync.get("test-key", DatacenterPreference.LOCAL_PREFERRED);
            System.out.println("   ✅ Sync GET result: " + result);
        } catch (Exception e) {
            System.out.println("   ⚠️  Sync GET failed (protected by resilience): " + e.getClass().getSimpleName());
        }
        
        System.out.println("\n2. Asynchronous operations with resilience:");
        try {
            CompletableFuture<String> futureResult = async.get("test-key", DatacenterPreference.ANY_AVAILABLE);
            // Don't wait for completion in demo to avoid long timeouts
            System.out.println("   ✅ Async GET submitted (protected by resilience patterns)");
        } catch (Exception e) {
            System.out.println("   ⚠️  Async GET failed (protected by resilience): " + e.getClass().getSimpleName());
        }
        
        System.out.println("\n3. Reactive operations with resilience:");
        try {
            var reactive = client.reactive();
            var mono = reactive.get("test-key");
            System.out.println("   ✅ Reactive GET created (protected by resilience patterns)");
        } catch (Exception e) {
            System.out.println("   ⚠️  Reactive GET failed (protected by resilience): " + e.getClass().getSimpleName());
        }
    }
    
    private static void demonstrateConnectionPoolMetrics(DefaultMultiDatacenterRedisClient client) {
        System.out.println("\n4. Connection Pool Status:");
        try {
            var aggregatedMetrics = client.getAggregatedPoolMetrics();
            System.out.println("   ✅ Pool metrics collected:");
            System.out.printf("   • Total pools: %d%n", aggregatedMetrics.getTotalPools());
            System.out.printf("   • Max connections: %d%n", aggregatedMetrics.getTotalMaxPoolSize());
            System.out.printf("   • Pool health: %s%n", client.areAllPoolsHealthy() ? "HEALTHY" : "DEGRADED");
            
            // Show individual pool metrics
            var config = client.getConfiguration();
            config.getDatacenters().forEach(endpoint -> {
                var poolMetrics = client.getConnectionPoolMetrics(endpoint.getId());
                if (poolMetrics != null) {
                    System.out.printf("   • %s: %d/%d connections (%.1f%% utilization)%n",
                        endpoint.getId(), 
                        poolMetrics.getActiveConnections(),
                        poolMetrics.getMaxPoolSize(),
                        poolMetrics.getUtilizationPercentage());
                }
            });
        } catch (Exception e) {
            System.out.println("   ⚠️  Pool metrics failed (expected with demo endpoints): " + e.getClass().getSimpleName());
        }
    }
}
