package com.redis.multidc.demo;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.ResilienceConfig;
import com.redis.multidc.impl.DefaultMultiDatacenterRedisClient;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.pool.ConnectionPoolConfig;
import com.redis.multidc.pool.ConnectionPoolManager.AggregatedPoolMetrics;
import com.redis.multidc.pool.ConnectionPoolMetrics;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Production-ready demonstration of the Redis Multi-Datacenter Client with connection pooling.
 * This demo showcases enterprise-grade features including connection pooling, metrics,
 * health monitoring, and high-performance patterns.
 */
public class ProductionConnectionPoolDemo {
    
    public static void main(String[] args) {
        System.out.println("🚀 Redis Multi-Datacenter Client - Production Connection Pool Demo");
        System.out.println("===================================================================\n");
        
        demonstrateProductionConfiguration();
    }
    
    private static void demonstrateProductionConfiguration() {
        System.out.println("📋 Creating Production-Ready Multi-Datacenter Configuration");
        System.out.println("----------------------------------------------------------");
        
        // Create production-grade resilience configuration
        ResilienceConfig resilienceConfig = ResilienceConfig.builder()
                .enableAllPatterns() // All resilience patterns
                .circuitBreakerConfig(
                        30.0f, // 30% failure rate threshold (production setting)
                        Duration.ofSeconds(60), // Wait 60 seconds in open state
                        100, // Larger sliding window for production
                        10   // Minimum calls before evaluation
                )
                .retryConfig(
                        5, // Up to 5 retries for production
                        Duration.ofMillis(200) // 200ms between retries
                )
                .rateLimiterConfig(
                        1000, // 1000 requests per second limit
                        Duration.ofSeconds(1), // refresh period
                        Duration.ofMillis(100) // timeout for acquiring permits
                )
                .bulkheadConfig(50, Duration.ofMillis(100)) // 50 concurrent operations max, 100ms wait
                .timeLimiterConfig(Duration.ofSeconds(10), true) // 10 second timeout, cancel running futures
                .build();
        
        // Create high-performance connection pool configurations
        ConnectionPoolConfig primaryPoolConfig = ConnectionPoolConfig.builder()
                .lowLatency() // Optimized for low latency
                .maxPoolSize(100) // Large pool for primary datacenter
                .minPoolSize(20)
                .acquisitionTimeout(Duration.ofSeconds(2))
                .idleTimeout(Duration.ofMinutes(5))
                .validateOnAcquire(true)
                .build();
        
        ConnectionPoolConfig replicaPoolConfig = ConnectionPoolConfig.builder()
                .highThroughput() // Optimized for high throughput
                .maxPoolSize(50) // Smaller pool for replicas
                .minPoolSize(10)
                .build();
        
        ConnectionPoolConfig backupPoolConfig = ConnectionPoolConfig.builder()
                .resourceConstrained() // Minimal resources for backup
                .maxPoolSize(10)
                .minPoolSize(2)
                .build();
        
        // Configure datacenters with production-grade settings
        DatacenterConfiguration config = DatacenterConfiguration.builder()
                .datacenters(List.of(
                        // Primary datacenter - optimized for low latency
                        DatacenterEndpoint.builder()
                                .id("primary-us-east-1")
                                .region("us-east-1")
                                .host("redis-primary.us-east-1.example.com")
                                .port(6380) // SSL port
                                .ssl(true)
                                .priority(1)
                                .weight(1.0)
                                .poolConfig(primaryPoolConfig)
                                .build(),
                        
                        // Read replica - optimized for throughput
                        DatacenterEndpoint.builder()
                                .id("replica-us-east-1a")
                                .region("us-east-1")
                                .host("redis-replica-1a.us-east-1.example.com")
                                .port(6380)
                                .ssl(true)
                                .priority(1)
                                .weight(0.8)
                                .readOnly(true)
                                .poolConfig(replicaPoolConfig)
                                .build(),
                        
                        // Secondary datacenter
                        DatacenterEndpoint.builder()
                                .id("secondary-us-west-2")
                                .region("us-west-2")
                                .host("redis-secondary.us-west-2.example.com")
                                .port(6380)
                                .ssl(true)
                                .priority(2)
                                .weight(0.9)
                                .poolConfig(replicaPoolConfig)
                                .build(),
                        
                        // Backup datacenter - minimal resources
                        DatacenterEndpoint.builder()
                                .id("backup-eu-west-1")
                                .region("eu-west-1")
                                .host("redis-backup.eu-west-1.example.com")
                                .port(6380)
                                .ssl(true)
                                .priority(3)
                                .weight(0.5)
                                .poolConfig(backupPoolConfig)
                                .build()
                ))
                .localDatacenter("primary-us-east-1")
                .resilienceConfig(resilienceConfig)
                .connectionTimeout(Duration.ofSeconds(5))
                .requestTimeout(Duration.ofSeconds(10))
                .healthCheckInterval(Duration.ofSeconds(30))
                .maxRetries(3)
                .retryDelay(Duration.ofMillis(100))
                .build();
        
        printConfigurationSummary(config);
        
        try {
            System.out.println("🔧 Creating production client with connection pooling...");
            var client = new DefaultMultiDatacenterRedisClient(config);
            System.out.println("✅ Client created successfully!");
            
            // Demonstrate production features
            demonstrateConnectionPoolMetrics(client);
            demonstrateHighLoadOperations(client);
            demonstrateHealthMonitoring(client);
            
            // Clean up
            client.close();
            System.out.println("\n✅ Production demo completed successfully!");
            
        } catch (Exception e) {
            System.out.println("❌ Demo failed: " + e.getMessage());
            System.out.println("   Note: This demo requires actual Redis endpoints to showcase");
            System.out.println("   connection pooling and production features.");
            e.printStackTrace();
        }
    }
    
    private static void printConfigurationSummary(DatacenterConfiguration config) {
        System.out.println("\n📊 Production Configuration Summary:");
        System.out.println("=====================================");
        
        config.getDatacenters().forEach(endpoint -> {
            ConnectionPoolConfig poolConfig = endpoint.getPoolConfig();
            System.out.printf("🏢 Datacenter: %s (%s)%n", endpoint.getId(), endpoint.getRegion());
            System.out.printf("   📡 Endpoint: %s:%d (SSL: %s)%n", 
                endpoint.getHost(), endpoint.getPort(), endpoint.isSsl());
            System.out.printf("   🔄 Pool: %d-%d connections (acquisition timeout: %s)%n",
                poolConfig.getMinPoolSize(), poolConfig.getMaxPoolSize(), 
                poolConfig.getAcquisitionTimeout());
            System.out.printf("   ⚖️  Priority: %d, Weight: %.1f, ReadOnly: %s%n%n",
                endpoint.getPriority(), endpoint.getWeight(), endpoint.isReadOnly());
        });
        
        ResilienceConfig resilience = config.getResilienceConfig();
        System.out.printf("🛡️  Resilience: CB(%s) | Retry(%s) | RateLimit(%s) | Bulkhead(%s) | TimeLimit(%s)%n%n",
            resilience.isCircuitBreakerEnabled() ? "✓" : "✗",
            resilience.isRetryEnabled() ? "✓" : "✗",
            resilience.isRateLimiterEnabled() ? "✓" : "✗",
            resilience.isBulkheadEnabled() ? "✓" : "✗",
            resilience.isTimeLimiterEnabled() ? "✓" : "✗");
    }
    
    private static void demonstrateConnectionPoolMetrics(DefaultMultiDatacenterRedisClient client) {
        System.out.println("📈 Connection Pool Metrics Demonstration");
        System.out.println("=========================================");
        
        // Get metrics for each datacenter
        var config = client.getConfiguration();
        config.getDatacenters().forEach(endpoint -> {
            ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics(endpoint.getId());
            if (metrics != null) {
                System.out.printf("🏢 %s Pool Metrics:%n", endpoint.getId());
                System.out.printf("   📊 Connections: %d active / %d idle / %d max%n",
                    metrics.getActiveConnections(), metrics.getIdleConnections(), metrics.getMaxPoolSize());
                System.out.printf("   ⚡ Utilization: %.1f%% | Efficiency: %.3f%n",
                    metrics.getUtilizationPercentage(), metrics.getEfficiencyRatio());
                System.out.printf("   📈 Created: %d | Acquired: %d | Timeouts: %d%n%n",
                    metrics.getTotalConnectionsCreated(), 
                    metrics.getTotalConnectionsAcquired(),
                    metrics.getTotalAcquisitionTimeouts());
            }
        });
        
        // Get aggregated metrics
        AggregatedPoolMetrics aggregated = client.getAggregatedPoolMetrics();
        System.out.printf("🌐 Aggregated Metrics: %d pools, %d/%d active connections, %.1f%% avg utilization%n%n",
            aggregated.getTotalPools(), aggregated.getTotalActiveConnections(), 
            aggregated.getTotalMaxPoolSize(), aggregated.getAverageUtilization());
    }
    
    private static void demonstrateHighLoadOperations(DefaultMultiDatacenterRedisClient client) {
        System.out.println("🚀 High-Load Operations Demonstration");
        System.out.println("=====================================");
        
        var sync = client.sync();
        ExecutorService executor = Executors.newFixedThreadPool(20);
        
        try {
            System.out.println("⚡ Executing 1000 concurrent operations across pools...");
            
            long startTime = System.currentTimeMillis();
            
            // Submit 1000 concurrent operations
            List<CompletableFuture<Void>> futures = IntStream.range(0, 1000)
                .mapToObj(i -> CompletableFuture.runAsync(() -> {
                    try {
                        String key = "load-test-key-" + i;
                        String value = "value-" + i + "-" + System.currentTimeMillis();
                        
                        // Mix of read and write operations
                        if (i % 3 == 0) {
                            sync.set(key, value, DatacenterPreference.LOCAL_PREFERRED);
                        } else {
                            sync.get(key, DatacenterPreference.ANY_AVAILABLE);
                        }
                    } catch (Exception e) {
                        // Expected for non-existent endpoints
                        System.out.printf("⚠️  Operation %d failed: %s%n", i, e.getMessage());
                    }
                }, executor))
                .toList();
            
            // Wait for completion
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);
            
            long duration = System.currentTimeMillis() - startTime;
            System.out.printf("✅ Completed 1000 operations in %dms (%.1f ops/sec)%n%n", 
                duration, 1000.0 / duration * 1000);
            
            // Show updated metrics
            AggregatedPoolMetrics metrics = client.getAggregatedPoolMetrics();
            System.out.printf("📊 Post-load metrics: %d total acquisitions, %d timeouts%n%n",
                metrics.getTotalConnectionsAcquired(), metrics.getTotalAcquisitionTimeouts());
                
        } catch (Exception e) {
            System.out.println("⚠️  High-load test failed (expected with demo endpoints): " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }
    
    private static void demonstrateHealthMonitoring(DefaultMultiDatacenterRedisClient client) {
        System.out.println("💓 Health Monitoring Demonstration");
        System.out.println("==================================");
        
        // Check pool health
        boolean allHealthy = client.areAllPoolsHealthy();
        System.out.printf("🏥 All pools healthy: %s%n", allHealthy ? "✅ YES" : "❌ NO");
        
        // Force maintenance
        System.out.println("🔧 Forcing connection pool maintenance...");
        client.maintainAllConnectionPools();
        System.out.println("✅ Maintenance completed");
        
        // Subscribe to health changes
        System.out.println("👂 Subscribing to health changes...");
        var subscription = client.subscribeToHealthChanges((datacenter, healthy) -> {
            System.out.printf("💓 Health change: %s is now %s%n", 
                datacenter.getId(), healthy ? "HEALTHY" : "UNHEALTHY");
        });
        
        // Simulate some time for potential health changes
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        subscription.dispose();
        System.out.println("✅ Health monitoring demo completed\n");
    }
}
