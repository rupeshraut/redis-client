package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.pool.ConnectionPoolMetrics;
import com.redis.multidc.pool.ConnectionPoolManager.AggregatedPoolMetrics;
import com.redis.multidc.pool.PoolEvent;
import com.redis.multidc.pool.PoolEventListener;
import reactor.core.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive example demonstrating connection pool management and monitoring
 * capabilities of the Redis Multi-Datacenter Client.
 * 
 * Features demonstrated:
 * - Connection pool metrics monitoring
 * - Pool health management
 * - Pool event handling
 * - Connection lifecycle management
 * - Pool maintenance operations
 * - Performance optimization
 * - Pool troubleshooting
 */
public class ConnectionPoolManagementExample {
    
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPoolManagementExample.class);
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
    private final AtomicInteger operationCount = new AtomicInteger(0);
    private final AtomicInteger poolEventCount = new AtomicInteger(0);
    
    public static void main(String[] args) {
        ConnectionPoolManagementExample example = new ConnectionPoolManagementExample();
        try {
            example.runExample();
        } finally {
            example.shutdown();
        }
    }
    
    public void runExample() {
        logger.info("Starting Connection Pool Management Example");
        
        // Configure datacenters with pool-specific settings
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
                    .port(6381) // This will likely fail for demonstration
                    .build()
            ))
            .build();
        
        // Create client
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            
            // Setup pool monitoring
            setupPoolMonitoring(client);
            
            // Demonstrate pool operations
            demonstratePoolMetrics(client);
            demonstratePoolHealthManagement(client);
            demonstratePoolMaintenance(client);
            demonstrateConnectionLifecycle(client);
            demonstratePoolStressTest(client);
            demonstratePoolRecovery(client);
            demonstratePoolOptimization(client);
            
            // Run monitoring period
            runMonitoringPeriod(client);
            
            // Report final statistics
            reportFinalStatistics(client);
            
            logger.info("Connection Pool Management Example completed successfully");
            
        } catch (Exception e) {
            logger.error("Error in connection pool management example", e);
        }
    }
    
    private void setupPoolMonitoring(MultiDatacenterRedisClient client) {
        logger.info("=== Setting up Pool Monitoring ===");
        
        // Subscribe to pool events  
        Disposable poolSubscription = client.subscribeToPoolEvents((datacenterId, event, details) -> {
            poolEventCount.incrementAndGet();
            
            // Handle different event types
            switch (event.toString()) {
                case "CONNECTION_CREATED":
                    logger.info("🟢 CONNECTION CREATED - DC: {}, Details: {}", datacenterId, details);
                    break;
                case "CONNECTION_DESTROYED":
                    logger.info("🔴 CONNECTION DESTROYED - DC: {}, Details: {}", datacenterId, details);
                    break;
                case "CONNECTION_BORROWED":
                    if (poolEventCount.get() % 20 == 0) {
                        logger.debug("📤 CONNECTION BORROWED - DC: {}, Details: {}", datacenterId, details);
                    }
                    break;
                case "CONNECTION_RETURNED":
                    if (poolEventCount.get() % 20 == 0) {
                        logger.debug("📥 CONNECTION RETURNED - DC: {}, Details: {}", datacenterId, details);
                    }
                    break;
                case "POOL_MAINTENANCE":
                    logger.info("🔧 POOL MAINTENANCE - DC: {}, Details: {}", datacenterId, details);
                    break;
                case "POOL_HEALTH_CHANGE":
                    if (details.contains("healthy")) {
                        logger.info("💚 POOL HEALTHY - DC: {}", datacenterId);
                    } else {
                        logger.warn("� POOL UNHEALTHY - DC: {}, Details: {}", datacenterId, details);
                    }
                    break;
                default:
                    logger.info("� POOL EVENT - DC: {}, Event: {}, Details: {}", datacenterId, event, details);
                    break;
            }
        });
        
        // Schedule periodic metrics reporting
        scheduler.scheduleAtFixedRate(() -> {
            try {
                reportPoolMetrics(client);
            } catch (Exception e) {
                logger.error("Error during scheduled pool metrics reporting", e);
            }
        }, 10, 15, TimeUnit.SECONDS);
        
        logger.info("Pool monitoring setup completed");
    }
    
    private void demonstratePoolMetrics(MultiDatacenterRedisClient client) {
        logger.info("=== Pool Metrics Demonstration ===");
        
        // Get aggregated metrics
        AggregatedPoolMetrics aggregated = client.getAggregatedPoolMetrics();
        logger.info("Aggregated Pool Metrics:");
        logger.info("  - Pool count: {}", aggregated.getTotalPools());
        logger.info("  - Active connections: {}", aggregated.getTotalActiveConnections());
        
        // Get individual pool metrics
        Map<String, ConnectionPoolMetrics> allMetrics = client.getAllConnectionPoolMetrics();
        logger.info("Individual Pool Metrics:");
        for (Map.Entry<String, ConnectionPoolMetrics> entry : allMetrics.entrySet()) {
            String datacenterId = entry.getKey();
            ConnectionPoolMetrics metrics = entry.getValue();
            boolean healthy = client.isConnectionPoolHealthy(datacenterId);
            
            logger.info("  Datacenter: {} ({})", datacenterId, healthy ? "HEALTHY" : "UNHEALTHY");
            logger.info("    - Active: {}, Idle: {}, Max: {}", 
                metrics.getActiveConnections(), metrics.getIdleConnections(), metrics.getMaxPoolSize());
            logger.info("    - Current pool size: {}", metrics.getCurrentPoolSize());
        }
    }
    
    private void demonstratePoolHealthManagement(MultiDatacenterRedisClient client) {
        logger.info("=== Pool Health Management ===");
        
        // Check overall health
        boolean allHealthy = client.areAllPoolsHealthy();
        logger.info("All pools healthy: {}", allHealthy);
        
        // Check individual pool health
        Map<String, Boolean> healthMap = client.getConnectionPoolHealth();
        logger.info("Individual pool health status:");
        healthMap.forEach((datacenterId, healthy) -> {
            String status = healthy ? "✅ HEALTHY" : "❌ UNHEALTHY";
            logger.info("  - {}: {}", datacenterId, status);
            
            if (!healthy) {
                // Get specific metrics for unhealthy pool
                ConnectionPoolMetrics metrics = client.getConnectionPoolMetrics(datacenterId);
                if (metrics != null) {
                    logger.info("    Health details: active={}, max={}", 
                        metrics.getActiveConnections(), metrics.getMaxPoolSize());
                }
            }
        });
        
        // Demonstrate health recovery actions
        if (!allHealthy) {
            logger.info("Triggering maintenance on unhealthy pools...");
            client.maintainAllConnectionPools();
            
            // Wait a bit and check again
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            boolean healthyAfterMaintenance = client.areAllPoolsHealthy();
            logger.info("Pools healthy after maintenance: {}", healthyAfterMaintenance);
        }
    }
    
    private void demonstratePoolMaintenance(MultiDatacenterRedisClient client) {
        logger.info("=== Pool Maintenance Operations ===");
        
        // Get pre-maintenance metrics
        AggregatedPoolMetrics preMetrics = client.getAggregatedPoolMetrics();
        logger.info("Pre-maintenance metrics: {} active connections", 
            preMetrics.getTotalActiveConnections());
        
        // Trigger maintenance
        logger.info("Triggering maintenance on all pools...");
        client.maintainAllConnectionPools();
        
        // Wait for maintenance to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Get post-maintenance metrics
        AggregatedPoolMetrics postMetrics = client.getAggregatedPoolMetrics();
        logger.info("Post-maintenance metrics: {} active connections", 
            postMetrics.getTotalActiveConnections());
        
        logger.info("Maintenance completed");
        
        // Reset metrics for clean measurement
        logger.info("Resetting pool metrics...");
        for (String datacenterId : client.getConnectionPoolHealth().keySet()) {
            client.resetConnectionPoolMetrics(datacenterId);
        }
    }
    
    private void demonstrateConnectionLifecycle(MultiDatacenterRedisClient client) {
        logger.info("=== Connection Lifecycle Demonstration ===");
        
        // Perform operations to trigger connection creation/usage
        for (int i = 1; i <= 10; i++) {
            String key = "lifecycle:test:" + i;
            String value = "value_" + i + "_" + System.currentTimeMillis();
            
            try {
                client.sync().set(key, value);
                String retrieved = client.sync().get(key);
                operationCount.incrementAndGet();
                
                if (i % 3 == 0) {
                    logger.debug("Operation {}: set and retrieved {}", i, key);
                }
            } catch (Exception e) {
                logger.warn("Operation {} failed: {}", i, e.getMessage());
            }
            
            // Small delay to observe connection behavior
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        logger.info("Completed {} operations for lifecycle demonstration", operationCount.get());
        
        // Check connection usage patterns
        reportConnectionUsagePatterns(client);
    }
    
    private void demonstratePoolStressTest(MultiDatacenterRedisClient client) {
        logger.info("=== Pool Stress Test ===");
        
        int numThreads = 15;
        int operationsPerThread = 20;
        CountDownLatch stressLatch = new CountDownLatch(numThreads);
        
        logger.info("Starting stress test with {} threads, {} operations each", numThreads, operationsPerThread);
        
        long startTime = System.currentTimeMillis();
        
        for (int thread = 1; thread <= numThreads; thread++) {
            final int threadId = thread;
            
            scheduler.submit(() -> {
                try {
                    for (int op = 1; op <= operationsPerThread; op++) {
                        String key = "stress:thread:" + threadId + ":op:" + op;
                        String value = "stress_value_" + threadId + "_" + op + "_" + System.currentTimeMillis();
                        
                        try {
                            client.sync().set(key, value);
                            client.sync().get(key);
                            operationCount.incrementAndGet();
                        } catch (Exception e) {
                            logger.debug("Stress operation failed (thread {}, op {}): {}", threadId, op, e.getMessage());
                        }
                        
                        // Small delay to vary timing
                        Thread.sleep(50 + (int)(Math.random() * 100));
                    }
                } catch (Exception e) {
                    logger.error("Stress test thread {} failed", threadId, e);
                } finally {
                    stressLatch.countDown();
                }
            });
        }
        
        // Wait for stress test to complete
        try {
            stressLatch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Stress test interrupted");
        }
        
        long duration = System.currentTimeMillis() - startTime;
        int totalOperations = numThreads * operationsPerThread;
        double opsPerSecond = (double) totalOperations / duration * 1000;
        
        logger.info("Stress test completed: {} operations in {}ms ({:.1f} ops/sec)", 
            totalOperations, duration, opsPerSecond);
        
        // Report pool state after stress test
        reportPoolStateAfterStress(client);
    }
    
    private void demonstratePoolRecovery(MultiDatacenterRedisClient client) {
        logger.info("=== Pool Recovery Demonstration ===");
        
        // Find a datacenter to drain for recovery test
        String targetDatacenter = client.getConnectionPoolHealth().keySet().iterator().next();
        
        logger.info("Testing recovery scenario with datacenter: {}", targetDatacenter);
        
        // Get pre-drain metrics
        ConnectionPoolMetrics preMetrics = client.getConnectionPoolMetrics(targetDatacenter);
        logger.info("Pre-drain metrics: {} active, {} idle", 
            preMetrics.getActiveConnections(), preMetrics.getIdleConnections());
        
        // Drain the pool
        logger.info("Draining pool for datacenter: {}", targetDatacenter);
        client.drainConnectionPool(targetDatacenter);
        
        // Wait for drain to complete
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Check post-drain state
        ConnectionPoolMetrics postDrainMetrics = client.getConnectionPoolMetrics(targetDatacenter);
        logger.info("Post-drain metrics: {} active, {} idle", 
            postDrainMetrics.getActiveConnections(), postDrainMetrics.getIdleConnections());
        
        // Perform operations to trigger recovery
        logger.info("Performing operations to trigger pool recovery...");
        for (int i = 1; i <= 5; i++) {
            try {
                String key = "recovery:test:" + i;
                client.sync().set(key, "recovery_value_" + i);
                operationCount.incrementAndGet();
            } catch (Exception e) {
                logger.debug("Recovery operation {} failed (expected during recovery): {}", i, e.getMessage());
            }
            
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        // Check recovery state
        ConnectionPoolMetrics recoveryMetrics = client.getConnectionPoolMetrics(targetDatacenter);
        logger.info("Recovery metrics: {} active, {} idle", 
            recoveryMetrics.getActiveConnections(), recoveryMetrics.getIdleConnections());
        
        boolean recovered = client.isConnectionPoolHealthy(targetDatacenter);
        logger.info("Pool recovery status: {}", recovered ? "✅ RECOVERED" : "❌ STILL_UNHEALTHY");
    }
    
    private void demonstratePoolOptimization(MultiDatacenterRedisClient client) {
        logger.info("=== Pool Optimization Demonstration ===");
        
        // Analyze current pool configuration vs usage
        Map<String, ConnectionPoolMetrics> allMetrics = client.getAllConnectionPoolMetrics();
        
        for (Map.Entry<String, ConnectionPoolMetrics> entry : allMetrics.entrySet()) {
            String datacenterId = entry.getKey();
            ConnectionPoolMetrics metrics = entry.getValue();
            
            logger.info("Pool optimization analysis for {}:", datacenterId);
            logger.info("  - Active connections: {}", metrics.getActiveConnections());
            logger.info("  - Max pool size: {}", metrics.getMaxPoolSize());
            
            // Provide optimization recommendations
            double utilization = (double) metrics.getActiveConnections() / metrics.getMaxPoolSize();
            if (utilization > 0.8) {
                logger.info("  🔧 RECOMMENDATION: Consider increasing max pool size (high utilization)");
            } else if (utilization < 0.2 && metrics.getIdleConnections() > 5) {
                logger.info("  🔧 RECOMMENDATION: Consider reducing pool size (low utilization)");
            } else {
                logger.info("  ✅ Pool configuration appears optimal");
            }
        }
    }
    
    private void runMonitoringPeriod(MultiDatacenterRedisClient client) {
        logger.info("=== Running Extended Monitoring Period (30 seconds) ===");
        
        CountDownLatch monitoringLatch = new CountDownLatch(1);
        
        // Generate varied workload during monitoring
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Simulate different operation patterns
                for (int i = 1; i <= 5; i++) {
                    String key = "monitoring:pattern:" + i + ":" + System.currentTimeMillis();
                    client.async().set(key, "monitoring_value_" + i)
                        .thenCompose(v -> client.async().get(key))
                        .thenAccept(value -> operationCount.incrementAndGet());
                }
            } catch (Exception e) {
                logger.debug("Monitoring operation failed", e);
            }
        }, 1, 2, TimeUnit.SECONDS);
        
        // End monitoring after 30 seconds
        scheduler.schedule(() -> monitoringLatch.countDown(), 30, TimeUnit.SECONDS);
        
        try {
            monitoringLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Monitoring period interrupted");
        }
        
        logger.info("Extended monitoring period completed");
    }
    
    private void reportPoolMetrics(MultiDatacenterRedisClient client) {
        AggregatedPoolMetrics metrics = client.getAggregatedPoolMetrics();
        logger.info("📊 POOL METRICS: {} pools, {} active connections", 
            metrics.getTotalPools(),
            metrics.getTotalActiveConnections());
    }
    
    private void reportConnectionUsagePatterns(MultiDatacenterRedisClient client) {
        logger.info("Connection Usage Patterns:");
        Map<String, ConnectionPoolMetrics> allMetrics = client.getAllConnectionPoolMetrics();
        
        for (Map.Entry<String, ConnectionPoolMetrics> entry : allMetrics.entrySet()) {
            ConnectionPoolMetrics metrics = entry.getValue();
            logger.info("  {}: max={}, active={}", 
                entry.getKey(), metrics.getMaxPoolSize(),
                metrics.getActiveConnections());
        }
    }
    
    private void reportPoolStateAfterStress(MultiDatacenterRedisClient client) {
        logger.info("Pool State After Stress Test:");
        AggregatedPoolMetrics metrics = client.getAggregatedPoolMetrics();
        
        logger.info("  - Active connections: {}", metrics.getTotalActiveConnections());
        logger.info("  - Pool count: {}", metrics.getTotalPools());
    }
    
    private void reportFinalStatistics(MultiDatacenterRedisClient client) {
        logger.info("=== Final Statistics ===");
        logger.info("Total operations performed: {}", operationCount.get());
        logger.info("Total pool events received: {}", poolEventCount.get());
        
        AggregatedPoolMetrics finalMetrics = client.getAggregatedPoolMetrics();
        logger.info("Final pool state:");
        logger.info("  - Pool count: {}", finalMetrics.getTotalPools());
        logger.info("  - Active connections: {}", finalMetrics.getTotalActiveConnections());
    }
    
    public void shutdown() {
        logger.info("Shutting down connection pool management example");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
