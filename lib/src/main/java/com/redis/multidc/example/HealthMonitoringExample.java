package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.pool.ConnectionPoolMetrics;
import com.redis.multidc.pool.ConnectionPoolManager.AggregatedPoolMetrics;
import com.redis.multidc.pool.PoolEvent;
import com.redis.multidc.pool.PoolEventListener;
import reactor.core.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive example demonstrating health monitoring and event subscription
 * capabilities of the Redis Multi-Datacenter Client.
 * 
 * Features demonstrated:
 * - Datacenter health monitoring
 * - Health change event subscription
 * - Connection pool metrics monitoring
 * - Pool event subscription
 * - Real-time health alerting
 * - Automated health checks
 * - Health status reporting
 */
public class HealthMonitoringExample {
    
    private static final Logger logger = LoggerFactory.getLogger(HealthMonitoringExample.class);
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final AtomicInteger healthChangeCount = new AtomicInteger(0);
    private final AtomicInteger poolEventCount = new AtomicInteger(0);
    
    public static void main(String[] args) {
        HealthMonitoringExample example = new HealthMonitoringExample();
        try {
            example.runExample();
        } finally {
            example.shutdown();
        }
    }
    
    public void runExample() {
        logger.info("Starting Health Monitoring Example");
        
        // Configure datacenters with health check settings
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
            
            // Setup health monitoring
            setupHealthMonitoring(client);
            
            // Setup connection pool monitoring
            setupConnectionPoolMonitoring(client);
            
            // Demonstrate health operations
            demonstrateHealthOperations(client);
            
            // Demonstrate connection pool management
            demonstrateConnectionPoolManagement(client);
            
            // Run monitoring for a period
            runMonitoringPeriod(client);
            
            logger.info("Health Monitoring Example completed successfully");
            
        } catch (Exception e) {
            logger.error("Error in health monitoring example", e);
        }
    }
    
    private void setupHealthMonitoring(MultiDatacenterRedisClient client) {
        logger.info("=== Setting up Health Monitoring ===");
        
        // Subscribe to health change events
        Disposable healthSubscription = client.subscribeToHealthChanges((datacenter, healthy) -> {
            int changeCount = healthChangeCount.incrementAndGet();
            
            if (healthy) {
                logger.info("🟢 HEALTH RESTORED - Datacenter {} is now healthy (change #{})", 
                    datacenter.getId(), changeCount);
            } else {
                logger.error("🔴 HEALTH DEGRADED - Datacenter {} is now unhealthy (change #{})", 
                    datacenter.getId(), changeCount);
            }
            
            // Log additional datacenter info
            logger.info("   - Datacenter ID: {}", datacenter.getId());
            logger.info("   - Is healthy: {}", datacenter.isHealthy());
            
            // Trigger alerting logic (in real implementation)
            if (!healthy) {
                triggerHealthAlert(datacenter);
            }
        });
        
        // Schedule health check reporting
        scheduler.scheduleAtFixedRate(() -> {
            try {
                reportHealthStatus(client);
            } catch (Exception e) {
                logger.error("Error during health status reporting", e);
            }
        }, 10, 15, TimeUnit.SECONDS);
        
        logger.info("Health monitoring setup completed");
    }
    
    private void setupConnectionPoolMonitoring(MultiDatacenterRedisClient client) {
        logger.info("=== Setting up Connection Pool Monitoring ===");
        
        // Subscribe to pool events
        Disposable poolSubscription = client.subscribeToPoolEvents((datacenterId, event, details) -> {
            poolEventCount.incrementAndGet();
            
            // Handle different event types
            switch (event.toString()) {
                case "CONNECTION_CREATED":
                    logger.info("📈 CONNECTION CREATED - Datacenter: {}, Details: {}", datacenterId, details);
                    break;
                case "CONNECTION_DESTROYED":
                    logger.info("📉 CONNECTION DESTROYED - Datacenter: {}, Details: {}", datacenterId, details);
                    break;
                case "CONNECTION_BORROWED":
                    if (poolEventCount.get() % 10 == 0) {
                        logger.debug("📤 CONNECTION BORROWED - Datacenter: {}, Details: {}", datacenterId, details);
                    }
                    break;
                case "CONNECTION_RETURNED":
                    if (poolEventCount.get() % 10 == 0) {
                        logger.debug("📥 CONNECTION RETURNED - Datacenter: {}, Details: {}", datacenterId, details);
                    }
                    break;
                case "POOL_MAINTENANCE":
                    logger.info("🔧 POOL MAINTENANCE - Datacenter: {}, Details: {}", datacenterId, details);
                    break;
                case "POOL_HEALTH_CHANGE":
                    if (details.contains("healthy")) {
                        logger.info("💚 POOL HEALTHY - Datacenter: {}", datacenterId);
                    } else {
                        logger.warn("💔 POOL UNHEALTHY - Datacenter: {}, Details: {}", datacenterId, details);
                    }
                    break;            default:
                logger.info("📊 POOL EVENT - Datacenter: {}, Event: {}, Details: {}", datacenterId, event, details);
                break;
            }
        });
        
        // Schedule pool metrics reporting
        scheduler.scheduleAtFixedRate(() -> {
            try {
                reportPoolMetrics(client);
            } catch (Exception e) {
                logger.error("Error during pool metrics reporting", e);
            }
        }, 5, 20, TimeUnit.SECONDS);
        
        logger.info("Connection pool monitoring setup completed");
    }
    
    private void demonstrateHealthOperations(MultiDatacenterRedisClient client) {
        logger.info("=== Demonstrating Health Operations ===");
        
        // Check current health status
        logger.info("Current datacenters:");
        for (DatacenterInfo datacenter : client.getDatacenters()) {
            logger.info("  - {}: healthy={}", 
                datacenter.getId(), datacenter.isHealthy());
        }
        
        // Get local datacenter info
        DatacenterInfo localDc = client.getLocalDatacenter();
        logger.info("Local datacenter: {} (healthy: {})", localDc.getId(), localDc.isHealthy());
        
        // Force health check
        client.checkDatacenterHealth()
            .thenAccept(datacenters -> {
                logger.info("Health check completed for {} datacenters", datacenters.size());
                for (DatacenterInfo dc : datacenters) {
                    logger.info("  - {}: {}", dc.getId(), dc.isHealthy() ? "HEALTHY" : "UNHEALTHY");
                }
            })
            .exceptionally(throwable -> {
                logger.error("Error during health check", throwable);
                return null;
            });
        
        // Force refresh of datacenter info
        client.refreshDatacenterInfo()
            .thenRun(() -> logger.info("Datacenter info refresh completed"))
            .exceptionally(throwable -> {
                logger.error("Error during datacenter info refresh", throwable);
                return null;
            });
    }
    
    private void demonstrateConnectionPoolManagement(MultiDatacenterRedisClient client) {
        logger.info("=== Demonstrating Connection Pool Management ===");
        
        // Check overall pool health
        boolean allPoolsHealthy = client.areAllPoolsHealthy();
        logger.info("All pools healthy: {}", allPoolsHealthy);
        
        // Check individual pool health
        var poolHealthMap = client.getConnectionPoolHealth();
        logger.info("Individual pool health:");
        poolHealthMap.forEach((datacenterId, healthy) -> 
            logger.info("  - {}: {}", datacenterId, healthy ? "HEALTHY" : "UNHEALTHY"));
        
        // Get aggregated pool metrics
        var aggregatedMetrics = client.getAggregatedPoolMetrics();
        logger.info("Aggregated pool metrics:");
        logger.info("  - Pool count: {}", aggregatedMetrics.getTotalPools());
        logger.info("  - Total connections: {}", aggregatedMetrics.getTotalActiveConnections());
        logger.info("  - Active connections: {}", aggregatedMetrics.getTotalActiveConnections());
        
        // Force maintenance on all pools
        logger.info("Triggering maintenance on all connection pools...");
        client.maintainAllConnectionPools();
        
        // Get detailed metrics for each pool
        var allMetrics = client.getAllConnectionPoolMetrics();
        logger.info("Detailed pool metrics:");
        allMetrics.forEach((datacenterId, metrics) -> {
            logger.info("  Datacenter {}: active={}, idle={}, max={}", 
                datacenterId, 
                metrics.getActiveConnections(),
                metrics.getIdleConnections(),
                metrics.getMaxPoolSize());
        });
    }
    
    private void runMonitoringPeriod(MultiDatacenterRedisClient client) {
        logger.info("=== Running Monitoring Period (45 seconds) ===");
        
        CountDownLatch monitoringLatch = new CountDownLatch(1);
        
        // Generate some activity to trigger events
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // Perform some operations to generate pool activity
                client.sync().set("monitoring:test:" + System.currentTimeMillis(), "test_value");
                client.async().get("monitoring:test:key");
                
                // Force a ping to check connectivity
                client.reactive().ping().subscribe(
                    result -> logger.debug("Ping successful: {}", result),
                    error -> logger.warn("Ping failed", error)
                );
                
            } catch (Exception e) {
                logger.debug("Operation during monitoring failed (expected)", e);
            }
        }, 1, 3, TimeUnit.SECONDS);
        
        // End monitoring after 45 seconds
        scheduler.schedule(() -> monitoringLatch.countDown(), 45, TimeUnit.SECONDS);
        
        try {
            monitoringLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Monitoring period interrupted");
        }
        
        // Report final statistics
        logger.info("Monitoring period completed:");
        logger.info("  - Health changes detected: {}", healthChangeCount.get());
        logger.info("  - Pool events received: {}", poolEventCount.get());
    }
    
    private void reportHealthStatus(MultiDatacenterRedisClient client) {
        logger.info("📊 HEALTH STATUS REPORT:");
        
        for (DatacenterInfo datacenter : client.getDatacenters()) {
            String status = datacenter.isHealthy() ? "🟢 HEALTHY" : "🔴 UNHEALTHY";
            logger.info("  {} - {} (datacenter ID: {})", 
                datacenter.getId(), status, datacenter.getId());
        }
        
        var aggregatedMetrics = client.getAggregatedPoolMetrics();
        logger.info("  Pool Summary: {} pools, {} active connections", 
            aggregatedMetrics.getTotalPools(),
            aggregatedMetrics.getTotalActiveConnections());
    }
    
    private void reportPoolMetrics(MultiDatacenterRedisClient client) {
        logger.info("📈 POOL METRICS REPORT:");
        
        var allMetrics = client.getAllConnectionPoolMetrics();
        allMetrics.forEach((datacenterId, metrics) -> {
            boolean healthy = client.isConnectionPoolHealthy(datacenterId);
            String healthIcon = healthy ? "💚" : "💔";
            
            logger.info("  {} {} - Active: {}, Idle: {}, Max: {}", 
                healthIcon, datacenterId,
                metrics.getActiveConnections(),
                metrics.getIdleConnections(),
                metrics.getMaxPoolSize());
        });
    }
    
    private void triggerHealthAlert(DatacenterInfo datacenter) {
        // In a real implementation, this would:
        // - Send alerts via email, Slack, PagerDuty, etc.
        // - Update monitoring dashboards
        // - Trigger automated remediation
        // - Log to audit trail
        
        logger.warn("🚨 ALERT: Datacenter {} health degraded - implementing fallback strategies", 
            datacenter.getId());
        
        // Example alert payload (would be sent to external systems)
        AlertPayload alert = new AlertPayload(
            "DATACENTER_HEALTH_DEGRADED",
            "Datacenter " + datacenter.getId() + " is unhealthy",
            "HIGH",
            System.currentTimeMillis(),
            datacenter
        );
        
        logger.info("Alert payload: {}", alert);
    }
    
    public void shutdown() {
        logger.info("Shutting down health monitoring example");
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
    
    // Example alert payload class
    private static class AlertPayload {
        private final String type;
        private final String message;
        private final String severity;
        private final long timestamp;
        private final DatacenterInfo datacenter;
        
        public AlertPayload(String type, String message, String severity, long timestamp, DatacenterInfo datacenter) {
            this.type = type;
            this.message = message;
            this.severity = severity;
            this.timestamp = timestamp;
            this.datacenter = datacenter;
        }
        
        @Override
        public String toString() {
            return String.format("AlertPayload{type='%s', message='%s', severity='%s', timestamp=%d, datacenter='%s'}", 
                type, message, severity, timestamp, datacenter.getId());
        }
    }
}
