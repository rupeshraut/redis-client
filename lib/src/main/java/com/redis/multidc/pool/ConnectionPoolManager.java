package com.redis.multidc.pool;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.pool.impl.DefaultConnectionPool;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.resource.ClientResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages connection pools for all configured datacenters.
 * Provides centralized pool lifecycle management and metrics aggregation.
 */
public class ConnectionPoolManager implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPoolManager.class);
    
    private final DatacenterConfiguration configuration;
    private final ClientResources clientResources;
    private final Map<String, ConnectionPool<String, String>> pools;
    private final Map<String, RedisClient> clients;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    public ConnectionPoolManager(DatacenterConfiguration configuration) {
        this.configuration = configuration;
        this.clientResources = ClientResources.builder()
            .build();
        this.pools = new ConcurrentHashMap<>();
        this.clients = new ConcurrentHashMap<>();
        
        initializePools();
    }
    
    /**
     * Gets a connection from the specified datacenter pool.
     * 
     * @param datacenterId the datacenter identifier
     * @return a future that completes with a pooled connection
     * @throws ConnectionPoolException if datacenter not found or pool unavailable
     */
    public CompletableFuture<ConnectionPool.PooledConnection<String, String>> getConnection(String datacenterId) {
        ConnectionPool<String, String> pool = pools.get(datacenterId);
        if (pool == null) {
            return CompletableFuture.failedFuture(
                new ConnectionPoolException("No pool found for datacenter: " + datacenterId));
        }
        
        if (!pool.isHealthy()) {
            return CompletableFuture.failedFuture(
                new ConnectionPoolException("Pool is not healthy for datacenter: " + datacenterId));
        }
        
        return pool.acquireConnection();
    }
    
    /**
     * Gets the connection pool for a specific datacenter.
     * 
     * @param datacenterId the datacenter identifier
     * @return the connection pool, or null if not found
     */
    public ConnectionPool<String, String> getPool(String datacenterId) {
        return pools.get(datacenterId);
    }
    
    /**
     * Gets metrics for a specific datacenter pool.
     * 
     * @param datacenterId the datacenter identifier
     * @return pool metrics, or null if datacenter not found
     */
    public ConnectionPoolMetrics getPoolMetrics(String datacenterId) {
        ConnectionPool<String, String> pool = pools.get(datacenterId);
        return pool != null ? pool.getMetrics() : null;
    }
    
    /**
     * Gets aggregated metrics across all pools.
     * 
     * @return aggregated connection pool metrics
     */
    public AggregatedPoolMetrics getAggregatedMetrics() {
        return new AggregatedPoolMetrics(pools.values().stream()
            .map(ConnectionPool::getMetrics)
            .toList());
    }
    
    /**
     * Checks if all pools are healthy.
     * 
     * @return true if all pools are healthy
     */
    public boolean areAllPoolsHealthy() {
        return pools.values().stream().allMatch(ConnectionPool::isHealthy);
    }
    
    /**
     * Gets the number of configured pools.
     * 
     * @return number of connection pools
     */
    public int getPoolCount() {
        return pools.size();
    }
    
    /**
     * Forces maintenance on all pools.
     * This method triggers immediate maintenance tasks like connection validation
     * and cleanup across all pools.
     */
    public void maintainAllPools() {
        pools.values().forEach(ConnectionPool::maintainPool);
    }
    
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing connection pool manager with {} pools", pools.size());
            
            // Close all pools
            pools.values().forEach(pool -> {
                try {
                    pool.close();
                } catch (Exception e) {
                    logger.warn("Error closing pool for datacenter {}: {}", 
                               pool.getDatacenterId(), e.getMessage());
                }
            });
            
            // Close all Redis clients
            clients.values().forEach(client -> {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    logger.warn("Error shutting down Redis client: {}", e.getMessage());
                }
            });
            
            // Close client resources
            try {
                clientResources.shutdown();
            } catch (Exception e) {
                logger.warn("Error shutting down client resources: {}", e.getMessage());
            }
            
            pools.clear();
            clients.clear();
            
            logger.info("Connection pool manager closed successfully");
        }
    }
    
    private void initializePools() {
        logger.info("Initializing connection pools for {} datacenters", 
                   configuration.getDatacenters().size());
        
        for (DatacenterEndpoint endpoint : configuration.getDatacenters()) {
            try {
                createPoolForDatacenter(endpoint);
                logger.info("Created connection pool for datacenter: {}", endpoint.getId());
            } catch (Exception e) {
                logger.error("Failed to create connection pool for datacenter {}: {}", 
                           endpoint.getId(), e.getMessage(), e);
                // Continue with other datacenters
            }
        }
        
        if (pools.isEmpty()) {
            throw new RuntimeException("Failed to create any connection pools");
        }
        
        logger.info("Successfully initialized {} connection pools", pools.size());
    }
    
    private void createPoolForDatacenter(DatacenterEndpoint endpoint) {
        RedisURI uri = RedisURI.create(endpoint.getConnectionString());
        uri.setTimeout(configuration.getConnectionTimeout());
        
        RedisClient client = RedisClient.create(clientResources, uri);
        ConnectionPool<String, String> pool = new DefaultConnectionPool(
            endpoint.getId(), client, uri, endpoint.getPoolConfig());
        
        clients.put(endpoint.getId(), client);
        pools.put(endpoint.getId(), pool);
    }
    
    /**
     * Check if a specific pool is healthy.
     * 
     * @param datacenterId the datacenter identifier
     * @return true if the pool is healthy and can provide connections
     */
    public boolean isPoolHealthy(String datacenterId) {
        ConnectionPool<String, String> pool = pools.get(datacenterId);
        if (pool == null) {
            return false;
        }
        
        ConnectionPoolMetrics metrics = pool.getMetrics();
        // Consider pool healthy if:
        // 1. Efficiency ratio > 90%
        // 2. Utilization < 95%
        // 3. Average acquisition time < 1000ms
        return metrics.getEfficiencyRatio() > 0.9 &&
               metrics.getUtilizationPercentage() < 95.0 &&
               metrics.getAverageAcquisitionTime() < 1000.0;
    }
    
    /**
     * Get health status of all pools.
     * 
     * @return map of datacenter ID to health status
     */
    public Map<String, Boolean> getPoolHealth() {
        Map<String, Boolean> health = new ConcurrentHashMap<>();
        for (String datacenterId : pools.keySet()) {
            health.put(datacenterId, isPoolHealthy(datacenterId));
        }
        return health;
    }
    
    /**
     * Drain connections from a specific pool.
     * 
     * @param datacenterId the datacenter identifier
     */
    public void drainPool(String datacenterId) {
        ConnectionPool<String, String> pool = pools.get(datacenterId);
        if (pool != null) {
            pool.drain();
            logger.info("Pool drained for datacenter: {}", datacenterId);
        } else {
            logger.warn("Cannot drain pool for unknown datacenter: {}", datacenterId);
        }
    }
    
    /**
     * Reset metrics for a specific pool.
     * 
     * @param datacenterId the datacenter identifier
     */
    public void resetMetrics(String datacenterId) {
        ConnectionPool<String, String> pool = pools.get(datacenterId);
        if (pool != null) {
            pool.resetMetrics();
            logger.info("Pool metrics reset for datacenter: {}", datacenterId);
        } else {
            logger.warn("Cannot reset metrics for unknown datacenter: {}", datacenterId);
        }
    }
    
    /**
     * Get metrics for all pools.
     * 
     * @return map of datacenter ID to pool metrics
     */
    public Map<String, ConnectionPoolMetrics> getAllMetrics() {
        Map<String, ConnectionPoolMetrics> allMetrics = new ConcurrentHashMap<>();
        for (Map.Entry<String, ConnectionPool<String, String>> entry : pools.entrySet()) {
            allMetrics.put(entry.getKey(), entry.getValue().getMetrics());
        }
        return allMetrics;
    }
    
    /**
     * Aggregated metrics across all connection pools.
     */
    public static class AggregatedPoolMetrics {
        private final int totalPools;
        private final int totalMaxPoolSize;
        private final int totalCurrentPoolSize;
        private final int totalActiveConnections;
        private final int totalIdleConnections;
        private final long totalConnectionsCreated;
        private final long totalConnectionsDestroyed;
        private final long totalConnectionsAcquired;
        private final long totalConnectionsReturned;
        private final long totalAcquisitionTimeouts;
        private final long totalValidationFailures;
        private final double averageUtilization;
        private final double averageEfficiency;
        
        public AggregatedPoolMetrics(java.util.List<ConnectionPoolMetrics> allMetrics) {
            this.totalPools = allMetrics.size();
            this.totalMaxPoolSize = allMetrics.stream().mapToInt(ConnectionPoolMetrics::getMaxPoolSize).sum();
            this.totalCurrentPoolSize = allMetrics.stream().mapToInt(ConnectionPoolMetrics::getCurrentPoolSize).sum();
            this.totalActiveConnections = allMetrics.stream().mapToInt(ConnectionPoolMetrics::getActiveConnections).sum();
            this.totalIdleConnections = allMetrics.stream().mapToInt(ConnectionPoolMetrics::getIdleConnections).sum();
            this.totalConnectionsCreated = allMetrics.stream().mapToLong(ConnectionPoolMetrics::getTotalConnectionsCreated).sum();
            this.totalConnectionsDestroyed = allMetrics.stream().mapToLong(ConnectionPoolMetrics::getTotalConnectionsDestroyed).sum();
            this.totalConnectionsAcquired = allMetrics.stream().mapToLong(ConnectionPoolMetrics::getTotalConnectionsAcquired).sum();
            this.totalConnectionsReturned = allMetrics.stream().mapToLong(ConnectionPoolMetrics::getTotalConnectionsReturned).sum();
            this.totalAcquisitionTimeouts = allMetrics.stream().mapToLong(ConnectionPoolMetrics::getTotalAcquisitionTimeouts).sum();
            this.totalValidationFailures = allMetrics.stream().mapToLong(ConnectionPoolMetrics::getTotalValidationFailures).sum();
            this.averageUtilization = allMetrics.stream().mapToDouble(ConnectionPoolMetrics::getUtilizationPercentage).average().orElse(0.0);
            this.averageEfficiency = allMetrics.stream().mapToDouble(ConnectionPoolMetrics::getEfficiencyRatio).average().orElse(0.0);
        }
        
        // Getters
        public int getTotalPools() { return totalPools; }
        public int getTotalMaxPoolSize() { return totalMaxPoolSize; }
        public int getTotalCurrentPoolSize() { return totalCurrentPoolSize; }
        public int getTotalActiveConnections() { return totalActiveConnections; }
        public int getTotalIdleConnections() { return totalIdleConnections; }
        public long getTotalConnectionsCreated() { return totalConnectionsCreated; }
        public long getTotalConnectionsDestroyed() { return totalConnectionsDestroyed; }
        public long getTotalConnectionsAcquired() { return totalConnectionsAcquired; }
        public long getTotalConnectionsReturned() { return totalConnectionsReturned; }
        public long getTotalAcquisitionTimeouts() { return totalAcquisitionTimeouts; }
        public long getTotalValidationFailures() { return totalValidationFailures; }
        public double getAverageUtilization() { return averageUtilization; }
        public double getAverageEfficiency() { return averageEfficiency; }
        
        @Override
        public String toString() {
            return String.format(
                "AggregatedPoolMetrics{pools=%d, totalActive=%d/%d, utilization=%.1f%%, efficiency=%.3f}",
                totalPools, totalActiveConnections, totalMaxPoolSize, averageUtilization, averageEfficiency
            );
        }
    }
}
