package com.redis.multidc;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.operations.AsyncOperations;
import com.redis.multidc.operations.ReactiveOperations;
import com.redis.multidc.operations.SyncOperations;
import com.redis.multidc.pool.ConnectionPoolMetrics;
import com.redis.multidc.pool.ConnectionPoolManager.AggregatedPoolMetrics;
import reactor.core.Disposable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Main client interface for Redis operations across multiple datacenters.
 * Provides sync, async, and reactive programming models with intelligent
 * datacenter routing and locality-aware operations.
 */
public interface MultiDatacenterRedisClient extends AutoCloseable {
    
    /**
     * Get synchronous operations interface.
     * @return SyncOperations instance for blocking operations
     */
    SyncOperations sync();
    
    /**
     * Get asynchronous operations interface.
     * @return AsyncOperations instance for non-blocking CompletableFuture-based operations
     */
    AsyncOperations async();
    
    /**
     * Get reactive operations interface.
     * @return ReactiveOperations instance for reactive stream-based operations
     */
    ReactiveOperations reactive();
    
    /**
     * Get information about all configured datacenters.
     * @return List of datacenter information including health and latency metrics
     */
    List<DatacenterInfo> getDatacenters();
    
    /**
     * Get the currently active (local) datacenter.
     * @return DatacenterInfo for the local datacenter
     */
    DatacenterInfo getLocalDatacenter();
    
    /**
     * Check the health of all datacenters.
     * @return CompletableFuture containing health status of all datacenters
     */
    CompletableFuture<List<DatacenterInfo>> checkDatacenterHealth();
    
    /**
     * Force a refresh of datacenter routing information.
     * This will re-evaluate latencies and health status.
     * @return CompletableFuture that completes when refresh is done
     */
    CompletableFuture<Void> refreshDatacenterInfo();
    
    /**
     * Get current configuration.
     * @return DatacenterConfiguration instance
     */
    DatacenterConfiguration getConfiguration();
    
    /**
     * Subscribe to datacenter health change events.
     * @param listener callback for health change events
     * @return Disposable to unsubscribe from events
     */
    Disposable subscribeToHealthChanges(DatacenterHealthListener listener);
    
    /**
     * Get connection pool metrics for a specific datacenter.
     * @param datacenterId the datacenter identifier
     * @return connection pool metrics, or null if datacenter not found
     */
    ConnectionPoolMetrics getConnectionPoolMetrics(String datacenterId);
    
    /**
     * Get aggregated connection pool metrics across all datacenters.
     * @return aggregated pool metrics
     */
    AggregatedPoolMetrics getAggregatedPoolMetrics();
    
    /**
     * Check if all connection pools are healthy.
     * @return true if all pools are healthy and can provide connections
     */
    boolean areAllPoolsHealthy();
    
    /**
     * Force maintenance on all connection pools.
     * This triggers immediate cleanup of idle/expired connections.
     */
    void maintainAllConnectionPools();
    
    /**
     * Close the client and release all resources.
     */
    @Override
    void close();
    
    /**
     * Callback interface for datacenter health change events.
     */
    @FunctionalInterface
    interface DatacenterHealthListener {
        void onHealthChange(DatacenterInfo datacenter, boolean healthy);
    }
}
