package com.redis.multidc.pool;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for managing Redis connection pools per datacenter.
 * Provides connection lifecycle management with proper resource cleanup.
 */
public interface ConnectionPool<K, V> extends AutoCloseable {
    
    /**
     * Acquires a connection from the pool.
     * 
     * @return a future that completes with a pooled connection
     * @throws ConnectionPoolException if unable to acquire connection
     */
    CompletableFuture<PooledConnection<K, V>> acquireConnection();
    
    /**
     * Acquires a connection from the pool with timeout.
     * 
     * @param timeout maximum time to wait for a connection
     * @return a future that completes with a pooled connection
     * @throws ConnectionPoolException if unable to acquire connection within timeout
     */
    CompletableFuture<PooledConnection<K, V>> acquireConnection(Duration timeout);
    
    /**
     * Returns a connection to the pool.
     * This method should be called automatically when using try-with-resources.
     * 
     * @param connection the connection to return
     */
    void returnConnection(PooledConnection<K, V> connection);
    
    /**
     * Returns pool statistics.
     * 
     * @return current pool metrics
     */
    ConnectionPoolMetrics getMetrics();
    
    /**
     * Gets the datacenter ID this pool serves.
     * 
     * @return datacenter identifier
     */
    String getDatacenterId();
    
    /**
     * Checks if the pool is healthy and can provide connections.
     * 
     * @return true if pool is operational
     */
    boolean isHealthy();
    
    /**
     * Validates and cleans up idle connections.
     * This method is typically called periodically by a background task.
     */
    void maintainPool();
    
    /**
     * Closes the pool and all its connections.
     * After calling this method, the pool becomes unusable.
     */
    @Override
    void close();
    
    /**
     * A connection wrapper that automatically returns the connection to the pool
     * when closed or when used with try-with-resources.
     */
    interface PooledConnection<K, V> extends AutoCloseable {
        
        /**
         * Gets the underlying Redis connection.
         * 
         * @return the stateful Redis connection
         */
        StatefulRedisConnection<K, V> getConnection();
        
        /**
         * Gets synchronous Redis commands.
         * 
         * @return sync commands interface
         */
        RedisCommands<K, V> sync();
        
        /**
         * Gets asynchronous Redis commands.
         * 
         * @return async commands interface
         */
        RedisAsyncCommands<K, V> async();
        
        /**
         * Gets reactive Redis commands.
         * 
         * @return reactive commands interface
         */
        RedisReactiveCommands<K, V> reactive();
        
        /**
         * Checks if the connection is still valid.
         * 
         * @return true if connection is valid and usable
         */
        boolean isValid();
        
        /**
         * Gets the time when this connection was created.
         * 
         * @return connection creation timestamp in milliseconds
         */
        long getCreationTime();
        
        /**
         * Gets the time when this connection was last used.
         * 
         * @return last usage timestamp in milliseconds
         */
        long getLastUsedTime();
        
        /**
         * Returns the connection to the pool.
         * This method is idempotent and can be called multiple times safely.
         */
        @Override
        void close();
    }
}
