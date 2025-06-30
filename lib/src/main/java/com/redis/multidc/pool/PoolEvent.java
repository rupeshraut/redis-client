package com.redis.multidc.pool;

/**
 * Enumeration of connection pool events for monitoring and alerting.
 * These events provide insights into pool behavior and can trigger
 * automated responses or alerts.
 */
public enum PoolEvent {
    
    /**
     * A new connection was created in the pool.
     */
    CONNECTION_CREATED,
    
    /**
     * A connection was destroyed/closed in the pool.
     */
    CONNECTION_DESTROYED,
    
    /**
     * A connection was successfully acquired from the pool.
     */
    CONNECTION_ACQUIRED,
    
    /**
     * A connection was returned to the pool.
     */
    CONNECTION_RETURNED,
    
    /**
     * Connection acquisition timed out.
     * This indicates the pool may be under-sized or connections are not being returned.
     */
    ACQUISITION_TIMEOUT,
    
    /**
     * Connection validation failed.
     * This may indicate network issues or Redis server problems.
     */
    VALIDATION_FAILURE,
    
    /**
     * The connection pool is exhausted (no connections available).
     * This is a critical event that may indicate a connection leak or insufficient pool size.
     */
    POOL_EXHAUSTED,
    
    /**
     * The pool has been drained (all connections removed).
     * This typically happens during maintenance or shutdown.
     */
    POOL_DRAINED,
    
    /**
     * The pool has been restored after being drained.
     */
    POOL_RESTORED,
    
    /**
     * An idle connection was evicted from the pool.
     */
    IDLE_CONNECTION_EVICTED,
    
    /**
     * A connection exceeded its maximum age and was removed.
     */
    CONNECTION_AGED_OUT,
    
    /**
     * The pool size was adjusted (increased or decreased).
     */
    POOL_SIZE_ADJUSTED,
    
    /**
     * High acquisition times detected (above configured threshold).
     */
    HIGH_ACQUISITION_TIME,
    
    /**
     * Validation failure spike detected (multiple failures in short period).
     */
    VALIDATION_FAILURE_SPIKE,
    
    /**
     * Pool utilization is critically high (near or at maximum capacity).
     */
    HIGH_UTILIZATION_WARNING,
    
    /**
     * Pool efficiency has dropped below acceptable threshold.
     */
    LOW_EFFICIENCY_WARNING,
    
    /**
     * Pool health check completed successfully.
     */
    HEALTH_CHECK_PASSED,
    
    /**
     * Pool health check failed.
     */
    HEALTH_CHECK_FAILED,
    
    /**
     * Pool metrics were reset.
     */
    METRICS_RESET
}
