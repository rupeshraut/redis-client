package com.redis.multidc.config;

/**
 * Routing strategy for determining which datacenter to use for operations.
 */
public enum RoutingStrategy {
    
    /**
     * Always use the local datacenter for both reads and writes.
     * Falls back to remote datacenters only if local is unavailable.
     */
    LOCAL_ONLY,
    
    /**
     * Route based on measured latency to each datacenter.
     * Automatically selects the datacenter with lowest latency.
     */
    LATENCY_BASED,
    
    /**
     * Use priority-based routing based on datacenter configuration.
     * Higher priority datacenters are preferred.
     */
    PRIORITY_BASED,
    
    /**
     * Distribute load across datacenters based on configured weights.
     * Useful for load balancing across multiple healthy datacenters.
     */
    WEIGHTED_ROUND_ROBIN,
    
    /**
     * Use local datacenter for writes, any available datacenter for reads.
     * Optimizes for write consistency and read performance.
     */
    LOCAL_WRITE_ANY_READ,
    
    /**
     * Custom routing strategy - allows pluggable routing logic.
     */
    CUSTOM
}
