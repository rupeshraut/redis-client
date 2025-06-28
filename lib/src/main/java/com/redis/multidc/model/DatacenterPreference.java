package com.redis.multidc.model;

/**
 * Preference for datacenter selection when performing operations.
 */
public enum DatacenterPreference {
    
    /**
     * Use the local datacenter only.
     * Operation will fail if local datacenter is unavailable.
     */
    LOCAL_ONLY,
    
    /**
     * Prefer local datacenter, fallback to remote if local is unavailable.
     */
    LOCAL_PREFERRED,
    
    /**
     * Use any available datacenter based on routing strategy.
     */
    ANY_AVAILABLE,
    
    /**
     * Use the datacenter with lowest latency.
     */
    LOWEST_LATENCY,
    
    /**
     * Use a specific datacenter by ID.
     * Must be combined with datacenter ID specification.
     */
    SPECIFIC_DATACENTER,
    
    /**
     * Use remote datacenters only, exclude local.
     */
    REMOTE_ONLY
}
