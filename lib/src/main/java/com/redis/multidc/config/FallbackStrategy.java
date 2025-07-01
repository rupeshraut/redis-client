package com.redis.multidc.config;

/**
 * Fallback strategy for handling datacenter failures and unavailability.
 * These strategies define how the client should behave when the preferred
 * datacenter or routing strategy fails to find an available datacenter.
 */
public enum FallbackStrategy {
    
    /**
     * No fallback - fail immediately if preferred datacenter is unavailable.
     * Operations will fail with an exception if the target datacenter cannot be reached.
     */
    FAIL_FAST,
    
    /**
     * Fallback to the next available datacenter based on priority or latency.
     * This is the most common strategy for production environments.
     */
    NEXT_AVAILABLE,
    
    /**
     * Try all datacenters in order of preference until one succeeds.
     * More resilient but may introduce higher latency.
     */
    TRY_ALL,
    
    /**
     * Fallback to local datacenter only, regardless of original routing preference.
     * Useful for ensuring data locality in failure scenarios.
     */
    LOCAL_ONLY,
    
    /**
     * Fallback to remote datacenters only, avoiding the local datacenter.
     * Useful when local datacenter issues are suspected.
     */
    REMOTE_ONLY,
    
    /**
     * Use cached/stale data from any available datacenter when preferred is unavailable.
     * Prioritizes availability over consistency.
     */
    BEST_EFFORT,
    
    /**
     * Queue operations and retry when datacenters become available.
     * Includes configurable timeout and queue size limits.
     */
    QUEUE_AND_RETRY,
    
    /**
     * Custom fallback strategy - allows pluggable fallback logic.
     * Implementation can be provided via configuration.
     */
    CUSTOM
}
