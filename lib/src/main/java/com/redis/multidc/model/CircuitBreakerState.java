package com.redis.multidc.model;

/**
 * Circuit breaker state for datacenter connections.
 */
public enum CircuitBreakerState {
    
    /**
     * Circuit breaker is closed - requests are allowed through.
     */
    CLOSED,
    
    /**
     * Circuit breaker is open - requests are rejected to prevent cascading failures.
     */
    OPEN,
    
    /**
     * Circuit breaker is half-open - limited requests are allowed to test recovery.
     */
    HALF_OPEN
}
