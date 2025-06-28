package com.redis.multidc.model;

/**
 * Callback interface for datacenter health change events.
 */
@FunctionalInterface
public interface DatacenterHealthListener {
    /**
     * Called when a datacenter's health status changes.
     * 
     * @param datacenter the datacenter information
     * @param healthy true if the datacenter is now healthy, false otherwise
     */
    void onHealthChange(DatacenterInfo datacenter, boolean healthy);
}
