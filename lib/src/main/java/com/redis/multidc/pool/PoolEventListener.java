package com.redis.multidc.pool;

import com.redis.multidc.observability.HealthEventPublisher.PoolEventNotification;

/**
 * Functional interface for pool event listeners.
 * Allows subscribers to receive notifications about connection pool events.
 */
@FunctionalInterface
public interface PoolEventListener {
    
    /**
     * Called when a pool event occurs.
     * 
     * @param datacenterId the datacenter where the event occurred
     * @param event the type of pool event
     * @param details additional details about the event
     */
    void onPoolEvent(String datacenterId, PoolEvent event, String details);
    
    /**
     * Creates a PoolEventListener from a PoolEventNotification consumer.
     * 
     * @param consumer the notification consumer
     * @return PoolEventListener that delegates to the consumer
     */
    static PoolEventListener fromConsumer(java.util.function.Consumer<PoolEventNotification> consumer) {
        return (datacenterId, event, details) -> {
            PoolEventNotification notification = new PoolEventNotification(
                datacenterId, event, details, System.currentTimeMillis());
            consumer.accept(notification);
        };
    }
}
