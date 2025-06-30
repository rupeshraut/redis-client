package com.redis.multidc.observability;

import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.pool.PoolEvent;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Publishes health events and pool events for real-time monitoring and alerting.
 * Provides reactive streams for event subscription and handles event distribution
 * to multiple subscribers.
 */
public class HealthEventPublisher {
    
    private static final Logger logger = LoggerFactory.getLogger(HealthEventPublisher.class);
    
    private final Sinks.Many<HealthChangeEvent> healthEventSink;
    private final Sinks.Many<PoolEventNotification> poolEventSink;
    private final ConcurrentMap<String, Integer> subscriberCounts;
    
    public HealthEventPublisher() {
        this.healthEventSink = Sinks.many().multicast().onBackpressureBuffer();
        this.poolEventSink = Sinks.many().multicast().onBackpressureBuffer();
        this.subscriberCounts = new ConcurrentHashMap<>();
        
        logger.info("HealthEventPublisher initialized");
    }
    
    /**
     * Publishes a datacenter health change event.
     * 
     * @param datacenter the datacenter that changed health status
     * @param healthy the new health status
     */
    public void publishHealthChange(DatacenterInfo datacenter, boolean healthy) {
        HealthChangeEvent event = new HealthChangeEvent(datacenter, healthy, System.currentTimeMillis());
        
        Sinks.EmitResult result = healthEventSink.tryEmitNext(event);
        if (result.isFailure()) {
            logger.warn("Failed to publish health change event for datacenter {}: {}", 
                datacenter.getId(), result);
        } else {
            logger.debug("Published health change: datacenter={}, healthy={}", 
                datacenter.getId(), healthy);
        }
    }
    
    /**
     * Publishes a connection pool event.
     * 
     * @param datacenterId the datacenter identifier
     * @param event the pool event type
     * @param details additional event details
     */
    public void publishPoolEvent(String datacenterId, PoolEvent event, String details) {
        PoolEventNotification notification = new PoolEventNotification(
            datacenterId, event, details, System.currentTimeMillis());
        
        Sinks.EmitResult result = poolEventSink.tryEmitNext(notification);
        if (result.isFailure()) {
            logger.warn("Failed to publish pool event for datacenter {}: {}", datacenterId, result);
        } else {
            logger.debug("Published pool event: datacenter={}, event={}, details={}", 
                datacenterId, event, details);
        }
    }
    
    /**
     * Subscribes to datacenter health change events.
     * 
     * @param listener the callback for health change events
     * @return Disposable to unsubscribe
     */
    public Disposable subscribeToHealthChanges(Consumer<HealthChangeEvent> listener) {
        String subscriberId = "health-" + System.nanoTime();
        subscriberCounts.put(subscriberId, subscriberCounts.size() + 1);
        
        logger.info("New health change subscriber: {} (total subscribers: {})", 
            subscriberId, subscriberCounts.size());
        
        return healthEventSink.asFlux()
            .doOnSubscribe(subscription -> 
                logger.debug("Health change subscription activated: {}", subscriberId))
            .doOnCancel(() -> {
                subscriberCounts.remove(subscriberId);
                logger.info("Health change subscription cancelled: {} (remaining: {})", 
                    subscriberId, subscriberCounts.size());
            })
            .doOnError(error -> 
                logger.error("Error in health change subscription: {}", subscriberId, error))
            .subscribe(
                listener::accept,
                error -> logger.error("Health change subscriber error", error)
            );
    }
    
    /**
     * Subscribes to connection pool events.
     * 
     * @param listener the callback for pool events
     * @return Disposable to unsubscribe
     */
    public Disposable subscribeToPoolEvents(Consumer<PoolEventNotification> listener) {
        String subscriberId = "pool-" + System.nanoTime();
        subscriberCounts.put(subscriberId, subscriberCounts.size() + 1);
        
        logger.info("New pool event subscriber: {} (total subscribers: {})", 
            subscriberId, subscriberCounts.size());
        
        return poolEventSink.asFlux()
            .doOnSubscribe(subscription -> 
                logger.debug("Pool event subscription activated: {}", subscriberId))
            .doOnCancel(() -> {
                subscriberCounts.remove(subscriberId);
                logger.info("Pool event subscription cancelled: {} (remaining: {})", 
                    subscriberId, subscriberCounts.size());
            })
            .doOnError(error -> 
                logger.error("Error in pool event subscription: {}", subscriberId, error))
            .subscribe(
                listener::accept,
                error -> logger.error("Pool event subscriber error", error)
            );
    }
    
    /**
     * Gets the current number of active subscribers.
     * 
     * @return number of active subscribers
     */
    public int getSubscriberCount() {
        return subscriberCounts.size();
    }
    
    /**
     * Gets the health event stream for advanced reactive operations.
     * 
     * @return Flux of health change events
     */
    public Flux<HealthChangeEvent> getHealthEventStream() {
        return healthEventSink.asFlux();
    }
    
    /**
     * Gets the pool event stream for advanced reactive operations.
     * 
     * @return Flux of pool event notifications
     */
    public Flux<PoolEventNotification> getPoolEventStream() {
        return poolEventSink.asFlux();
    }
    
    /**
     * Closes the publisher and completes all active streams.
     */
    public void close() {
        logger.info("Closing HealthEventPublisher with {} active subscribers", subscriberCounts.size());
        
        healthEventSink.tryEmitComplete();
        poolEventSink.tryEmitComplete();
        subscriberCounts.clear();
        
        logger.info("HealthEventPublisher closed");
    }
    
    /**
     * Represents a datacenter health change event.
     */
    public static class HealthChangeEvent {
        private final DatacenterInfo datacenter;
        private final boolean healthy;
        private final long timestamp;
        
        public HealthChangeEvent(DatacenterInfo datacenter, boolean healthy, long timestamp) {
            this.datacenter = datacenter;
            this.healthy = healthy;
            this.timestamp = timestamp;
        }
        
        public DatacenterInfo getDatacenter() {
            return datacenter;
        }
        
        public boolean isHealthy() {
            return healthy;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("HealthChangeEvent{datacenter='%s', healthy=%s, timestamp=%d}",
                datacenter.getId(), healthy, timestamp);
        }
    }
    
    /**
     * Represents a connection pool event notification.
     */
    public static class PoolEventNotification {
        private final String datacenterId;
        private final PoolEvent event;
        private final String details;
        private final long timestamp;
        
        public PoolEventNotification(String datacenterId, PoolEvent event, String details, long timestamp) {
            this.datacenterId = datacenterId;
            this.event = event;
            this.details = details;
            this.timestamp = timestamp;
        }
        
        public String getDatacenterId() {
            return datacenterId;
        }
        
        public PoolEvent getEvent() {
            return event;
        }
        
        public String getDetails() {
            return details;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("PoolEventNotification{datacenter='%s', event=%s, details='%s', timestamp=%d}",
                datacenterId, event, details, timestamp);
        }
    }
}
