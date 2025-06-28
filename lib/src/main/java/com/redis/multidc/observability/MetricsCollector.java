package com.redis.multidc.observability;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.model.DatacenterInfo;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and manages metrics for multi-datacenter Redis operations.
 * Provides observability into performance, health, and usage patterns.
 */
public class MetricsCollector {
    
    private static final Logger logger = LoggerFactory.getLogger(MetricsCollector.class);
    
    private final DatacenterConfiguration configuration;
    private final MeterRegistry meterRegistry;
    private final ConcurrentHashMap<String, DatacenterMetrics> datacenterMetrics;
    
    // Global metrics
    private final Counter totalRequests;
    private final Counter failedRequests;
    private final Timer requestLatency;
    private final AtomicLong activeConnections;
    
    public MetricsCollector(DatacenterConfiguration configuration) {
        this(configuration, new SimpleMeterRegistry());
    }
    
    public MetricsCollector(DatacenterConfiguration configuration, MeterRegistry meterRegistry) {
        this.configuration = configuration;
        this.meterRegistry = meterRegistry;
        this.datacenterMetrics = new ConcurrentHashMap<>();
        this.activeConnections = new AtomicLong(0);
        
        // Initialize global metrics
        this.totalRequests = Counter.builder("redis.multidc.requests.total")
            .description("Total number of Redis requests")
            .register(meterRegistry);
            
        this.failedRequests = Counter.builder("redis.multidc.requests.failed")
            .description("Total number of failed Redis requests")
            .register(meterRegistry);
            
        this.requestLatency = Timer.builder("redis.multidc.request.latency")
            .description("Redis request latency")
            .register(meterRegistry);
            
        Gauge.builder("redis.multidc.connections.active", activeConnections, AtomicLong::doubleValue)
            .description("Number of active connections")
            .register(meterRegistry);
        
        // Initialize datacenter-specific metrics
        initializeDatacenterMetrics();
        
        logger.info("Metrics collector initialized for {} datacenters", 
                   configuration.getDatacenters().size());
    }
    
    private void initializeDatacenterMetrics() {
        for (var datacenter : configuration.getDatacenters()) {
            DatacenterMetrics metrics = new DatacenterMetrics(datacenter.getId(), meterRegistry);
            datacenterMetrics.put(datacenter.getId(), metrics);
        }
    }
    
    public void recordRequest(String datacenterId, Duration latency, boolean success) {
        // Global metrics
        totalRequests.increment();
        if (!success) {
            failedRequests.increment();
        }
        requestLatency.record(latency);
        
        // Datacenter-specific metrics (only if datacenterId is not null)
        if (datacenterId != null) {
            DatacenterMetrics metrics = datacenterMetrics.get(datacenterId);
            if (metrics != null) {
                metrics.recordRequest(latency, success);
            }
        }
    }
    
    public void recordConnectionEvent(String datacenterId, boolean connected) {
        if (connected) {
            activeConnections.incrementAndGet();
        } else {
            activeConnections.decrementAndGet();
        }
        
        if (datacenterId != null) {
            DatacenterMetrics metrics = datacenterMetrics.get(datacenterId);
            if (metrics != null) {
                metrics.recordConnectionEvent(connected);
            }
        }
    }
    
    public void recordDatacenterUpdate(DatacenterInfo info) {
        DatacenterMetrics metrics = datacenterMetrics.get(info.getId());
        if (metrics != null) {
            metrics.updateHealthStatus(info.isHealthy());
            metrics.updateLatency(info.getLatencyMs());
        }
    }
    
    public void recordCircuitBreakerEvent(String datacenterId, String event) {
        if (datacenterId != null) {
            DatacenterMetrics metrics = datacenterMetrics.get(datacenterId);
            if (metrics != null) {
                metrics.recordCircuitBreakerEvent(event);
            }
        }
    }
    
    public void recordCacheOperation(String operation, boolean hit) {
        Counter.builder("redis.multidc.cache." + operation)
            .tag("hit", String.valueOf(hit))
            .description("Cache operation: " + operation)
            .register(meterRegistry)
            .increment();
    }
    
    public void recordTombstoneOperation(String operation, String type) {
        Counter.builder("redis.multidc.tombstone." + operation)
            .tag("type", type)
            .description("Tombstone operation: " + operation)
            .register(meterRegistry)
            .increment();
    }
    
    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }
    
    /**
     * Datacenter-specific metrics container.
     */
    private static class DatacenterMetrics {
        private final String datacenterId;
        private final MeterRegistry registry;
        private final Counter requests;
        private final Counter failures;
        private final Timer latency;
        private final AtomicLong connections;
        private final AtomicLong healthStatus; // 1 for healthy, 0 for unhealthy
        private final AtomicLong currentLatency;
        
        public DatacenterMetrics(String datacenterId, MeterRegistry registry) {
            this.datacenterId = datacenterId;
            this.registry = registry;
            this.connections = new AtomicLong(0);
            this.healthStatus = new AtomicLong(1);
            this.currentLatency = new AtomicLong(0);
            
            this.requests = Counter.builder("redis.multidc.datacenter.requests")
                .tag("datacenter", datacenterId)
                .description("Requests per datacenter")
                .register(registry);
                
            this.failures = Counter.builder("redis.multidc.datacenter.failures")
                .tag("datacenter", datacenterId)
                .description("Failures per datacenter")
                .register(registry);
                
            this.latency = Timer.builder("redis.multidc.datacenter.latency")
                .tag("datacenter", datacenterId)
                .description("Latency per datacenter")
                .register(registry);
                
            Gauge.builder("redis.multidc.datacenter.connections", connections, AtomicLong::doubleValue)
                .tag("datacenter", datacenterId)
                .description("Connections per datacenter")
                .register(registry);
                
            Gauge.builder("redis.multidc.datacenter.health", healthStatus, AtomicLong::doubleValue)
                .tag("datacenter", datacenterId)
                .description("Health status per datacenter")
                .register(registry);
                
            Gauge.builder("redis.multidc.datacenter.current_latency", currentLatency, AtomicLong::doubleValue)
                .tag("datacenter", datacenterId)
                .description("Current latency per datacenter")
                .register(registry);
        }
        
        public void recordRequest(Duration requestLatency, boolean success) {
            requests.increment();
            if (!success) {
                failures.increment();
            }
            latency.record(requestLatency);
        }
        
        public void recordConnectionEvent(boolean connected) {
            if (connected) {
                connections.incrementAndGet();
            } else {
                connections.decrementAndGet();
            }
        }
        
        public void updateHealthStatus(boolean healthy) {
            healthStatus.set(healthy ? 1 : 0);
        }
        
        public void updateLatency(long latencyMs) {
            currentLatency.set(latencyMs);
        }
        
        public void recordCircuitBreakerEvent(String event) {
            Counter.builder("redis.multidc.circuit_breaker")
                .tag("datacenter", datacenterId)
                .tag("event", event)
                .description("Circuit breaker events")
                .register(registry)
                .increment();
        }
    }
}
