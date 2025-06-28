package com.redis.multidc.routing;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.model.CircuitBreakerState;
import io.lettuce.core.api.StatefulRedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Monitors the health of all datacenters and updates routing information.
 * Performs periodic health checks and circuit breaker management.
 */
public class DatacenterHealthMonitor {
    
    private static final Logger logger = LoggerFactory.getLogger(DatacenterHealthMonitor.class);
    
    private final DatacenterConfiguration configuration;
    private final Map<String, StatefulRedisConnection<String, String>> connections;
    private final Sinks.Many<HealthChangeEvent> healthEventSink;
    private final ScheduledExecutorService scheduler;
    private final Map<String, CircuitBreakerState> circuitBreakerStates;
    private final Map<String, Long> lastHealthCheckTimes;
    private final Map<String, Boolean> healthStates;
    private volatile boolean running = false;
    private ScheduledFuture<?> healthCheckTask;
    
    public DatacenterHealthMonitor(DatacenterConfiguration configuration,
                                 Map<String, StatefulRedisConnection<String, String>> connections) {
        this.configuration = configuration;
        this.connections = connections;
        this.healthEventSink = null; // Simplified for now
        this.scheduler = Executors.newScheduledThreadPool(2, 
            r -> new Thread(r, "datacenter-health-monitor"));
        this.circuitBreakerStates = new ConcurrentHashMap<>();
        this.lastHealthCheckTimes = new ConcurrentHashMap<>();
        this.healthStates = new ConcurrentHashMap<>();
        
        // Initialize circuit breaker states
        for (var datacenter : configuration.getDatacenters()) {
            circuitBreakerStates.put(datacenter.getId(), CircuitBreakerState.CLOSED);
            healthStates.put(datacenter.getId(), true);
        }
    }
    
    public void start() {
        if (running) {
            return;
        }
        
        running = true;
        Duration interval = configuration.getHealthCheckInterval();
        
        healthCheckTask = scheduler.scheduleWithFixedDelay(
            this::performHealthCheck,
            0,
            interval.toMillis(),
            TimeUnit.MILLISECONDS
        );
        
        logger.info("Health monitor started with interval: {}", interval);
    }
    
    public void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        
        if (healthCheckTask != null) {
            healthCheckTask.cancel(false);
        }
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        logger.info("Health monitor stopped");
    }
    
    private void performHealthCheck() {
        try {
            for (var datacenter : configuration.getDatacenters()) {
                checkDatacenterHealth(datacenter.getId());
            }
        } catch (Exception e) {
            logger.error("Error during health check", e);
        }
    }
    
    private void checkDatacenterHealth(String datacenterId) {
        StatefulRedisConnection<String, String> connection = connections.get(datacenterId);
        if (connection == null) {
            updateHealthState(datacenterId, false, "No connection available");
            return;
        }
        
        long startTime = System.currentTimeMillis();
        boolean healthy = false;
        String errorMessage = null;
        
        try {
            // Perform ping with timeout
            CompletableFuture<String> pingFuture = connection.async().ping().toCompletableFuture();
            String result = pingFuture.get(configuration.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS);
            healthy = "PONG".equals(result);
            
            if (!healthy) {
                errorMessage = "Unexpected ping response: " + result;
            }
            
        } catch (TimeoutException e) {
            errorMessage = "Health check timeout";
        } catch (Exception e) {
            errorMessage = "Health check failed: " + e.getMessage();
        }
        
        long latency = System.currentTimeMillis() - startTime;
        lastHealthCheckTimes.put(datacenterId, System.currentTimeMillis());
        
        updateHealthState(datacenterId, healthy, errorMessage);
        updateCircuitBreakerState(datacenterId, healthy);
        
        logger.debug("Health check for datacenter {}: healthy={}, latency={}ms", 
                    datacenterId, healthy, latency);
    }
    
    private void updateHealthState(String datacenterId, boolean healthy, String errorMessage) {
        Boolean previousState = healthStates.put(datacenterId, healthy);
        
        if (previousState == null || previousState != healthy) {
            logger.info("Datacenter {} health changed: {} -> {}", 
                       datacenterId, previousState, healthy);
            
            if (!healthy && errorMessage != null) {
                logger.warn("Datacenter {} is unhealthy: {}", datacenterId, errorMessage);
            }
            
            // Emit health change event (simplified - no emission for now)
            try {
                DatacenterInfo info = createDatacenterInfo(datacenterId, healthy);
                // TODO: Implement health event emission
                // healthEventSink.tryEmitNext(new HealthChangeEvent(info, healthy));
            } catch (Exception e) {
                logger.warn("Failed to create datacenter info", e);
            }
        }
    }
    
    private void updateCircuitBreakerState(String datacenterId, boolean healthy) {
        if (!configuration.isCircuitBreakerEnabled()) {
            return;
        }
        
        CircuitBreakerState currentState = circuitBreakerStates.get(datacenterId);
        CircuitBreakerState newState = calculateNewCircuitBreakerState(currentState, healthy);
        
        if (currentState != newState) {
            circuitBreakerStates.put(datacenterId, newState);
            logger.info("Circuit breaker state changed for datacenter {}: {} -> {}", 
                       datacenterId, currentState, newState);
        }
    }
    
    private CircuitBreakerState calculateNewCircuitBreakerState(CircuitBreakerState currentState, boolean healthy) {
        // Simplified circuit breaker logic
        // In a real implementation, this would track failure counts, success counts, etc.
        
        return switch (currentState) {
            case CLOSED -> healthy ? CircuitBreakerState.CLOSED : CircuitBreakerState.OPEN;
            case OPEN -> healthy ? CircuitBreakerState.HALF_OPEN : CircuitBreakerState.OPEN;
            case HALF_OPEN -> healthy ? CircuitBreakerState.CLOSED : CircuitBreakerState.OPEN;
        };
    }
    
    public CompletableFuture<List<DatacenterInfo>> checkAllDatacenters() {
        return CompletableFuture.supplyAsync(() -> {
            List<CompletableFuture<DatacenterInfo>> futures = configuration.getDatacenters().stream()
                .map(datacenter -> CompletableFuture.supplyAsync(() -> {
                    checkDatacenterHealth(datacenter.getId());
                    return createDatacenterInfo(datacenter.getId(), 
                                              healthStates.getOrDefault(datacenter.getId(), false));
                }))
                .collect(Collectors.toList());
            
            return futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
        });
    }
    
    public void forceHealthCheck() {
        if (running) {
            scheduler.execute(this::performHealthCheck);
        }
    }
    
    private DatacenterInfo createDatacenterInfo(String datacenterId, boolean healthy) {
        var endpoint = configuration.getDatacenters().stream()
            .filter(dc -> dc.getId().equals(datacenterId))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown datacenter: " + datacenterId));
        
        return DatacenterInfo.builder()
            .id(datacenterId)
            .region(endpoint.getRegion())
            .host(endpoint.getHost())
            .port(endpoint.getPort())
            .healthy(healthy)
            .lastHealthCheck(Instant.now())
            .circuitBreakerState(circuitBreakerStates.get(datacenterId))
            .local(datacenterId.equals(configuration.getLocalDatacenterId()))
            .build();
    }
    
    /**
     * Health change event for internal use.
     */
    private static class HealthChangeEvent {
        private final DatacenterInfo datacenterInfo;
        private final boolean healthy;
        
        public HealthChangeEvent(DatacenterInfo datacenterInfo, boolean healthy) {
            this.datacenterInfo = datacenterInfo;
            this.healthy = healthy;
        }
        
        public DatacenterInfo getDatacenterInfo() {
            return datacenterInfo;
        }
        
        public boolean isHealthy() {
            return healthy;
        }
    }
}
