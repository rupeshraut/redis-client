package com.redis.multidc.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Configuration for multi-datacenter Redis client operations.
 * Defines datacenter endpoints, routing preferences, and operational parameters.
 */
public class DatacenterConfiguration {
    
    private final List<DatacenterEndpoint> datacenters;
    private final String localDatacenterId;
    private final RoutingStrategy routingStrategy;
    private final Duration healthCheckInterval;
    private final Duration connectionTimeout;
    private final Duration requestTimeout;
    private final int maxRetries;
    private final Duration retryDelay;
    private final boolean enableCircuitBreaker;
    private final CircuitBreakerConfig circuitBreakerConfig;
    private final Map<String, Object> additionalProperties;
    
    private DatacenterConfiguration(Builder builder) {
        this.datacenters = List.copyOf(builder.datacenters);
        this.localDatacenterId = builder.localDatacenterId;
        this.routingStrategy = builder.routingStrategy;
        this.healthCheckInterval = builder.healthCheckInterval;
        this.connectionTimeout = builder.connectionTimeout;
        this.requestTimeout = builder.requestTimeout;
        this.maxRetries = builder.maxRetries;
        this.retryDelay = builder.retryDelay;
        this.enableCircuitBreaker = builder.enableCircuitBreaker;
        this.circuitBreakerConfig = builder.circuitBreakerConfig;
        this.additionalProperties = Map.copyOf(builder.additionalProperties);
    }
    
    public List<DatacenterEndpoint> getDatacenters() {
        return datacenters;
    }
    
    public String getLocalDatacenterId() {
        return localDatacenterId;
    }
    
    public RoutingStrategy getRoutingStrategy() {
        return routingStrategy;
    }
    
    public Duration getHealthCheckInterval() {
        return healthCheckInterval;
    }
    
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }
    
    public Duration getRequestTimeout() {
        return requestTimeout;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public Duration getRetryDelay() {
        return retryDelay;
    }
    
    public boolean isCircuitBreakerEnabled() {
        return enableCircuitBreaker;
    }
    
    public CircuitBreakerConfig getCircuitBreakerConfig() {
        return circuitBreakerConfig;
    }
    
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private List<DatacenterEndpoint> datacenters = List.of();
        private String localDatacenterId;
        private RoutingStrategy routingStrategy = RoutingStrategy.LATENCY_BASED;
        private Duration healthCheckInterval = Duration.ofSeconds(30);
        private Duration connectionTimeout = Duration.ofSeconds(5);
        private Duration requestTimeout = Duration.ofSeconds(10);
        private int maxRetries = 3;
        private Duration retryDelay = Duration.ofMillis(100);
        private boolean enableCircuitBreaker = true;
        private CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.defaultConfig();
        private Map<String, Object> additionalProperties = Map.of();
        
        public Builder datacenters(List<DatacenterEndpoint> datacenters) {
            this.datacenters = datacenters;
            return this;
        }
        
        public Builder localDatacenter(String datacenterId) {
            this.localDatacenterId = datacenterId;
            return this;
        }
        
        public Builder routingStrategy(RoutingStrategy strategy) {
            this.routingStrategy = strategy;
            return this;
        }
        
        public Builder healthCheckInterval(Duration interval) {
            this.healthCheckInterval = interval;
            return this;
        }
        
        public Builder connectionTimeout(Duration timeout) {
            this.connectionTimeout = timeout;
            return this;
        }
        
        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }
        
        public Builder maxRetries(int retries) {
            this.maxRetries = retries;
            return this;
        }
        
        public Builder retryDelay(Duration delay) {
            this.retryDelay = delay;
            return this;
        }
        
        public Builder enableCircuitBreaker(boolean enable) {
            this.enableCircuitBreaker = enable;
            return this;
        }
        
        public Builder circuitBreakerConfig(CircuitBreakerConfig config) {
            this.circuitBreakerConfig = config;
            return this;
        }
        
        public Builder additionalProperties(Map<String, Object> properties) {
            this.additionalProperties = properties;
            return this;
        }
        
        public DatacenterConfiguration build() {
            if (datacenters.isEmpty()) {
                throw new IllegalArgumentException("At least one datacenter must be configured");
            }
            if (localDatacenterId == null || localDatacenterId.trim().isEmpty()) {
                throw new IllegalArgumentException("Local datacenter ID must be specified");
            }
            boolean localFound = datacenters.stream()
                .anyMatch(dc -> dc.getId().equals(localDatacenterId));
            if (!localFound) {
                throw new IllegalArgumentException("Local datacenter ID not found in datacenter list");
            }
            return new DatacenterConfiguration(this);
        }
    }
}
