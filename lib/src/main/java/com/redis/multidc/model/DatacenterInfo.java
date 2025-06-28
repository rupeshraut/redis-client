package com.redis.multidc.model;

import java.time.Instant;

/**
 * Information about a datacenter including health, latency, and operational metrics.
 */
public class DatacenterInfo {
    
    private final String id;
    private final String region;
    private final String host;
    private final int port;
    private final boolean healthy;
    private final long latencyMs;
    private final Instant lastHealthCheck;
    private final int activeConnections;
    private final long totalRequests;
    private final long failedRequests;
    private final CircuitBreakerState circuitBreakerState;
    private final boolean local;
    
    public DatacenterInfo(String id, String region, String host, int port, boolean healthy, 
                         long latencyMs, Instant lastHealthCheck, int activeConnections,
                         long totalRequests, long failedRequests, CircuitBreakerState circuitBreakerState,
                         boolean local) {
        this.id = id;
        this.region = region;
        this.host = host;
        this.port = port;
        this.healthy = healthy;
        this.latencyMs = latencyMs;
        this.lastHealthCheck = lastHealthCheck;
        this.activeConnections = activeConnections;
        this.totalRequests = totalRequests;
        this.failedRequests = failedRequests;
        this.circuitBreakerState = circuitBreakerState;
        this.local = local;
    }
    
    public String getId() {
        return id;
    }
    
    public String getRegion() {
        return region;
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public boolean isHealthy() {
        return healthy;
    }
    
    public long getLatencyMs() {
        return latencyMs;
    }
    
    public Instant getLastHealthCheck() {
        return lastHealthCheck;
    }
    
    public int getActiveConnections() {
        return activeConnections;
    }
    
    public long getTotalRequests() {
        return totalRequests;
    }
    
    public long getFailedRequests() {
        return failedRequests;
    }
    
    public CircuitBreakerState getCircuitBreakerState() {
        return circuitBreakerState;
    }
    
    public boolean isLocal() {
        return local;
    }
    
    public double getSuccessRate() {
        if (totalRequests == 0) {
            return 100.0;
        }
        return ((double) (totalRequests - failedRequests) / totalRequests) * 100.0;
    }
    
    public boolean isAvailable() {
        return healthy && circuitBreakerState != CircuitBreakerState.OPEN;
    }
    
    @Override
    public String toString() {
        return String.format("DatacenterInfo{id='%s', region='%s', host='%s:%d', healthy=%s, latency=%dms, " +
                           "successRate=%.1f%%, circuitBreaker=%s, local=%s}",
            id, region, host, port, healthy, latencyMs, getSuccessRate(), circuitBreakerState, local);
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String id;
        private String region;
        private String host;
        private int port;
        private boolean healthy = true;
        private long latencyMs = 0;
        private Instant lastHealthCheck = Instant.now();
        private int activeConnections = 0;
        private long totalRequests = 0;
        private long failedRequests = 0;
        private CircuitBreakerState circuitBreakerState = CircuitBreakerState.CLOSED;
        private boolean local = false;
        
        public Builder id(String id) {
            this.id = id;
            return this;
        }
        
        public Builder region(String region) {
            this.region = region;
            return this;
        }
        
        public Builder host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder healthy(boolean healthy) {
            this.healthy = healthy;
            return this;
        }
        
        public Builder latencyMs(long latencyMs) {
            this.latencyMs = latencyMs;
            return this;
        }
        
        public Builder lastHealthCheck(Instant lastHealthCheck) {
            this.lastHealthCheck = lastHealthCheck;
            return this;
        }
        
        public Builder activeConnections(int activeConnections) {
            this.activeConnections = activeConnections;
            return this;
        }
        
        public Builder totalRequests(long totalRequests) {
            this.totalRequests = totalRequests;
            return this;
        }
        
        public Builder failedRequests(long failedRequests) {
            this.failedRequests = failedRequests;
            return this;
        }
        
        public Builder circuitBreakerState(CircuitBreakerState circuitBreakerState) {
            this.circuitBreakerState = circuitBreakerState;
            return this;
        }
        
        public Builder local(boolean local) {
            this.local = local;
            return this;
        }
        
        public DatacenterInfo build() {
            return new DatacenterInfo(id, region, host, port, healthy, latencyMs, 
                                    lastHealthCheck, activeConnections, totalRequests, 
                                    failedRequests, circuitBreakerState, local);
        }
    }
}
