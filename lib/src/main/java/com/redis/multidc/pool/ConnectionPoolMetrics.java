package com.redis.multidc.pool;

/**
 * Metrics for connection pool monitoring and observability.
 */
public class ConnectionPoolMetrics {
    
    private final String datacenterId;
    private final int maxPoolSize;
    private final int currentPoolSize;
    private final int activeConnections;
    private final int idleConnections;
    private final long totalConnectionsCreated;
    private final long totalConnectionsDestroyed;
    private final long totalConnectionsAcquired;
    private final long totalConnectionsReturned;
    private final long totalAcquisitionTimeouts;
    private final long totalValidationFailures;
    private final double averageAcquisitionTime;
    private final double averageConnectionAge;
    private final long peakActiveConnections;
    
    private ConnectionPoolMetrics(Builder builder) {
        this.datacenterId = builder.datacenterId;
        this.maxPoolSize = builder.maxPoolSize;
        this.currentPoolSize = builder.currentPoolSize;
        this.activeConnections = builder.activeConnections;
        this.idleConnections = builder.idleConnections;
        this.totalConnectionsCreated = builder.totalConnectionsCreated;
        this.totalConnectionsDestroyed = builder.totalConnectionsDestroyed;
        this.totalConnectionsAcquired = builder.totalConnectionsAcquired;
        this.totalConnectionsReturned = builder.totalConnectionsReturned;
        this.totalAcquisitionTimeouts = builder.totalAcquisitionTimeouts;
        this.totalValidationFailures = builder.totalValidationFailures;
        this.averageAcquisitionTime = builder.averageAcquisitionTime;
        this.averageConnectionAge = builder.averageConnectionAge;
        this.peakActiveConnections = builder.peakActiveConnections;
    }
    
    public String getDatacenterId() {
        return datacenterId;
    }
    
    public int getMaxPoolSize() {
        return maxPoolSize;
    }
    
    public int getCurrentPoolSize() {
        return currentPoolSize;
    }
    
    public int getActiveConnections() {
        return activeConnections;
    }
    
    public int getIdleConnections() {
        return idleConnections;
    }
    
    public long getTotalConnectionsCreated() {
        return totalConnectionsCreated;
    }
    
    public long getTotalConnectionsDestroyed() {
        return totalConnectionsDestroyed;
    }
    
    public long getTotalConnectionsAcquired() {
        return totalConnectionsAcquired;
    }
    
    public long getTotalConnectionsReturned() {
        return totalConnectionsReturned;
    }
    
    public long getTotalAcquisitionTimeouts() {
        return totalAcquisitionTimeouts;
    }
    
    public long getTotalValidationFailures() {
        return totalValidationFailures;
    }
    
    public double getAverageAcquisitionTime() {
        return averageAcquisitionTime;
    }
    
    public double getAverageConnectionAge() {
        return averageConnectionAge;
    }
    
    public long getPeakActiveConnections() {
        return peakActiveConnections;
    }
    
    /**
     * Gets the connection pool utilization as a percentage.
     * 
     * @return utilization percentage (0.0 to 100.0)
     */
    public double getUtilizationPercentage() {
        if (maxPoolSize == 0) {
            return 0.0;
        }
        return (double) activeConnections / maxPoolSize * 100.0;
    }
    
    /**
     * Gets the pool efficiency ratio (successful acquisitions vs timeouts).
     * 
     * @return efficiency ratio (0.0 to 1.0)
     */
    public double getEfficiencyRatio() {
        long totalAttempts = totalConnectionsAcquired + totalAcquisitionTimeouts;
        if (totalAttempts == 0) {
            return 1.0;
        }
        return (double) totalConnectionsAcquired / totalAttempts;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String datacenterId;
        private int maxPoolSize;
        private int currentPoolSize;
        private int activeConnections;
        private int idleConnections;
        private long totalConnectionsCreated;
        private long totalConnectionsDestroyed;
        private long totalConnectionsAcquired;
        private long totalConnectionsReturned;
        private long totalAcquisitionTimeouts;
        private long totalValidationFailures;
        private double averageAcquisitionTime;
        private double averageConnectionAge;
        private long peakActiveConnections;
        
        public Builder datacenterId(String datacenterId) {
            this.datacenterId = datacenterId;
            return this;
        }
        
        public Builder maxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }
        
        public Builder currentPoolSize(int currentPoolSize) {
            this.currentPoolSize = currentPoolSize;
            return this;
        }
        
        public Builder activeConnections(int activeConnections) {
            this.activeConnections = activeConnections;
            return this;
        }
        
        public Builder idleConnections(int idleConnections) {
            this.idleConnections = idleConnections;
            return this;
        }
        
        public Builder totalConnectionsCreated(long totalConnectionsCreated) {
            this.totalConnectionsCreated = totalConnectionsCreated;
            return this;
        }
        
        public Builder totalConnectionsDestroyed(long totalConnectionsDestroyed) {
            this.totalConnectionsDestroyed = totalConnectionsDestroyed;
            return this;
        }
        
        public Builder totalConnectionsAcquired(long totalConnectionsAcquired) {
            this.totalConnectionsAcquired = totalConnectionsAcquired;
            return this;
        }
        
        public Builder totalConnectionsReturned(long totalConnectionsReturned) {
            this.totalConnectionsReturned = totalConnectionsReturned;
            return this;
        }
        
        public Builder totalAcquisitionTimeouts(long totalAcquisitionTimeouts) {
            this.totalAcquisitionTimeouts = totalAcquisitionTimeouts;
            return this;
        }
        
        public Builder totalValidationFailures(long totalValidationFailures) {
            this.totalValidationFailures = totalValidationFailures;
            return this;
        }
        
        public Builder averageAcquisitionTime(double averageAcquisitionTime) {
            this.averageAcquisitionTime = averageAcquisitionTime;
            return this;
        }
        
        public Builder averageConnectionAge(double averageConnectionAge) {
            this.averageConnectionAge = averageConnectionAge;
            return this;
        }
        
        public Builder peakActiveConnections(long peakActiveConnections) {
            this.peakActiveConnections = peakActiveConnections;
            return this;
        }
        
        public ConnectionPoolMetrics build() {
            return new ConnectionPoolMetrics(this);
        }
    }
    
    @Override
    public String toString() {
        return String.format(
            "ConnectionPoolMetrics{datacenter='%s', maxSize=%d, currentSize=%d, " +
            "active=%d, idle=%d, utilization=%.1f%%, efficiency=%.3f, " +
            "avgAcquisitionTime=%.2fms, avgConnectionAge=%.2fs}",
            datacenterId, maxPoolSize, currentPoolSize, activeConnections, idleConnections,
            getUtilizationPercentage(), getEfficiencyRatio(), 
            averageAcquisitionTime, averageConnectionAge / 1000.0
        );
    }
}
