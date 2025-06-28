package com.redis.multidc.pool;

import java.time.Duration;

/**
 * Configuration for Redis connection pools.
 * Defines pool sizing, timeouts, and lifecycle parameters.
 */
public class ConnectionPoolConfig {
    
    private final int minPoolSize;
    private final int maxPoolSize;
    private final Duration connectionTimeout;
    private final Duration idleTimeout;
    private final Duration maxConnectionAge;
    private final Duration acquisitionTimeout;
    private final Duration validationTimeout;
    private final boolean validateOnAcquire;
    private final boolean validateOnReturn;
    private final Duration maintenanceInterval;
    private final boolean enableMetrics;
    private final int maintenanceCorePoolSize;
    
    private ConnectionPoolConfig(Builder builder) {
        this.minPoolSize = builder.minPoolSize;
        this.maxPoolSize = builder.maxPoolSize;
        this.connectionTimeout = builder.connectionTimeout;
        this.idleTimeout = builder.idleTimeout;
        this.maxConnectionAge = builder.maxConnectionAge;
        this.acquisitionTimeout = builder.acquisitionTimeout;
        this.validationTimeout = builder.validationTimeout;
        this.validateOnAcquire = builder.validateOnAcquire;
        this.validateOnReturn = builder.validateOnReturn;
        this.maintenanceInterval = builder.maintenanceInterval;
        this.enableMetrics = builder.enableMetrics;
        this.maintenanceCorePoolSize = builder.maintenanceCorePoolSize;
    }
    
    public int getMinPoolSize() {
        return minPoolSize;
    }
    
    public int getMaxPoolSize() {
        return maxPoolSize;
    }
    
    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }
    
    public Duration getIdleTimeout() {
        return idleTimeout;
    }
    
    public Duration getMaxConnectionAge() {
        return maxConnectionAge;
    }
    
    public Duration getAcquisitionTimeout() {
        return acquisitionTimeout;
    }
    
    public Duration getValidationTimeout() {
        return validationTimeout;
    }
    
    public boolean isValidateOnAcquire() {
        return validateOnAcquire;
    }
    
    public boolean isValidateOnReturn() {
        return validateOnReturn;
    }
    
    public Duration getMaintenanceInterval() {
        return maintenanceInterval;
    }
    
    public boolean isEnableMetrics() {
        return enableMetrics;
    }
    
    public int getMaintenanceCorePoolSize() {
        return maintenanceCorePoolSize;
    }
    
    public static ConnectionPoolConfig defaultConfig() {
        return builder().build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private int minPoolSize = 5;
        private int maxPoolSize = 20;
        private Duration connectionTimeout = Duration.ofSeconds(5);
        private Duration idleTimeout = Duration.ofMinutes(10);
        private Duration maxConnectionAge = Duration.ofHours(1);
        private Duration acquisitionTimeout = Duration.ofSeconds(10);
        private Duration validationTimeout = Duration.ofSeconds(3);
        private boolean validateOnAcquire = true;
        private boolean validateOnReturn = false;
        private Duration maintenanceInterval = Duration.ofMinutes(1);
        private boolean enableMetrics = true;
        private int maintenanceCorePoolSize = 1;
        
        /**
         * Sets the minimum number of connections to maintain in the pool.
         * 
         * @param minPoolSize minimum pool size (must be >= 0)
         * @return this builder
         */
        public Builder minPoolSize(int minPoolSize) {
            if (minPoolSize < 0) {
                throw new IllegalArgumentException("minPoolSize must be >= 0");
            }
            this.minPoolSize = minPoolSize;
            return this;
        }
        
        /**
         * Sets the maximum number of connections allowed in the pool.
         * 
         * @param maxPoolSize maximum pool size (must be > 0)
         * @return this builder
         */
        public Builder maxPoolSize(int maxPoolSize) {
            if (maxPoolSize <= 0) {
                throw new IllegalArgumentException("maxPoolSize must be > 0");
            }
            this.maxPoolSize = maxPoolSize;
            return this;
        }
        
        /**
         * Sets the timeout for creating new connections.
         * 
         * @param connectionTimeout connection creation timeout
         * @return this builder
         */
        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }
        
        /**
         * Sets the idle timeout after which unused connections are closed.
         * 
         * @param idleTimeout idle timeout duration
         * @return this builder
         */
        public Builder idleTimeout(Duration idleTimeout) {
            this.idleTimeout = idleTimeout;
            return this;
        }
        
        /**
         * Sets the maximum age for connections before they are replaced.
         * 
         * @param maxConnectionAge maximum connection age
         * @return this builder
         */
        public Builder maxConnectionAge(Duration maxConnectionAge) {
            this.maxConnectionAge = maxConnectionAge;
            return this;
        }
        
        /**
         * Sets the timeout for acquiring connections from the pool.
         * 
         * @param acquisitionTimeout acquisition timeout
         * @return this builder
         */
        public Builder acquisitionTimeout(Duration acquisitionTimeout) {
            this.acquisitionTimeout = acquisitionTimeout;
            return this;
        }
        
        /**
         * Sets the timeout for connection validation operations.
         * 
         * @param validationTimeout validation timeout
         * @return this builder
         */
        public Builder validationTimeout(Duration validationTimeout) {
            this.validationTimeout = validationTimeout;
            return this;
        }
        
        /**
         * Sets whether to validate connections when acquiring from pool.
         * 
         * @param validateOnAcquire true to validate on acquire
         * @return this builder
         */
        public Builder validateOnAcquire(boolean validateOnAcquire) {
            this.validateOnAcquire = validateOnAcquire;
            return this;
        }
        
        /**
         * Sets whether to validate connections when returning to pool.
         * 
         * @param validateOnReturn true to validate on return
         * @return this builder
         */
        public Builder validateOnReturn(boolean validateOnReturn) {
            this.validateOnReturn = validateOnReturn;
            return this;
        }
        
        /**
         * Sets the interval for pool maintenance operations.
         * 
         * @param maintenanceInterval maintenance interval
         * @return this builder
         */
        public Builder maintenanceInterval(Duration maintenanceInterval) {
            this.maintenanceInterval = maintenanceInterval;
            return this;
        }
        
        /**
         * Sets whether to enable detailed metrics collection.
         * 
         * @param enableMetrics true to enable metrics
         * @return this builder
         */
        public Builder enableMetrics(boolean enableMetrics) {
            this.enableMetrics = enableMetrics;
            return this;
        }
        
        /**
         * Sets the core pool size for maintenance thread pool.
         * 
         * @param maintenanceCorePoolSize core pool size for maintenance
         * @return this builder
         */
        public Builder maintenanceCorePoolSize(int maintenanceCorePoolSize) {
            if (maintenanceCorePoolSize <= 0) {
                throw new IllegalArgumentException("maintenanceCorePoolSize must be > 0");
            }
            this.maintenanceCorePoolSize = maintenanceCorePoolSize;
            return this;
        }
        
        /**
         * Convenience method to configure for high-throughput scenarios.
         * 
         * @return this builder configured for high throughput
         */
        public Builder highThroughput() {
            return maxPoolSize(50)
                   .minPoolSize(10)
                   .acquisitionTimeout(Duration.ofSeconds(5))
                   .idleTimeout(Duration.ofMinutes(5))
                   .validateOnAcquire(false)
                   .maintenanceInterval(Duration.ofSeconds(30));
        }
        
        /**
         * Convenience method to configure for low-latency scenarios.
         * 
         * @return this builder configured for low latency
         */
        public Builder lowLatency() {
            return maxPoolSize(100)
                   .minPoolSize(20)
                   .acquisitionTimeout(Duration.ofSeconds(1))
                   .connectionTimeout(Duration.ofSeconds(2))
                   .validateOnAcquire(true)
                   .maintenanceInterval(Duration.ofSeconds(15));
        }
        
        /**
         * Convenience method to configure for resource-constrained environments.
         * 
         * @return this builder configured for minimal resource usage
         */
        public Builder resourceConstrained() {
            return maxPoolSize(5)
                   .minPoolSize(1)
                   .idleTimeout(Duration.ofMinutes(2))
                   .maxConnectionAge(Duration.ofMinutes(30))
                   .maintenanceInterval(Duration.ofMinutes(5));
        }
        
        public ConnectionPoolConfig build() {
            if (minPoolSize > maxPoolSize) {
                throw new IllegalArgumentException("minPoolSize cannot be greater than maxPoolSize");
            }
            return new ConnectionPoolConfig(this);
        }
    }
    
    @Override
    public String toString() {
        return String.format(
            "ConnectionPoolConfig{minSize=%d, maxSize=%d, connectionTimeout=%s, " +
            "idleTimeout=%s, maxAge=%s, acquisitionTimeout=%s, validateOnAcquire=%s}",
            minPoolSize, maxPoolSize, connectionTimeout, idleTimeout, 
            maxConnectionAge, acquisitionTimeout, validateOnAcquire
        );
    }
}
