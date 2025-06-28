package com.redis.multidc.config;

import java.time.Duration;

/**
 * Circuit breaker configuration for datacenter fault tolerance.
 */
public class CircuitBreakerConfig {
    
    private final int failureThreshold;
    private final int successThreshold;
    private final Duration openTimeout;
    private final Duration halfOpenTimeout;
    private final double failureRateThreshold;
    private final int minimumNumberOfCalls;
    private final Duration slidingWindowDuration;
    
    private CircuitBreakerConfig(Builder builder) {
        this.failureThreshold = builder.failureThreshold;
        this.successThreshold = builder.successThreshold;
        this.openTimeout = builder.openTimeout;
        this.halfOpenTimeout = builder.halfOpenTimeout;
        this.failureRateThreshold = builder.failureRateThreshold;
        this.minimumNumberOfCalls = builder.minimumNumberOfCalls;
        this.slidingWindowDuration = builder.slidingWindowDuration;
    }
    
    public int getFailureThreshold() {
        return failureThreshold;
    }
    
    public int getSuccessThreshold() {
        return successThreshold;
    }
    
    public Duration getOpenTimeout() {
        return openTimeout;
    }
    
    public Duration getHalfOpenTimeout() {
        return halfOpenTimeout;
    }
    
    public double getFailureRateThreshold() {
        return failureRateThreshold;
    }
    
    public int getMinimumNumberOfCalls() {
        return minimumNumberOfCalls;
    }
    
    public Duration getSlidingWindowDuration() {
        return slidingWindowDuration;
    }
    
    public static CircuitBreakerConfig defaultConfig() {
        return builder().build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private int failureThreshold = 5;
        private int successThreshold = 3;
        private Duration openTimeout = Duration.ofSeconds(60);
        private Duration halfOpenTimeout = Duration.ofSeconds(30);
        private double failureRateThreshold = 50.0;
        private int minimumNumberOfCalls = 10;
        private Duration slidingWindowDuration = Duration.ofMinutes(1);
        
        public Builder failureThreshold(int threshold) {
            this.failureThreshold = threshold;
            return this;
        }
        
        public Builder successThreshold(int threshold) {
            this.successThreshold = threshold;
            return this;
        }
        
        public Builder openTimeout(Duration timeout) {
            this.openTimeout = timeout;
            return this;
        }
        
        public Builder halfOpenTimeout(Duration timeout) {
            this.halfOpenTimeout = timeout;
            return this;
        }
        
        public Builder failureRateThreshold(double threshold) {
            this.failureRateThreshold = threshold;
            return this;
        }
        
        public Builder minimumNumberOfCalls(int calls) {
            this.minimumNumberOfCalls = calls;
            return this;
        }
        
        public Builder slidingWindowDuration(Duration duration) {
            this.slidingWindowDuration = duration;
            return this;
        }
        
        public CircuitBreakerConfig build() {
            return new CircuitBreakerConfig(this);
        }
    }
}
