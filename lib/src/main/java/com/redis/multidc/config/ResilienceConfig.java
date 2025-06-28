package com.redis.multidc.config;

import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;

import java.time.Duration;

/**
 * Comprehensive resilience configuration using Resilience4j patterns.
 * Provides circuit breaker, retry, rate limiter, bulkhead, and time limiter configurations
 * for robust multi-datacenter Redis operations.
 */
public class ResilienceConfig {
    
    private final CircuitBreakerConfig circuitBreakerConfig;
    private final RetryConfig retryConfig;
    private final RateLimiterConfig rateLimiterConfig;
    private final BulkheadConfig bulkheadConfig;
    private final TimeLimiterConfig timeLimiterConfig;
    private final boolean enableCircuitBreaker;
    private final boolean enableRetry;
    private final boolean enableRateLimiter;
    private final boolean enableBulkhead;
    private final boolean enableTimeLimiter;
    
    private ResilienceConfig(Builder builder) {
        this.circuitBreakerConfig = builder.circuitBreakerConfig;
        this.retryConfig = builder.retryConfig;
        this.rateLimiterConfig = builder.rateLimiterConfig;
        this.bulkheadConfig = builder.bulkheadConfig;
        this.timeLimiterConfig = builder.timeLimiterConfig;
        this.enableCircuitBreaker = builder.enableCircuitBreaker;
        this.enableRetry = builder.enableRetry;
        this.enableRateLimiter = builder.enableRateLimiter;
        this.enableBulkhead = builder.enableBulkhead;
        this.enableTimeLimiter = builder.enableTimeLimiter;
    }
    
    public CircuitBreakerConfig getCircuitBreakerConfig() {
        return circuitBreakerConfig;
    }
    
    public RetryConfig getRetryConfig() {
        return retryConfig;
    }
    
    public RateLimiterConfig getRateLimiterConfig() {
        return rateLimiterConfig;
    }
    
    public BulkheadConfig getBulkheadConfig() {
        return bulkheadConfig;
    }
    
    public TimeLimiterConfig getTimeLimiterConfig() {
        return timeLimiterConfig;
    }
    
    public boolean isCircuitBreakerEnabled() {
        return enableCircuitBreaker;
    }
    
    public boolean isRetryEnabled() {
        return enableRetry;
    }
    
    public boolean isRateLimiterEnabled() {
        return enableRateLimiter;
    }
    
    public boolean isBulkheadEnabled() {
        return enableBulkhead;
    }
    
    public boolean isTimeLimiterEnabled() {
        return enableTimeLimiter;
    }
    
    /**
     * Creates a default resilience configuration suitable for production Redis operations.
     */
    public static ResilienceConfig defaultConfig() {
        return builder().build();
    }
    
    /**
     * Creates a relaxed configuration suitable for development or testing.
     */
    public static ResilienceConfig relaxedConfig() {
        return builder()
                .circuitBreaker(CircuitBreakerConfig.custom()
                        .failureRateThreshold(80.0f)
                        .waitDurationInOpenState(Duration.ofSeconds(30))
                        .slidingWindowSize(20)
                        .minimumNumberOfCalls(5)
                        .build())
                .retry(RetryConfig.custom()
                        .maxAttempts(2)
                        .waitDuration(Duration.ofMillis(100))
                        .build())
                .enableCircuitBreaker(true)
                .enableRetry(true)
                .enableRateLimiter(false)
                .enableBulkhead(false)
                .enableTimeLimiter(false)
                .build();
    }
    
    /**
     * Creates a high-throughput configuration for performance-critical scenarios.
     */
    public static ResilienceConfig highThroughputConfig() {
        return builder()
                .circuitBreaker(CircuitBreakerConfig.custom()
                        .failureRateThreshold(75.0f)
                        .waitDurationInOpenState(Duration.ofSeconds(20))
                        .slidingWindowSize(100)
                        .minimumNumberOfCalls(20)
                        .build())
                .rateLimiter(RateLimiterConfig.custom()
                        .limitForPeriod(1000)
                        .limitRefreshPeriod(Duration.ofSeconds(1))
                        .timeoutDuration(Duration.ofMillis(10))
                        .build())
                .bulkhead(BulkheadConfig.custom()
                        .maxConcurrentCalls(50)
                        .maxWaitDuration(Duration.ofMillis(100))
                        .build())
                .enableCircuitBreaker(true)
                .enableRetry(false)
                .enableRateLimiter(true)
                .enableBulkhead(true)
                .enableTimeLimiter(false)
                .build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private CircuitBreakerConfig circuitBreakerConfig;
        private RetryConfig retryConfig;
        private RateLimiterConfig rateLimiterConfig;
        private BulkheadConfig bulkheadConfig;
        private TimeLimiterConfig timeLimiterConfig;
        private boolean enableCircuitBreaker = true;
        private boolean enableRetry = true;
        private boolean enableRateLimiter = false;
        private boolean enableBulkhead = false;
        private boolean enableTimeLimiter = false;
        
        public Builder() {
            // Default configurations
            this.circuitBreakerConfig = CircuitBreakerConfig.custom()
                    .failureRateThreshold(50.0f)
                    .waitDurationInOpenState(Duration.ofSeconds(60))
                    .slidingWindowSize(10)
                    .minimumNumberOfCalls(5)
                    .permittedNumberOfCallsInHalfOpenState(3)
                    .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                    .recordException(throwable -> true) // Record all exceptions as failures
                    .build();
                    
            this.retryConfig = RetryConfig.custom()
                    .maxAttempts(3)
                    .waitDuration(Duration.ofMillis(500))
                    .retryOnException(throwable -> true) // Retry on all exceptions
                    .build();
                    
            this.rateLimiterConfig = RateLimiterConfig.custom()
                    .limitForPeriod(100)
                    .limitRefreshPeriod(Duration.ofSeconds(1))
                    .timeoutDuration(Duration.ofMillis(500))
                    .build();
                    
            this.bulkheadConfig = BulkheadConfig.custom()
                    .maxConcurrentCalls(25)
                    .maxWaitDuration(Duration.ofMillis(100))
                    .build();
                    
            this.timeLimiterConfig = TimeLimiterConfig.custom()
                    .timeoutDuration(Duration.ofSeconds(10))
                    .cancelRunningFuture(true)
                    .build();
        }
        
        public Builder circuitBreaker(CircuitBreakerConfig config) {
            this.circuitBreakerConfig = config;
            return this;
        }
        
        public Builder retry(RetryConfig config) {
            this.retryConfig = config;
            return this;
        }
        
        public Builder rateLimiter(RateLimiterConfig config) {
            this.rateLimiterConfig = config;
            return this;
        }
        
        public Builder bulkhead(BulkheadConfig config) {
            this.bulkheadConfig = config;
            return this;
        }
        
        public Builder timeLimiter(TimeLimiterConfig config) {
            this.timeLimiterConfig = config;
            return this;
        }
        
        public Builder enableCircuitBreaker(boolean enable) {
            this.enableCircuitBreaker = enable;
            return this;
        }
        
        public Builder enableRetry(boolean enable) {
            this.enableRetry = enable;
            return this;
        }
        
        public Builder enableRateLimiter(boolean enable) {
            this.enableRateLimiter = enable;
            return this;
        }
        
        public Builder enableBulkhead(boolean enable) {
            this.enableBulkhead = enable;
            return this;
        }
        
        public Builder enableTimeLimiter(boolean enable) {
            this.enableTimeLimiter = enable;
            return this;
        }
        
        /**
         * Convenience method to configure circuit breaker with common parameters.
         */
        public Builder circuitBreakerConfig(
                float failureRateThreshold, 
                Duration waitDurationInOpenState, 
                int slidingWindowSize,
                int minimumNumberOfCalls) {
            this.circuitBreakerConfig = CircuitBreakerConfig.custom()
                    .failureRateThreshold(failureRateThreshold)
                    .waitDurationInOpenState(waitDurationInOpenState)
                    .slidingWindowSize(slidingWindowSize)
                    .minimumNumberOfCalls(minimumNumberOfCalls)
                    .build();
            return this;
        }
        
        /**
         * Convenience method to configure retry with common parameters.
         */
        public Builder retryConfig(int maxAttempts, Duration waitDuration) {
            this.retryConfig = RetryConfig.custom()
                    .maxAttempts(maxAttempts)
                    .waitDuration(waitDuration)
                    .build();
            return this;
        }
        
        /**
         * Convenience method to configure rate limiter with common parameters.
         */
        public Builder rateLimiterConfig(int limitForPeriod, Duration refreshPeriod, Duration timeoutDuration) {
            this.rateLimiterConfig = RateLimiterConfig.custom()
                    .limitForPeriod(limitForPeriod)
                    .limitRefreshPeriod(refreshPeriod)
                    .timeoutDuration(timeoutDuration)
                    .build();
            return this;
        }
        
        /**
         * Convenience method to configure bulkhead with common parameters.
         */
        public Builder bulkheadConfig(int maxConcurrentCalls, Duration maxWaitDuration) {
            this.bulkheadConfig = BulkheadConfig.custom()
                    .maxConcurrentCalls(maxConcurrentCalls)
                    .maxWaitDuration(maxWaitDuration)
                    .build();
            return this;
        }
        
        /**
         * Convenience method to configure time limiter.
         */
        public Builder timeLimiterConfig(Duration timeoutDuration, boolean cancelRunningFuture) {
            this.timeLimiterConfig = TimeLimiterConfig.custom()
                    .timeoutDuration(timeoutDuration)
                    .cancelRunningFuture(cancelRunningFuture)
                    .build();
            return this;
        }
        
        /**
         * Enable all resilience patterns with default configurations.
         */
        public Builder enableAllPatterns() {
            this.enableCircuitBreaker = true;
            this.enableRetry = true;
            this.enableRateLimiter = true;
            this.enableBulkhead = true;
            this.enableTimeLimiter = true;
            return this;
        }
        
        /**
         * Enable only basic resilience patterns (circuit breaker and retry).
         */
        public Builder enableBasicPatterns() {
            this.enableCircuitBreaker = true;
            this.enableRetry = true;
            this.enableRateLimiter = false;
            this.enableBulkhead = false;
            this.enableTimeLimiter = false;
            return this;
        }
        
        public ResilienceConfig build() {
            return new ResilienceConfig(this);
        }
    }
}
