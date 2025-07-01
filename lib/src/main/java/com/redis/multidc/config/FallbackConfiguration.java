package com.redis.multidc.config;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

/**
 * Configuration for fallback behavior when primary datacenter routing fails.
 * This class encapsulates all fallback-related settings including strategy,
 * timeouts, retry policies, and custom fallback logic.
 */
public class FallbackConfiguration {
    
    private final FallbackStrategy strategy;
    private final Duration fallbackTimeout;
    private final int maxFallbackAttempts;
    private final Duration fallbackRetryDelay;
    private final List<String> fallbackDatacenterOrder;
    private final boolean enableStaleReads;
    private final Duration staleReadTolerance;
    private final int queueSize;
    private final Duration queueTimeout;
    private final Supplier<String> customFallbackLogic;
    
    private FallbackConfiguration(Builder builder) {
        this.strategy = builder.strategy;
        this.fallbackTimeout = builder.fallbackTimeout;
        this.maxFallbackAttempts = builder.maxFallbackAttempts;
        this.fallbackRetryDelay = builder.fallbackRetryDelay;
        this.fallbackDatacenterOrder = List.copyOf(builder.fallbackDatacenterOrder);
        this.enableStaleReads = builder.enableStaleReads;
        this.staleReadTolerance = builder.staleReadTolerance;
        this.queueSize = builder.queueSize;
        this.queueTimeout = builder.queueTimeout;
        this.customFallbackLogic = builder.customFallbackLogic;
    }
    
    public FallbackStrategy getStrategy() {
        return strategy;
    }
    
    public Duration getFallbackTimeout() {
        return fallbackTimeout;
    }
    
    public int getMaxFallbackAttempts() {
        return maxFallbackAttempts;
    }
    
    public Duration getFallbackRetryDelay() {
        return fallbackRetryDelay;
    }
    
    public List<String> getFallbackDatacenterOrder() {
        return fallbackDatacenterOrder;
    }
    
    public boolean isEnableStaleReads() {
        return enableStaleReads;
    }
    
    public Duration getStaleReadTolerance() {
        return staleReadTolerance;
    }
    
    public int getQueueSize() {
        return queueSize;
    }
    
    public Duration getQueueTimeout() {
        return queueTimeout;
    }
    
    public Supplier<String> getCustomFallbackLogic() {
        return customFallbackLogic;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static FallbackConfiguration defaultConfig() {
        return builder().build();
    }
    
    public static class Builder {
        private FallbackStrategy strategy = FallbackStrategy.NEXT_AVAILABLE;
        private Duration fallbackTimeout = Duration.ofSeconds(5);
        private int maxFallbackAttempts = 3;
        private Duration fallbackRetryDelay = Duration.ofMillis(100);
        private List<String> fallbackDatacenterOrder = List.of();
        private boolean enableStaleReads = false;
        private Duration staleReadTolerance = Duration.ofSeconds(30);
        private int queueSize = 1000;
        private Duration queueTimeout = Duration.ofSeconds(10);
        private Supplier<String> customFallbackLogic;
        
        public Builder strategy(FallbackStrategy strategy) {
            this.strategy = strategy;
            return this;
        }
        
        public Builder fallbackTimeout(Duration timeout) {
            this.fallbackTimeout = timeout;
            return this;
        }
        
        public Builder maxFallbackAttempts(int attempts) {
            this.maxFallbackAttempts = attempts;
            return this;
        }
        
        public Builder fallbackRetryDelay(Duration delay) {
            this.fallbackRetryDelay = delay;
            return this;
        }
        
        public Builder fallbackDatacenterOrder(List<String> order) {
            this.fallbackDatacenterOrder = order;
            return this;
        }
        
        public Builder enableStaleReads(boolean enable) {
            this.enableStaleReads = enable;
            return this;
        }
        
        public Builder staleReadTolerance(Duration tolerance) {
            this.staleReadTolerance = tolerance;
            return this;
        }
        
        public Builder queueSize(int size) {
            this.queueSize = size;
            return this;
        }
        
        public Builder queueTimeout(Duration timeout) {
            this.queueTimeout = timeout;
            return this;
        }
        
        public Builder customFallbackLogic(Supplier<String> logic) {
            this.customFallbackLogic = logic;
            return this;
        }
        
        public FallbackConfiguration build() {
            if (strategy == FallbackStrategy.CUSTOM && customFallbackLogic == null) {
                throw new IllegalArgumentException("Custom fallback logic must be provided when using CUSTOM strategy");
            }
            return new FallbackConfiguration(this);
        }
    }
}
