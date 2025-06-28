package com.redis.multidc.resilience;

import com.redis.multidc.config.ResilienceConfig;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.github.resilience4j.reactor.retry.RetryOperator;
import io.github.resilience4j.reactor.ratelimiter.operator.RateLimiterOperator;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.timelimiter.TimeLimiterOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Comprehensive resilience manager using Resilience4j patterns.
 * Provides circuit breaker, retry, rate limiter, bulkhead, and time limiter
 * functionality for multi-datacenter Redis operations.
 */
public class ResilienceManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ResilienceManager.class);
    
    private final ResilienceConfig config;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final RateLimiterRegistry rateLimiterRegistry;
    private final BulkheadRegistry bulkheadRegistry;
    private final TimeLimiterRegistry timeLimiterRegistry;
    
    public ResilienceManager(ResilienceConfig config) {
        this.config = config;
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(config.getCircuitBreakerConfig());
        this.retryRegistry = RetryRegistry.of(config.getRetryConfig());
        this.rateLimiterRegistry = RateLimiterRegistry.of(config.getRateLimiterConfig());
        this.bulkheadRegistry = BulkheadRegistry.of(config.getBulkheadConfig());
        this.timeLimiterRegistry = TimeLimiterRegistry.of(config.getTimeLimiterConfig());
        
        setupEventHandlers();
    }
    
    /**
     * Get or create a circuit breaker for a specific datacenter.
     */
    public CircuitBreaker getCircuitBreaker(String datacenterId) {
        return circuitBreakerRegistry.circuitBreaker(datacenterId);
    }
    
    /**
     * Get or create a retry instance for a specific datacenter.
     */
    public Retry getRetry(String datacenterId) {
        return retryRegistry.retry(datacenterId);
    }
    
    /**
     * Get or create a rate limiter for a specific datacenter.
     */
    public RateLimiter getRateLimiter(String datacenterId) {
        return rateLimiterRegistry.rateLimiter(datacenterId);
    }
    
    /**
     * Get or create a bulkhead for a specific datacenter.
     */
    public Bulkhead getBulkhead(String datacenterId) {
        return bulkheadRegistry.bulkhead(datacenterId);
    }
    
    /**
     * Get or create a time limiter for a specific datacenter.
     */
    public TimeLimiter getTimeLimiter(String datacenterId) {
        return timeLimiterRegistry.timeLimiter(datacenterId);
    }
    
    /**
     * Decorate a supplier with all enabled resilience patterns.
     */
    public <T> Supplier<T> decorateSupplier(String datacenterId, Supplier<T> supplier) {
        Supplier<T> decoratedSupplier = supplier;
        
        if (config.isCircuitBreakerEnabled()) {
            decoratedSupplier = CircuitBreaker.decorateSupplier(getCircuitBreaker(datacenterId), decoratedSupplier);
        }
        
        if (config.isRetryEnabled()) {
            decoratedSupplier = Retry.decorateSupplier(getRetry(datacenterId), decoratedSupplier);
        }
        
        if (config.isRateLimiterEnabled()) {
            decoratedSupplier = RateLimiter.decorateSupplier(getRateLimiter(datacenterId), decoratedSupplier);
        }
        
        if (config.isBulkheadEnabled()) {
            decoratedSupplier = Bulkhead.decorateSupplier(getBulkhead(datacenterId), decoratedSupplier);
        }
        
        return decoratedSupplier;
    }
    
    /**
     * Decorate a CompletableFuture with all enabled resilience patterns.
     */
    public <T> Supplier<CompletableFuture<T>> decorateCompletionStage(String datacenterId, Supplier<CompletableFuture<T>> supplier) {
        Supplier<CompletableFuture<T>> decoratedSupplier = supplier;
        
        if (config.isCircuitBreakerEnabled()) {
            decoratedSupplier = CircuitBreaker.decorateSupplier(getCircuitBreaker(datacenterId), decoratedSupplier);
        }
        
        if (config.isRetryEnabled()) {
            decoratedSupplier = Retry.decorateSupplier(getRetry(datacenterId), decoratedSupplier);
        }
        
        if (config.isRateLimiterEnabled()) {
            decoratedSupplier = RateLimiter.decorateSupplier(getRateLimiter(datacenterId), decoratedSupplier);
        }
        
        if (config.isBulkheadEnabled()) {
            decoratedSupplier = Bulkhead.decorateSupplier(getBulkhead(datacenterId), decoratedSupplier);
        }
        
        return decoratedSupplier;
    }
    
    /**
     * Decorate a Mono with all enabled resilience patterns.
     */
    public <T> Mono<T> decorateMono(String datacenterId, Mono<T> mono) {
        Mono<T> decoratedMono = mono;
        
        if (config.isCircuitBreakerEnabled()) {
            decoratedMono = decoratedMono.transformDeferred(CircuitBreakerOperator.of(getCircuitBreaker(datacenterId)));
        }
        
        if (config.isRetryEnabled()) {
            decoratedMono = decoratedMono.transformDeferred(RetryOperator.of(getRetry(datacenterId)));
        }
        
        if (config.isRateLimiterEnabled()) {
            decoratedMono = decoratedMono.transformDeferred(RateLimiterOperator.of(getRateLimiter(datacenterId)));
        }
        
        if (config.isBulkheadEnabled()) {
            decoratedMono = decoratedMono.transformDeferred(BulkheadOperator.of(getBulkhead(datacenterId)));
        }
        
        if (config.isTimeLimiterEnabled()) {
            decoratedMono = decoratedMono.transformDeferred(TimeLimiterOperator.of(getTimeLimiter(datacenterId)));
        }
        
        return decoratedMono;
    }
    
    /**
     * Decorate a Flux with all enabled resilience patterns.
     */
    public <T> Flux<T> decorateFlux(String datacenterId, Flux<T> flux) {
        Flux<T> decoratedFlux = flux;
        
        if (config.isCircuitBreakerEnabled()) {
            decoratedFlux = decoratedFlux.transformDeferred(CircuitBreakerOperator.of(getCircuitBreaker(datacenterId)));
        }
        
        if (config.isRetryEnabled()) {
            decoratedFlux = decoratedFlux.transformDeferred(RetryOperator.of(getRetry(datacenterId)));
        }
        
        if (config.isRateLimiterEnabled()) {
            decoratedFlux = decoratedFlux.transformDeferred(RateLimiterOperator.of(getRateLimiter(datacenterId)));
        }
        
        if (config.isBulkheadEnabled()) {
            decoratedFlux = decoratedFlux.transformDeferred(BulkheadOperator.of(getBulkhead(datacenterId)));
        }
        
        if (config.isTimeLimiterEnabled()) {
            decoratedFlux = decoratedFlux.transformDeferred(TimeLimiterOperator.of(getTimeLimiter(datacenterId)));
        }
        
        return decoratedFlux;
    }
    
    /**
     * Get the current state of the circuit breaker for a datacenter.
     */
    public CircuitBreaker.State getCircuitBreakerState(String datacenterId) {
        return getCircuitBreaker(datacenterId).getState();
    }
    
    /**
     * Check if the circuit breaker is open for a datacenter.
     */
    public boolean isCircuitBreakerOpen(String datacenterId) {
        return getCircuitBreaker(datacenterId).getState() == CircuitBreaker.State.OPEN;
    }
    
    /**
     * Check if the circuit breaker is closed for a datacenter.
     */
    public boolean isCircuitBreakerClosed(String datacenterId) {
        return getCircuitBreaker(datacenterId).getState() == CircuitBreaker.State.CLOSED;
    }
    
    /**
     * Check if the circuit breaker is half-open for a datacenter.
     */
    public boolean isCircuitBreakerHalfOpen(String datacenterId) {
        return getCircuitBreaker(datacenterId).getState() == CircuitBreaker.State.HALF_OPEN;
    }
    
    /**
     * Force the circuit breaker to transition to open state.
     */
    public void openCircuitBreaker(String datacenterId) {
        getCircuitBreaker(datacenterId).transitionToOpenState();
        logger.warn("Circuit breaker for datacenter {} forced to OPEN state", datacenterId);
    }
    
    /**
     * Force the circuit breaker to transition to closed state.
     */
    public void closeCircuitBreaker(String datacenterId) {
        getCircuitBreaker(datacenterId).transitionToClosedState();
        logger.info("Circuit breaker for datacenter {} forced to CLOSED state", datacenterId);
    }
    
    /**
     * Reset circuit breaker metrics for a datacenter.
     */
    public void resetCircuitBreaker(String datacenterId) {
        getCircuitBreaker(datacenterId).reset();
        logger.info("Circuit breaker for datacenter {} has been reset", datacenterId);
    }
    
    /**
     * Get resilience configuration.
     */
    public ResilienceConfig getConfig() {
        return config;
    }
    
    private void setupEventHandlers() {
        // Circuit breaker event handlers
        circuitBreakerRegistry.getEventPublisher().onEntryAdded(event -> {
            CircuitBreaker circuitBreaker = event.getAddedEntry();
            String name = circuitBreaker.getName();
            
            circuitBreaker.getEventPublisher()
                    .onStateTransition(e -> logger.info("Circuit breaker {} state transition: {} -> {}", 
                            name, e.getStateTransition().getFromState(), e.getStateTransition().getToState()))
                    .onFailureRateExceeded(e -> logger.warn("Circuit breaker {} failure rate exceeded: {}%", 
                            name, e.getFailureRate()))
                    .onSlowCallRateExceeded(e -> logger.warn("Circuit breaker {} slow call rate exceeded: {}%", 
                            name, e.getSlowCallRate()))
                    .onCallNotPermitted(e -> logger.debug("Circuit breaker {} call not permitted", name));
        });
        
        // Retry event handlers
        retryRegistry.getEventPublisher().onEntryAdded(event -> {
            Retry retry = event.getAddedEntry();
            String name = retry.getName();
            
            retry.getEventPublisher()
                    .onRetry(e -> logger.debug("Retry {} attempt {} for exception: {}", 
                            name, e.getNumberOfRetryAttempts(), e.getLastThrowable().getMessage()))
                    .onSuccess(e -> logger.debug("Retry {} succeeded after {} attempts", 
                            name, e.getNumberOfRetryAttempts()))
                    .onError(e -> logger.warn("Retry {} failed after {} attempts: {}", 
                            name, e.getNumberOfRetryAttempts(), e.getLastThrowable().getMessage()));
        });
        
        // Rate limiter event handlers
        rateLimiterRegistry.getEventPublisher().onEntryAdded(event -> {
            RateLimiter rateLimiter = event.getAddedEntry();
            String name = rateLimiter.getName();
            
            rateLimiter.getEventPublisher()
                    .onSuccess(e -> logger.debug("Rate limiter {} permission acquired", name))
                    .onFailure(e -> logger.debug("Rate limiter {} permission denied", name));
        });
        
        // Bulkhead event handlers
        bulkheadRegistry.getEventPublisher().onEntryAdded(event -> {
            Bulkhead bulkhead = event.getAddedEntry();
            String name = bulkhead.getName();
            
            bulkhead.getEventPublisher()
                    .onCallPermitted(e -> logger.debug("Bulkhead {} call permitted", name))
                    .onCallRejected(e -> logger.debug("Bulkhead {} call rejected", name))
                    .onCallFinished(e -> logger.debug("Bulkhead {} call finished", name));
        });
    }
}
