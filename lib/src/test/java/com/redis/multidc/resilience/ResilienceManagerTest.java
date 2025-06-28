package com.redis.multidc.resilience;

import com.redis.multidc.config.ResilienceConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.timelimiter.TimeLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ResilienceManager using Resilience4j.
 */
public class ResilienceManagerTest {

    private ResilienceManager resilienceManager;
    private static final String DATACENTER_ID = "test-dc";

    @BeforeEach
    public void setUp() {
        ResilienceConfig config = ResilienceConfig.builder()
                .enableBasicPatterns()
                .build();
        resilienceManager = new ResilienceManager(config);
    }

    @Test
    public void testGetCircuitBreaker() {
        CircuitBreaker circuitBreaker = resilienceManager.getCircuitBreaker(DATACENTER_ID);
        
        assertNotNull(circuitBreaker);
        assertEquals(DATACENTER_ID, circuitBreaker.getName());
        assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
        assertTrue(resilienceManager.isCircuitBreakerClosed(DATACENTER_ID));
        assertFalse(resilienceManager.isCircuitBreakerOpen(DATACENTER_ID));
        assertFalse(resilienceManager.isCircuitBreakerHalfOpen(DATACENTER_ID));
    }

    @Test
    public void testGetRetry() {
        Retry retry = resilienceManager.getRetry(DATACENTER_ID);
        
        assertNotNull(retry);
        assertEquals(DATACENTER_ID, retry.getName());
    }

    @Test
    public void testGetRateLimiter() {
        RateLimiter rateLimiter = resilienceManager.getRateLimiter(DATACENTER_ID);
        
        assertNotNull(rateLimiter);
        assertEquals(DATACENTER_ID, rateLimiter.getName());
    }

    @Test
    public void testGetBulkhead() {
        Bulkhead bulkhead = resilienceManager.getBulkhead(DATACENTER_ID);
        
        assertNotNull(bulkhead);
        assertEquals(DATACENTER_ID, bulkhead.getName());
    }

    @Test
    public void testGetTimeLimiter() {
        TimeLimiter timeLimiter = resilienceManager.getTimeLimiter(DATACENTER_ID);
        
        assertNotNull(timeLimiter);
        assertEquals(DATACENTER_ID, timeLimiter.getName());
    }

    @Test
    public void testDecorateSupplierWithSuccess() {
        Supplier<String> supplier = () -> "success";
        Supplier<String> decoratedSupplier = resilienceManager.decorateSupplier(DATACENTER_ID, supplier);
        
        String result = decoratedSupplier.get();
        assertEquals("success", result);
    }

    @Test
    public void testDecorateSupplierWithRetry() {
        final int[] attempts = {0};
        Supplier<String> supplier = () -> {
            attempts[0]++;
            if (attempts[0] < 3) {
                throw new RuntimeException("Temporary failure");
            }
            return "success";
        };
        
        Supplier<String> decoratedSupplier = resilienceManager.decorateSupplier(DATACENTER_ID, supplier);
        String result = decoratedSupplier.get();
        
        assertEquals("success", result);
        assertEquals(3, attempts[0]); // Should have retried
    }

    @Test
    public void testDecorateMonoWithSuccess() {
        Mono<String> mono = Mono.just("success");
        Mono<String> decoratedMono = resilienceManager.decorateMono(DATACENTER_ID, mono);
        
        StepVerifier.create(decoratedMono)
                .expectNext("success")
                .verifyComplete();
    }

    @Test
    public void testDecorateMonoWithRetry() {
        final int[] attempts = {0};
        Mono<String> mono = Mono.fromSupplier(() -> {
            attempts[0]++;
            if (attempts[0] < 3) {
                throw new RuntimeException("Temporary failure");
            }
            return "success";
        });
        
        Mono<String> decoratedMono = resilienceManager.decorateMono(DATACENTER_ID, mono);
        
        StepVerifier.create(decoratedMono)
                .expectNext("success")
                .verifyComplete();
        
        assertEquals(3, attempts[0]); // Should have retried
    }

    @Test
    public void testDecorateFluxWithSuccess() {
        Flux<String> flux = Flux.just("item1", "item2", "item3");
        Flux<String> decoratedFlux = resilienceManager.decorateFlux(DATACENTER_ID, flux);
        
        StepVerifier.create(decoratedFlux)
                .expectNext("item1", "item2", "item3")
                .verifyComplete();
    }

    @Test
    public void testCircuitBreakerStateTransitions() {
        CircuitBreaker circuitBreaker = resilienceManager.getCircuitBreaker(DATACENTER_ID);
        
        // Initially closed
        assertEquals(CircuitBreaker.State.CLOSED, resilienceManager.getCircuitBreakerState(DATACENTER_ID));
        
        // Force open
        resilienceManager.openCircuitBreaker(DATACENTER_ID);
        assertEquals(CircuitBreaker.State.OPEN, resilienceManager.getCircuitBreakerState(DATACENTER_ID));
        assertTrue(resilienceManager.isCircuitBreakerOpen(DATACENTER_ID));
        
        // Force closed
        resilienceManager.closeCircuitBreaker(DATACENTER_ID);
        assertEquals(CircuitBreaker.State.CLOSED, resilienceManager.getCircuitBreakerState(DATACENTER_ID));
        assertTrue(resilienceManager.isCircuitBreakerClosed(DATACENTER_ID));
        
        // Reset
        resilienceManager.resetCircuitBreaker(DATACENTER_ID);
        assertEquals(CircuitBreaker.State.CLOSED, resilienceManager.getCircuitBreakerState(DATACENTER_ID));
    }

    @Test
    public void testDecorateCompletionStage() {
        Supplier<CompletableFuture<String>> supplier = () -> CompletableFuture.completedFuture("success");
        Supplier<CompletableFuture<String>> decoratedSupplier = resilienceManager.decorateCompletionStage(DATACENTER_ID, supplier);
        
        CompletableFuture<String> result = decoratedSupplier.get();
        assertTrue(result.isDone());
        assertEquals("success", result.join());
    }

    @Test
    public void testConfigAccess() {
        ResilienceConfig config = resilienceManager.getConfig();
        assertNotNull(config);
        assertTrue(config.isCircuitBreakerEnabled());
        assertTrue(config.isRetryEnabled());
    }

    @Test
    public void testMultipleDatacenters() {
        String dc1 = "datacenter-1";
        String dc2 = "datacenter-2";
        
        CircuitBreaker cb1 = resilienceManager.getCircuitBreaker(dc1);
        CircuitBreaker cb2 = resilienceManager.getCircuitBreaker(dc2);
        
        assertNotEquals(cb1, cb2);
        assertEquals(dc1, cb1.getName());
        assertEquals(dc2, cb2.getName());
        
        // State changes should be independent
        resilienceManager.openCircuitBreaker(dc1);
        assertTrue(resilienceManager.isCircuitBreakerOpen(dc1));
        assertTrue(resilienceManager.isCircuitBreakerClosed(dc2));
    }

    @Test
    public void testWithAllPatternsEnabled() {
        ResilienceConfig allPatternsConfig = ResilienceConfig.builder()
                .enableAllPatterns()
                .build();
        ResilienceManager manager = new ResilienceManager(allPatternsConfig);
        
        Supplier<String> supplier = () -> "test";
        Supplier<String> decoratedSupplier = manager.decorateSupplier(DATACENTER_ID, supplier);
        
        // Should not throw - all patterns should work together
        String result = decoratedSupplier.get();
        assertEquals("test", result);
    }

    @Test
    public void testWithHighThroughputConfig() {
        ResilienceConfig highThroughputConfig = ResilienceConfig.highThroughputConfig();
        ResilienceManager manager = new ResilienceManager(highThroughputConfig);
        
        // Test that rate limiter is enabled
        RateLimiter rateLimiter = manager.getRateLimiter(DATACENTER_ID);
        assertNotNull(rateLimiter);
        
        // Test that bulkhead is enabled
        Bulkhead bulkhead = manager.getBulkhead(DATACENTER_ID);
        assertNotNull(bulkhead);
        
        // Test that retry is disabled in high throughput config
        assertFalse(highThroughputConfig.isRetryEnabled());
    }
}
