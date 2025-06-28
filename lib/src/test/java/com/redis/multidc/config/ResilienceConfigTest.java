package com.redis.multidc.config;

import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Resilience4j-based ResilienceConfig.
 */
public class ResilienceConfigTest {

    @Test
    public void testDefaultConfiguration() {
        ResilienceConfig config = ResilienceConfig.defaultConfig();
        
        assertNotNull(config);
        assertTrue(config.isCircuitBreakerEnabled());
        assertTrue(config.isRetryEnabled());
        assertFalse(config.isRateLimiterEnabled());
        assertFalse(config.isBulkheadEnabled());
        assertFalse(config.isTimeLimiterEnabled());
        
        assertNotNull(config.getCircuitBreakerConfig());
        assertNotNull(config.getRetryConfig());
        assertNotNull(config.getRateLimiterConfig());
        assertNotNull(config.getBulkheadConfig());
        assertNotNull(config.getTimeLimiterConfig());
    }

    @Test
    public void testRelaxedConfiguration() {
        ResilienceConfig config = ResilienceConfig.relaxedConfig();
        
        assertTrue(config.isCircuitBreakerEnabled());
        assertTrue(config.isRetryEnabled());
        assertFalse(config.isRateLimiterEnabled());
        assertFalse(config.isBulkheadEnabled());
        assertFalse(config.isTimeLimiterEnabled());
        
        CircuitBreakerConfig cbConfig = config.getCircuitBreakerConfig();
        assertEquals(80.0f, cbConfig.getFailureRateThreshold());
        assertEquals(20, cbConfig.getSlidingWindowSize());
        assertEquals(5, cbConfig.getMinimumNumberOfCalls());
        
        RetryConfig retryConfig = config.getRetryConfig();
        assertEquals(2, retryConfig.getMaxAttempts());
    }

    @Test
    public void testHighThroughputConfiguration() {
        ResilienceConfig config = ResilienceConfig.highThroughputConfig();
        
        assertTrue(config.isCircuitBreakerEnabled());
        assertFalse(config.isRetryEnabled());
        assertTrue(config.isRateLimiterEnabled());
        assertTrue(config.isBulkheadEnabled());
        assertFalse(config.isTimeLimiterEnabled());
        
        CircuitBreakerConfig cbConfig = config.getCircuitBreakerConfig();
        assertEquals(75.0f, cbConfig.getFailureRateThreshold());
        assertEquals(100, cbConfig.getSlidingWindowSize());
        assertEquals(20, cbConfig.getMinimumNumberOfCalls());
        
        RateLimiterConfig rlConfig = config.getRateLimiterConfig();
        assertEquals(1000, rlConfig.getLimitForPeriod());
        assertEquals(Duration.ofSeconds(1), rlConfig.getLimitRefreshPeriod());
        assertEquals(Duration.ofMillis(10), rlConfig.getTimeoutDuration());
        
        BulkheadConfig bhConfig = config.getBulkheadConfig();
        assertEquals(50, bhConfig.getMaxConcurrentCalls());
        assertEquals(Duration.ofMillis(100), bhConfig.getMaxWaitDuration());
    }

    @Test
    public void testCustomConfiguration() {
        ResilienceConfig config = ResilienceConfig.builder()
                .circuitBreakerConfig(60.0f, Duration.ofSeconds(45), 15, 8)
                .retryConfig(5, Duration.ofMillis(200))
                .rateLimiterConfig(500, Duration.ofSeconds(2), Duration.ofMillis(50))
                .bulkheadConfig(30, Duration.ofMillis(200))
                .timeLimiterConfig(Duration.ofSeconds(15), false)
                .enableAllPatterns()
                .build();
        
        assertTrue(config.isCircuitBreakerEnabled());
        assertTrue(config.isRetryEnabled());
        assertTrue(config.isRateLimiterEnabled());
        assertTrue(config.isBulkheadEnabled());
        assertTrue(config.isTimeLimiterEnabled());
        
        CircuitBreakerConfig cbConfig = config.getCircuitBreakerConfig();
        assertEquals(60.0f, cbConfig.getFailureRateThreshold());
        assertEquals(15, cbConfig.getSlidingWindowSize());
        assertEquals(8, cbConfig.getMinimumNumberOfCalls());
        
        RetryConfig retryConfig = config.getRetryConfig();
        assertEquals(5, retryConfig.getMaxAttempts());
        
        RateLimiterConfig rlConfig = config.getRateLimiterConfig();
        assertEquals(500, rlConfig.getLimitForPeriod());
        assertEquals(Duration.ofSeconds(2), rlConfig.getLimitRefreshPeriod());
        assertEquals(Duration.ofMillis(50), rlConfig.getTimeoutDuration());
        
        BulkheadConfig bhConfig = config.getBulkheadConfig();
        assertEquals(30, bhConfig.getMaxConcurrentCalls());
        assertEquals(Duration.ofMillis(200), bhConfig.getMaxWaitDuration());
        
        TimeLimiterConfig tlConfig = config.getTimeLimiterConfig();
        assertEquals(Duration.ofSeconds(15), tlConfig.getTimeoutDuration());
        assertFalse(tlConfig.shouldCancelRunningFuture());
    }

    @Test
    public void testBasicPatternsConfiguration() {
        ResilienceConfig config = ResilienceConfig.builder()
                .enableBasicPatterns()
                .build();
        
        assertTrue(config.isCircuitBreakerEnabled());
        assertTrue(config.isRetryEnabled());
        assertFalse(config.isRateLimiterEnabled());
        assertFalse(config.isBulkheadEnabled());
        assertFalse(config.isTimeLimiterEnabled());
    }

    @Test
    public void testCustomCircuitBreakerConfig() {
        CircuitBreakerConfig customCbConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(70.0f)
                .waitDurationInOpenState(Duration.ofSeconds(120))
                .slidingWindowSize(50)
                .minimumNumberOfCalls(15)
                .permittedNumberOfCallsInHalfOpenState(5)
                .build();
        
        ResilienceConfig config = ResilienceConfig.builder()
                .circuitBreaker(customCbConfig)
                .enableCircuitBreaker(true)
                .build();
        
        CircuitBreakerConfig actualConfig = config.getCircuitBreakerConfig();
        assertEquals(70.0f, actualConfig.getFailureRateThreshold());
        assertEquals(50, actualConfig.getSlidingWindowSize());
        assertEquals(15, actualConfig.getMinimumNumberOfCalls());
        assertEquals(5, actualConfig.getPermittedNumberOfCallsInHalfOpenState());
    }

    @Test
    public void testCustomRetryConfig() {
        RetryConfig customRetryConfig = RetryConfig.custom()
                .maxAttempts(10)
                .waitDuration(Duration.ofMillis(1000))
                .build();
        
        ResilienceConfig config = ResilienceConfig.builder()
                .retry(customRetryConfig)
                .enableRetry(true)
                .build();
        
        RetryConfig actualConfig = config.getRetryConfig();
        assertEquals(10, actualConfig.getMaxAttempts());
    }

    @Test
    public void testDisableAllPatterns() {
        ResilienceConfig config = ResilienceConfig.builder()
                .enableCircuitBreaker(false)
                .enableRetry(false)
                .enableRateLimiter(false)
                .enableBulkhead(false)
                .enableTimeLimiter(false)
                .build();
        
        assertFalse(config.isCircuitBreakerEnabled());
        assertFalse(config.isRetryEnabled());
        assertFalse(config.isRateLimiterEnabled());
        assertFalse(config.isBulkheadEnabled());
        assertFalse(config.isTimeLimiterEnabled());
    }

    @Test
    public void testMixedConfiguration() {
        ResilienceConfig config = ResilienceConfig.builder()
                .enableCircuitBreaker(true)
                .enableRetry(false)
                .enableRateLimiter(true)
                .enableBulkhead(false)
                .enableTimeLimiter(true)
                .build();
        
        assertTrue(config.isCircuitBreakerEnabled());
        assertFalse(config.isRetryEnabled());
        assertTrue(config.isRateLimiterEnabled());
        assertFalse(config.isBulkheadEnabled());
        assertTrue(config.isTimeLimiterEnabled());
    }
}
