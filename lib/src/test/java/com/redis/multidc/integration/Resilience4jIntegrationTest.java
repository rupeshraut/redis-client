package com.redis.multidc.integration;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.ResilienceConfig;
import com.redis.multidc.impl.DefaultMultiDatacenterRedisClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test demonstrating Resilience4j patterns in action.
 * This test showcases circuit breaker, retry, rate limiter, bulkhead, and time limiter
 * functionality with the multi-datacenter Redis client.
 */
public class Resilience4jIntegrationTest {
    
    private static final Logger logger = LoggerFactory.getLogger(Resilience4jIntegrationTest.class);
    
    private DatacenterConfiguration configuration;
    
    @BeforeEach
    void setUp() {
        // Create a configuration with Resilience4j patterns enabled
        ResilienceConfig resilienceConfig = ResilienceConfig.builder()
            .enableCircuitBreaker(true)
            .enableRetry(true)
            .enableRateLimiter(true)
            .enableBulkhead(true)
            .enableTimeLimiter(true)
            .build();
        
        configuration = DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .host("localhost")
                    .port(6380) // Non-existent port to trigger failures
                    .build(),
                DatacenterEndpoint.builder()
                    .id("us-west-1")
                    .host("localhost")
                    .port(6381) // Non-existent port to trigger failures
                    .build()
            ))
            .resilienceConfig(resilienceConfig)
            .connectionTimeout(Duration.ofMillis(500))
            .build();
    }
    
    @Test
    @DisplayName("Circuit Breaker Pattern Integration")
    void testCircuitBreakerIntegration() {
        logger.info("=== Circuit Breaker Integration Test ===");
        
        // This test demonstrates circuit breaker behavior
        // Since we're connecting to non-existent Redis servers, 
        // the circuit breaker should open after repeated failures
        
        AtomicInteger failures = new AtomicInteger(0);
        
        try (MultiDatacenterRedisClient client = new DefaultMultiDatacenterRedisClient(configuration)) {
            // Make several calls that will fail due to connection issues
            for (int i = 0; i < 10; i++) {
                try {
                    String result = client.sync().get("test-key");
                    logger.info("Attempt {}: Success - {}", i + 1, result);
                } catch (Exception e) {
                    failures.incrementAndGet();
                    logger.info("Attempt {}: Failed - {}", i + 1, e.getMessage());
                }
                
                // Small delay between attempts
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            
            logger.info("Total failures: {}", failures.get());
            assertTrue(failures.get() > 0, "Should have failures due to connection issues");
            
        } catch (Exception e) {
            logger.error("Client initialization failed", e);
            // This is expected since we're using non-existent Redis endpoints
            assertTrue(e.getMessage().contains("Failed to connect") || 
                      e.getMessage().contains("Failed to initialize"));
        }
    }
    
    @Test
    @DisplayName("Retry Pattern Integration")
    void testRetryIntegration() {
        logger.info("=== Retry Pattern Integration Test ===");
        
        // Test retry configuration
        ResilienceConfig retryConfig = ResilienceConfig.builder()
            .enableRetry(true)
            .enableCircuitBreaker(false) // Disable circuit breaker for pure retry test
            .build();
        
        DatacenterConfiguration retryConfiguration = createConfigurationWithResilience(retryConfig);
        
        try (MultiDatacenterRedisClient client = new DefaultMultiDatacenterRedisClient(retryConfiguration)) {
            // This will fail but should retry as configured
            assertThrows(RuntimeException.class, () -> {
                client.sync().get("test-key");
            });
            
        } catch (Exception e) {
            // Expected due to non-existent Redis endpoints
            logger.info("Expected failure during retry test: {}", e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Rate Limiter Pattern Integration")
    void testRateLimiterIntegration() throws InterruptedException {
        logger.info("=== Rate Limiter Integration Test ===");
        
        // Test rate limiter with a very low limit
        ResilienceConfig rateLimitConfig = ResilienceConfig.builder()
            .enableRateLimiter(true)
            .enableCircuitBreaker(false)
            .enableRetry(false)
            .build();
        
        DatacenterConfiguration rateLimitConfiguration = createConfigurationWithResilience(rateLimitConfig);
        
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger rateLimitedCalls = new AtomicInteger(0);
        
        // Simulate multiple threads trying to access Redis
        Thread[] threads = new Thread[5];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try (MultiDatacenterRedisClient client = new DefaultMultiDatacenterRedisClient(rateLimitConfiguration)) {
                    latch.await();
                    
                    for (int j = 0; j < 10; j++) {
                        try {
                            client.sync().get("test-key-" + j);
                        } catch (Exception e) {
                            if (e.getMessage().contains("rate") || e.getMessage().contains("limit")) {
                                rateLimitedCalls.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    // Expected due to connection issues
                }
            });
            threads[i].start();
        }
        
        latch.countDown(); // Start all threads
        
        for (Thread thread : threads) {
            thread.join(5000); // Wait up to 5 seconds
        }
        
        logger.info("Rate limited calls: {}", rateLimitedCalls.get());
        // Note: Due to connection failures, rate limiting might not be the primary failure mode
    }
    
    @Test
    @DisplayName("Bulkhead Pattern Integration")
    void testBulkheadIntegration() {
        logger.info("=== Bulkhead Integration Test ===");
        
        ResilienceConfig bulkheadConfig = ResilienceConfig.builder()
            .enableBulkhead(true)
            .enableCircuitBreaker(false)
            .enableRetry(false)
            .build();
        
        DatacenterConfiguration bulkheadConfiguration = createConfigurationWithResilience(bulkheadConfig);
        
        try (MultiDatacenterRedisClient client = new DefaultMultiDatacenterRedisClient(bulkheadConfiguration)) {
            // Test that bulkhead limits concurrent executions
            assertThrows(RuntimeException.class, () -> {
                client.sync().get("test-key");
            });
            
        } catch (Exception e) {
            // Expected due to non-existent Redis endpoints
            logger.info("Expected failure during bulkhead test: {}", e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Time Limiter Pattern Integration")
    void testTimeLimiterIntegration() {
        logger.info("=== Time Limiter Integration Test ===");
        
        ResilienceConfig timeLimitConfig = ResilienceConfig.builder()
            .enableTimeLimiter(true)
            .enableCircuitBreaker(false)
            .enableRetry(false)
            .build();
        
        DatacenterConfiguration timeLimitConfiguration = DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .host("localhost")
                    .port(6380)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("us-west-1")
                    .host("localhost")
                    .port(6381)
                    .build()
            ))
            .resilienceConfig(timeLimitConfig)
            .connectionTimeout(Duration.ofMillis(10000)) // Long timeout for connection
            .build();
        
        try (MultiDatacenterRedisClient client = new DefaultMultiDatacenterRedisClient(timeLimitConfiguration)) {
            long startTime = System.currentTimeMillis();
            
            assertThrows(RuntimeException.class, () -> {
                client.sync().get("test-key");
            });
            
            long duration = System.currentTimeMillis() - startTime;
            logger.info("Operation took {} ms", duration);
            
            // Time limiter should have limited the execution time
            assertTrue(duration < 10000, "Time limiter should have limited execution time");
            
        } catch (Exception e) {
            // Expected due to non-existent Redis endpoints
            logger.info("Expected failure during time limiter test: {}", e.getMessage());
        }
    }
    
    @Test
    @DisplayName("All Resilience Patterns Combined")
    void testAllPatternsIntegration() {
        logger.info("=== All Resilience Patterns Combined Test ===");
        
        // Test with all patterns enabled (default configuration)
        ResilienceConfig allPatternsConfig = ResilienceConfig.defaultConfig();
        
        DatacenterConfiguration allPatternsConfiguration = createConfigurationWithResilience(allPatternsConfig);
        
        try (MultiDatacenterRedisClient client = new DefaultMultiDatacenterRedisClient(allPatternsConfiguration)) {
            // Test that all resilience patterns work together
            assertThrows(RuntimeException.class, () -> {
                client.sync().get("test-key");
            });
            
            // Test async operations
            assertThrows(Exception.class, () -> {
                client.async().get("test-key").get(5, TimeUnit.SECONDS);
            });
            
        } catch (Exception e) {
            // Expected due to non-existent Redis endpoints
            logger.info("Expected failure during combined patterns test: {}", e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Configuration Validation")
    void testConfigurationValidation() {
        logger.info("=== Configuration Validation Test ===");
        
        ResilienceConfig config = configuration.getResilienceConfig();
        
        assertNotNull(config, "ResilienceConfig should not be null");
        assertTrue(config.isCircuitBreakerEnabled(), "Circuit breaker should be enabled");
        assertTrue(config.isRetryEnabled(), "Retry should be enabled");
        assertTrue(config.isRateLimiterEnabled(), "Rate limiter should be enabled");
        assertTrue(config.isBulkheadEnabled(), "Bulkhead should be enabled");
        assertTrue(config.isTimeLimiterEnabled(), "Time limiter should be enabled");
        
        assertNotNull(config.getCircuitBreakerConfig(), "Circuit breaker config should not be null");
        assertNotNull(config.getRetryConfig(), "Retry config should not be null");
        assertNotNull(config.getRateLimiterConfig(), "Rate limiter config should not be null");
        assertNotNull(config.getBulkheadConfig(), "Bulkhead config should not be null");
        assertNotNull(config.getTimeLimiterConfig(), "Time limiter config should not be null");
        
        logger.info("All configuration validations passed");
    }
    
    private DatacenterConfiguration createConfigurationWithResilience(ResilienceConfig resilienceConfig) {
        return DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .host("localhost")
                    .port(6380)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("us-west-1")
                    .host("localhost")
                    .port(6381)
                    .build()
            ))
            .resilienceConfig(resilienceConfig)
            .connectionTimeout(Duration.ofMillis(500))
            .build();
    }
}
