package com.redis.multidc.example;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.ResilienceConfig;
import com.redis.multidc.config.RoutingStrategy;
import com.redis.multidc.resilience.ResilienceManager;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.bulkhead.BulkheadConfig;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

/**
 * Example demonstrating the new Resilience4j-based resilience configuration
 * for multi-datacenter Redis operations.
 */
public class Resilience4jExample {

    public static void main(String[] args) {
        System.out.println("=== Redis Multi-Datacenter Client with Resilience4j ===\n");

        // Example 1: Basic Resilience Configuration (Circuit Breaker + Retry)
        basicResilienceExample();

        // Example 2: High-Throughput Configuration with Rate Limiting and Bulkhead
        highThroughputExample();

        // Example 3: Custom Enterprise Configuration
        enterpriseResilienceExample();

        // Example 4: Using ResilienceManager to decorate operations
        resilienceManagerExample();
    }

    private static void basicResilienceExample() {
        System.out.println("1. Basic Resilience Configuration");
        System.out.println("==================================");

        // Create a basic resilience configuration with circuit breaker and retry
        ResilienceConfig resilienceConfig = ResilienceConfig.builder()
                .enableBasicPatterns() // Enables circuit breaker and retry only
                .circuitBreakerConfig(
                        50.0f, // 50% failure rate threshold
                        Duration.ofSeconds(30), // Wait 30 seconds in open state
                        10, // Sliding window size
                        5 // Minimum number of calls
                )
                .retryConfig(
                        3, // Max 3 retry attempts
                        Duration.ofMillis(500) // 500ms wait between retries
                )
                .build();

        DatacenterConfiguration config = DatacenterConfiguration.builder()
                .datacenters(List.of(
                        DatacenterEndpoint.builder()
                                .id("us-east-1")
                                .region("us-east")
                                .host("redis-us-east-1.example.com")
                                .port(6379)
                                .build(),
                        DatacenterEndpoint.builder()
                                .id("us-west-1")
                                .region("us-west")
                                .host("redis-us-west-1.example.com")
                                .port(6379)
                                .build()
                ))
                .localDatacenter("us-east-1")
                .resilienceConfig(resilienceConfig)
                .build();

        System.out.println("✓ Circuit Breaker: " + resilienceConfig.isCircuitBreakerEnabled());
        System.out.println("✓ Retry: " + resilienceConfig.isRetryEnabled());
        System.out.println("✗ Rate Limiter: " + resilienceConfig.isRateLimiterEnabled());
        System.out.println("✗ Bulkhead: " + resilienceConfig.isBulkheadEnabled());
        System.out.println("✗ Time Limiter: " + resilienceConfig.isTimeLimiterEnabled());
        System.out.println();
    }

    private static void highThroughputExample() {
        System.out.println("2. High-Throughput Configuration");
        System.out.println("================================");

        // Use the pre-configured high-throughput setup
        ResilienceConfig resilienceConfig = ResilienceConfig.highThroughputConfig();

        DatacenterConfiguration config = DatacenterConfiguration.builder()
                .datacenters(List.of(
                        DatacenterEndpoint.builder()
                                .id("production-primary")
                                .region("us-east")
                                .host("redis-prod-primary.example.com")
                                .port(6379)
                                .priority(1)
                                .weight(1.0)
                                .build(),
                        DatacenterEndpoint.builder()
                                .id("production-secondary")
                                .region("us-west")
                                .host("redis-prod-secondary.example.com")
                                .port(6379)
                                .priority(2)
                                .weight(0.8)
                                .build()
                ))
                .localDatacenter("production-primary")
                .resilienceConfig(resilienceConfig)
                .routingStrategy(RoutingStrategy.PRIORITY_BASED)
                .build();

        System.out.println("✓ Circuit Breaker: " + resilienceConfig.isCircuitBreakerEnabled() + " (75% failure threshold)");
        System.out.println("✗ Retry: " + resilienceConfig.isRetryEnabled() + " (disabled for high throughput)");
        System.out.println("✓ Rate Limiter: " + resilienceConfig.isRateLimiterEnabled() + " (1000 requests/second)");
        System.out.println("✓ Bulkhead: " + resilienceConfig.isBulkheadEnabled() + " (50 concurrent calls)");
        System.out.println("✗ Time Limiter: " + resilienceConfig.isTimeLimiterEnabled());
        System.out.println();
    }

    private static void enterpriseResilienceExample() {
        System.out.println("3. Custom Enterprise Configuration");
        System.out.println("=================================");

        // Custom configuration with all resilience patterns enabled
        ResilienceConfig resilienceConfig = ResilienceConfig.builder()
                .circuitBreaker(CircuitBreakerConfig.custom()
                        .failureRateThreshold(60.0f)
                        .waitDurationInOpenState(Duration.ofMinutes(2))
                        .slidingWindowSize(50)
                        .minimumNumberOfCalls(20)
                        .permittedNumberOfCallsInHalfOpenState(10)
                        .build())
                .retry(RetryConfig.custom()
                        .maxAttempts(5)
                        .waitDuration(Duration.ofSeconds(1))
                        .retryOnException(throwable -> !(throwable instanceof SecurityException))
                        .build())
                .rateLimiter(RateLimiterConfig.custom()
                        .limitForPeriod(500)
                        .limitRefreshPeriod(Duration.ofSeconds(1))
                        .timeoutDuration(Duration.ofMillis(100))
                        .build())
                .bulkhead(BulkheadConfig.custom()
                        .maxConcurrentCalls(20)
                        .maxWaitDuration(Duration.ofMillis(50))
                        .build())
                .enableAllPatterns()
                .build();

        DatacenterConfiguration config = DatacenterConfiguration.builder()
                .datacenters(List.of(
                        DatacenterEndpoint.builder()
                                .id("enterprise-primary")
                                .region("us-east")
                                .host("redis-enterprise-primary.example.com")
                                .port(6380)
                                .ssl(true)
                                .password("${REDIS_PASSWORD}")
                                .build(),
                        DatacenterEndpoint.builder()
                                .id("enterprise-secondary")
                                .region("eu-west")
                                .host("redis-enterprise-secondary.example.com")
                                .port(6380)
                                .ssl(true)
                                .password("${REDIS_PASSWORD}")
                                .build()
                ))
                .localDatacenter("enterprise-primary")
                .resilienceConfig(resilienceConfig)
                .connectionTimeout(Duration.ofSeconds(3))
                .requestTimeout(Duration.ofSeconds(5))
                .build();

        System.out.println("✓ Circuit Breaker: 60% failure threshold, 2min wait");
        System.out.println("✓ Retry: 5 attempts with 1s delay");
        System.out.println("✓ Rate Limiter: 500 requests/second");
        System.out.println("✓ Bulkhead: 20 concurrent calls max");
        System.out.println("✓ Time Limiter: 10s timeout");
        System.out.println();
    }

    private static void resilienceManagerExample() {
        System.out.println("4. Using ResilienceManager");
        System.out.println("=========================");

        // Create a resilience configuration
        ResilienceConfig config = ResilienceConfig.builder()
                .enableBasicPatterns()
                .build();

        // Create the resilience manager
        ResilienceManager resilienceManager = new ResilienceManager(config);

        // Example: Decorate a supplier with resilience patterns
        String datacenterId = "example-dc";
        
        Supplier<String> redisOperation = () -> {
            // Simulate a Redis operation that might fail
            if (Math.random() < 0.3) { // 30% chance of failure
                throw new RuntimeException("Redis connection timeout");
            }
            return "Redis operation successful";
        };

        // Decorate the operation with circuit breaker and retry
        Supplier<String> resilientOperation = resilienceManager.decorateSupplier(datacenterId, redisOperation);

        System.out.println("Executing resilient Redis operations...");
        
        for (int i = 1; i <= 5; i++) {
            try {
                String result = resilientOperation.get();
                System.out.println("Attempt " + i + ": " + result);
            } catch (Exception e) {
                System.out.println("Attempt " + i + ": Failed after retries - " + e.getMessage());
            }
        }

        // Show circuit breaker state
        System.out.println("\nCircuit Breaker State: " + resilienceManager.getCircuitBreakerState(datacenterId));
        
        if (resilienceManager.isCircuitBreakerOpen(datacenterId)) {
            System.out.println("⚠️  Circuit breaker is OPEN - protecting against cascading failures");
        } else if (resilienceManager.isCircuitBreakerClosed(datacenterId)) {
            System.out.println("✅ Circuit breaker is CLOSED - normal operations");
        } else {
            System.out.println("🔄 Circuit breaker is HALF-OPEN - testing recovery");
        }
        
        System.out.println();
    }
}
