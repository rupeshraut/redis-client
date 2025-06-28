# Resilience4j Migration Guide

## Overview

The Redis Multi-Datacenter Client library has been upgraded from a custom circuit breaker implementation to the industry-standard **Resilience4j** library. This migration provides:

- **Enhanced Resilience Patterns**: Circuit Breaker, Retry, Rate Limiter, Bulkhead, and Time Limiter
- **Better Observability**: Built-in metrics and event handling
- **Reactive Support**: Native integration with Project Reactor
- **Production-Ready**: Battle-tested patterns used by Netflix, Spring Cloud, and other major projects

## What Changed

### 1. Dependencies Added

```gradle
// New Resilience4j dependencies
api "io.github.resilience4j:resilience4j-circuitbreaker:2.1.0"
api "io.github.resilience4j:resilience4j-retry:2.1.0"
api "io.github.resilience4j:resilience4j-ratelimiter:2.1.0"
api "io.github.resilience4j:resilience4j-bulkhead:2.1.0"
api "io.github.resilience4j:resilience4j-timelimiter:2.1.0"
api "io.github.resilience4j:resilience4j-reactor:2.1.0"
api "io.github.resilience4j:resilience4j-micrometer:2.1.0"
api "io.github.resilience4j:resilience4j-all:2.1.0"
```

### 2. New Classes

- **`ResilienceConfig`**: Comprehensive resilience configuration using Resilience4j
- **`ResilienceManager`**: Manages all resilience patterns for datacenter operations

### 3. Configuration Changes

**Before (Old CircuitBreakerConfig):**
```java
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .enableCircuitBreaker(true)
    .circuitBreakerConfig(CircuitBreakerConfig.builder()
        .failureThreshold(5)
        .openTimeout(Duration.ofSeconds(60))
        .build())
    .build();
```

**After (New ResilienceConfig):**
```java
DatacenterConfiguration config = DatacenterConfiguration.builder()
    .resilienceConfig(ResilienceConfig.builder()
        .enableBasicPatterns() // Circuit breaker + retry
        .build())
    .build();
```

## Migration Steps

### Step 1: Update Configuration

Replace the old circuit breaker configuration with the new resilience configuration:

```java
// OLD - Custom CircuitBreakerConfig
.enableCircuitBreaker(true)
.circuitBreakerConfig(CircuitBreakerConfig.defaultConfig())

// NEW - Resilience4j ResilienceConfig
.resilienceConfig(ResilienceConfig.defaultConfig())
```

### Step 2: Use Pre-configured Setups

Choose from pre-configured resilience setups:

```java
// Basic: Circuit Breaker + Retry
ResilienceConfig.defaultConfig()

// Development: Relaxed settings
ResilienceConfig.relaxedConfig()

// Production: High-throughput with rate limiting
ResilienceConfig.highThroughputConfig()
```

### Step 3: Custom Configuration

For advanced use cases, create custom configurations:

```java
ResilienceConfig customConfig = ResilienceConfig.builder()
    .circuitBreaker(CircuitBreakerConfig.custom()
        .failureRateThreshold(60.0f)
        .waitDurationInOpenState(Duration.ofMinutes(2))
        .slidingWindowSize(50)
        .build())
    .retry(RetryConfig.custom()
        .maxAttempts(5)
        .waitDuration(Duration.ofSeconds(1))
        .build())
    .rateLimiter(RateLimiterConfig.custom()
        .limitForPeriod(1000)
        .limitRefreshPeriod(Duration.ofSeconds(1))
        .build())
    .enableAllPatterns()
    .build();
```

### Step 4: Use ResilienceManager for Operation Decoration

Decorate your Redis operations with resilience patterns:

```java
ResilienceManager resilienceManager = new ResilienceManager(resilienceConfig);

// Decorate synchronous operations
Supplier<String> resilientOperation = resilienceManager.decorateSupplier(
    datacenterId, 
    () -> redisClient.get("key")
);

// Decorate reactive operations
Mono<String> resilientMono = resilienceManager.decorateMono(
    datacenterId,
    redisReactiveClient.get("key")
);
```

## Available Resilience Patterns

### 1. Circuit Breaker
Prevents cascading failures by stopping calls to failing services.

```java
.circuitBreaker(CircuitBreakerConfig.custom()
    .failureRateThreshold(50.0f)        // Open when 50% of calls fail
    .waitDurationInOpenState(Duration.ofSeconds(60))  // Wait 60s before half-open
    .slidingWindowSize(10)              // Consider last 10 calls
    .minimumNumberOfCalls(5)            // Minimum calls before calculation
    .build())
```

### 2. Retry
Automatically retries failed operations with configurable backoff.

```java
.retry(RetryConfig.custom()
    .maxAttempts(3)                     // Maximum 3 attempts
    .waitDuration(Duration.ofMillis(500)) // Wait 500ms between retries
    .build())
```

### 3. Rate Limiter
Controls the rate of requests to prevent overloading.

```java
.rateLimiter(RateLimiterConfig.custom()
    .limitForPeriod(100)                // 100 requests per period
    .limitRefreshPeriod(Duration.ofSeconds(1)) // Period of 1 second
    .timeoutDuration(Duration.ofMillis(10))    // Wait up to 10ms for permit
    .build())
```

### 4. Bulkhead
Isolates resources to prevent resource exhaustion.

```java
.bulkhead(BulkheadConfig.custom()
    .maxConcurrentCalls(25)             // Maximum 25 concurrent calls
    .maxWaitDuration(Duration.ofMillis(100)) // Wait up to 100ms for slot
    .build())
```

### 5. Time Limiter
Limits the time allowed for operations.

```java
.timeLimiter(TimeLimiterConfig.custom()
    .timeoutDuration(Duration.ofSeconds(10)) // 10 second timeout
    .cancelRunningFuture(true)          // Cancel if timeout occurs
    .build())
```

## Backward Compatibility

The old `CircuitBreakerConfig` methods are deprecated but still work:

```java
// These methods are deprecated but functional
config.isCircuitBreakerEnabled()      // → resilienceConfig.isCircuitBreakerEnabled()
config.getCircuitBreakerConfig()      // → resilienceConfig.getCircuitBreakerConfig()
```

## Example Configurations

### Development Environment
```java
ResilienceConfig.relaxedConfig()
// - Circuit breaker: 80% failure threshold, 30s wait
// - Retry: 2 attempts with 100ms delay
// - Other patterns: disabled
```

### Production High-Throughput
```java
ResilienceConfig.highThroughputConfig()
// - Circuit breaker: 75% threshold, 20s wait
// - Rate limiter: 1000 requests/second
// - Bulkhead: 50 concurrent calls
// - Retry: disabled (for speed)
```

### Enterprise Mission-Critical
```java
ResilienceConfig.builder()
    .circuitBreakerConfig(60.0f, Duration.ofMinutes(2), 50, 20)
    .retryConfig(5, Duration.ofSeconds(1))
    .rateLimiterConfig(500, Duration.ofSeconds(1), Duration.ofMillis(100))
    .bulkheadConfig(20, Duration.ofMillis(50))
    .enableAllPatterns()
    .build()
```

## Monitoring and Observability

Resilience4j provides built-in metrics and events:

```java
ResilienceManager manager = new ResilienceManager(config);

// Get circuit breaker state
CircuitBreaker.State state = manager.getCircuitBreakerState(datacenterId);

// Check if circuit breaker is open
boolean isOpen = manager.isCircuitBreakerOpen(datacenterId);

// Force state transitions (for testing/maintenance)
manager.openCircuitBreaker(datacenterId);
manager.closeCircuitBreaker(datacenterId);
manager.resetCircuitBreaker(datacenterId);
```

## Benefits of the Migration

1. **Industry Standard**: Resilience4j is the de facto standard for resilience patterns in Java
2. **Comprehensive**: Provides 5 complementary resilience patterns
3. **Reactive Ready**: Native support for Project Reactor and RxJava
4. **Metrics Integration**: Built-in Micrometer integration for monitoring
5. **Event-Driven**: Rich event model for custom handling and logging
6. **Production Proven**: Used by major companies and frameworks
7. **Extensible**: Easy to extend and customize for specific needs

## Next Steps

1. Update your configuration to use `ResilienceConfig`
2. Choose appropriate pre-configured setups for your environment
3. Add monitoring for resilience metrics
4. Test failure scenarios to validate resilience behavior
5. Consider enabling additional patterns (rate limiting, bulkhead) for production
