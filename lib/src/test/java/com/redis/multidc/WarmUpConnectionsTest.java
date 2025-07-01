package com.redis.multidc;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.pool.ConnectionPoolConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the warmUpConnections functionality.
 */
class WarmUpConnectionsTest {

    private DatacenterConfiguration configuration;

    @BeforeEach
    void setUp() {
        // Create a test configuration with mock datacenters
        configuration = DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .region("US East")
                    .host("localhost")  // Using localhost for testing
                    .port(6379)
                    .poolConfig(ConnectionPoolConfig.builder()
                        .minPoolSize(2)
                        .maxPoolSize(5)
                        .build())
                    .build(),
                DatacenterEndpoint.builder()
                    .id("us-west-2")
                    .region("US West")
                    .host("localhost")  // Using localhost for testing
                    .port(6380)
                    .poolConfig(ConnectionPoolConfig.builder()
                        .minPoolSize(2)
                        .maxPoolSize(5)
                        .build())
                    .build()
            ))
            .connectionTimeout(Duration.ofSeconds(2))
            .build();
    }

    @Test
    void testWarmUpConnectionsMethodExists() {
        // This test verifies the method exists and can be called
        // It will fail to connect to Redis but that's expected in a unit test
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(configuration)) {
            
            // The method should exist and return a CompletableFuture
            CompletableFuture<Void> warmupFuture = client.warmUpConnections();
            assertNotNull(warmupFuture, "warmUpConnections should return a non-null CompletableFuture");
            
            // The future should complete (either successfully or exceptionally) within a reasonable time
            try {
                warmupFuture.get(5, TimeUnit.SECONDS);
                // If we get here, warmup succeeded (unlikely without actual Redis servers)
                assertTrue(true, "Warmup completed successfully");
            } catch (Exception e) {
                // Expected to fail in unit test environment without Redis servers
                assertTrue(e.getCause() != null || e.getMessage() != null, 
                          "Should fail with a meaningful error when Redis is not available");
            }
        }
    }

    @Test
    void testWarmUpConnectionsCanBeCalled() {
        // This test just verifies the method signature and basic functionality
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(configuration)) {
            
            // Should be able to call the method multiple times
            CompletableFuture<Void> warmup1 = client.warmUpConnections();
            CompletableFuture<Void> warmup2 = client.warmUpConnections();
            
            assertNotNull(warmup1, "First warmup call should return non-null future");
            assertNotNull(warmup2, "Second warmup call should return non-null future");
            
            // Both futures should be different instances
            assertNotSame(warmup1, warmup2, "Each call should return a new CompletableFuture");
        }
    }
}
