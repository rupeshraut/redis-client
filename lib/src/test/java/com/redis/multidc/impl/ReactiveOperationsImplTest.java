package com.redis.multidc.impl;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.observability.MetricsCollector;
import com.redis.multidc.routing.DatacenterRouter;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for ReactiveOperationsImpl.
 */
class ReactiveOperationsImplTest {

    @Test
    void testBasicFunctionality() {
        // Create a minimal configuration for testing
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .localDatacenter("dc1")
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("dc1")
                    .host("localhost")
                    .port(6379)
                    .build()
            ))
            .build();
        
        // Create mock dependencies
        Map<String, RedisReactiveCommands<String, String>> connections = new HashMap<>();
        DatacenterRouter router = new DatacenterRouter(config, new MetricsCollector(config));
        MetricsCollector metricsCollector = new MetricsCollector(config);
        
        // Create the reactive operations implementation
        ReactiveOperationsImpl reactiveOps = new ReactiveOperationsImpl(
            connections, router, metricsCollector, config);
        
        // Test that the implementation doesn't crash on basic operations
        assertNotNull(reactiveOps);
        
        // Test ping operation (should return a default message when no connections)
        Mono<String> pingResult = reactiveOps.ping();
        StepVerifier.create(pingResult)
            .expectNext("No datacenter available")
            .verifyComplete();
        
        // Test exists operation (should return false when no connections)
        Mono<Boolean> existsResult = reactiveOps.exists("test-key");
        StepVerifier.create(existsResult)
            .expectNext(false)
            .verifyComplete();
        
        // Test dbSize operation (should return 0 when no connections)
        Mono<Long> dbSizeResult = reactiveOps.dbSize();
        StepVerifier.create(dbSizeResult)
            .expectNext(0L)
            .verifyComplete();
    }
    
    @Test
    void testTombstoneOperations() {
        // Create a minimal configuration for testing
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .localDatacenter("dc1")
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("dc1")
                    .host("localhost")
                    .port(6379)
                    .build()
            ))
            .build();
        
        Map<String, RedisReactiveCommands<String, String>> connections = new HashMap<>();
        DatacenterRouter router = new DatacenterRouter(config, new MetricsCollector(config));
        MetricsCollector metricsCollector = new MetricsCollector(config);
        
        ReactiveOperationsImpl reactiveOps = new ReactiveOperationsImpl(
            connections, router, metricsCollector, config);
        
        // Test tombstone creation and checking
        String testKey = "test-tombstone-key";
        
        // Initially, key should not be tombstoned
        StepVerifier.create(reactiveOps.isTombstoned(testKey))
            .expectNext(false)
            .verifyComplete();
        
        // Create a tombstone
        StepVerifier.create(reactiveOps.createTombstone(testKey, 
            com.redis.multidc.model.TombstoneKey.Type.SOFT_DELETE))
            .verifyComplete();
        
        // Now key should be tombstoned
        StepVerifier.create(reactiveOps.isTombstoned(testKey))
            .expectNext(true)
            .verifyComplete();
        
        // Remove tombstone
        StepVerifier.create(reactiveOps.removeTombstone(testKey))
            .verifyComplete();
        
        // Key should no longer be tombstoned
        StepVerifier.create(reactiveOps.isTombstoned(testKey))
            .expectNext(false)
            .verifyComplete();
    }
}
