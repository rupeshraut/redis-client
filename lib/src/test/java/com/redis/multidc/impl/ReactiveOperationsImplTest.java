package com.redis.multidc.impl;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import com.redis.multidc.observability.MetricsCollector;
import com.redis.multidc.routing.DatacenterRouter;
import com.redis.multidc.resilience.ResilienceManager;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

class ReactiveOperationsImplTest {

    @Mock
    private Map<String, RedisReactiveCommands<String, String>> connections;
    
    @Mock
    private RedisReactiveCommands<String, String> reactiveCommands;
    
    @Mock 
    private DatacenterRouter router;
    
    @Mock
    private MetricsCollector metricsCollector;

    @Mock
    private ResilienceManager resilienceManager;

    private ReactiveOperationsImpl operations;
    private DatacenterConfiguration config;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        config = DatacenterConfiguration.builder()
            .localDatacenter("dc1")
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("dc1")
                    .host("localhost")
                    .port(6379)
                    .build()
            ))
            .build();
        
        when(connections.get(anyString())).thenReturn(reactiveCommands);
        when(connections.isEmpty()).thenReturn(false);
        
        // Default router behavior - returns dc1
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.of("dc1"));
        when(router.selectDatacenterForWrite(any())).thenReturn(Optional.of("dc1"));
        
        // Mock ResilienceManager to pass through operations unchanged
        when(resilienceManager.decorateMono(anyString(), any())).thenAnswer(invocation -> invocation.getArgument(1));
        
        operations = new ReactiveOperationsImpl(connections, router, metricsCollector, resilienceManager, config);
    }

    @Test
    void testBasicFunctionality() {
        when(reactiveCommands.exists(anyString())).thenReturn(Mono.just(1L));
        when(reactiveCommands.dbsize()).thenReturn(Mono.just(10L));
        when(reactiveCommands.ping()).thenReturn(Mono.just("PONG"));

        StepVerifier.create(operations.exists("test"))
                .expectNext(true)
                .verifyComplete();

        StepVerifier.create(operations.dbSize())
                .expectNext(10L)
                .verifyComplete();

        StepVerifier.create(operations.ping())
                .expectNext("PONG")
                .verifyComplete();
    }

    @Test
    void testTombstoneOperations() {
        when(reactiveCommands.get(anyString())).thenReturn(Mono.just("value"));
        when(reactiveCommands.setex(anyString(), anyLong(), anyString())).thenReturn(Mono.just("OK"));

        StepVerifier.create(operations.createTombstone("key", TombstoneKey.Type.SOFT_DELETE))
                .verifyComplete();

        StepVerifier.create(operations.createTombstone("key2", TombstoneKey.Type.SOFT_DELETE, Duration.ofMinutes(30)))
                .verifyComplete();

        StepVerifier.create(operations.isTombstoned("key"))
                .expectNext(true) // After creating tombstone, it should be tombstoned
                .verifyComplete();
                
        StepVerifier.create(operations.isTombstoned("nonexistent"))
                .expectNext(false) // Key that doesn't have a tombstone
                .verifyComplete();
    }

    @Test
    void testGetOperations() {
        when(reactiveCommands.get(anyString())).thenReturn(Mono.just("value"));
        
        StepVerifier.create(operations.get("key"))
                .expectNext("value")
                .verifyComplete();
        
        StepVerifier.create(operations.get("key", DatacenterPreference.LOCAL_ONLY))
                .expectNext("value")
                .verifyComplete();
    }

    @Test
    void testSetOperations() {
        when(reactiveCommands.set(anyString(), anyString())).thenReturn(Mono.just("OK"));
        when(reactiveCommands.setex(anyString(), anyLong(), anyString())).thenReturn(Mono.just("OK"));
        
        StepVerifier.create(operations.set("key", "value"))
                .verifyComplete();
        
        StepVerifier.create(operations.set("key", "value", Duration.ofMinutes(5)))
                .verifyComplete();
        
        StepVerifier.create(operations.set("key", "value", DatacenterPreference.LOCAL_ONLY))
                .verifyComplete();
    }

    @Test
    void testDeleteOperations() {
        when(reactiveCommands.del(anyString())).thenReturn(Mono.just(1L));
        when(reactiveCommands.del(any(String[].class))).thenReturn(Mono.just(2L));
        
        StepVerifier.create(operations.delete("key"))
                .expectNext(true)
                .verifyComplete();
        
        StepVerifier.create(operations.delete("key", DatacenterPreference.LOCAL_ONLY))
                .expectNext(true)
                .verifyComplete();
        
        StepVerifier.create(operations.delete("key1", "key2"))
                .expectNext(2L)
                .verifyComplete();
    }

    @Test
    void testErrorHandling() {
        when(reactiveCommands.get(anyString())).thenReturn(Mono.error(new RuntimeException("Redis error")));
        
        // The get method propagates errors through the resilience layer
        StepVerifier.create(operations.get("key"))
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void testNullAndEmptyResults() {
        when(reactiveCommands.get(anyString())).thenReturn(Mono.empty());
        when(reactiveCommands.exists(anyString())).thenReturn(Mono.just(0L));
        
        StepVerifier.create(operations.get("nonexistent"))
                .expectComplete()
                .verify();
        
        StepVerifier.create(operations.exists("nonexistent"))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void testPingWithNoDatacenters() {
        // Test ping when no datacenters are available - router returns empty
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.empty());
        
        StepVerifier.create(operations.ping())
                .expectNext("No datacenter available")
                .verifyComplete();
    }

    @Test
    void testDbSizeWithNoDatacenters() {
        // Test dbSize when no datacenters are available - router returns empty
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.empty());
        
        StepVerifier.create(operations.dbSize())
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    void testExistsWithNoDatacenters() {
        // Test exists when no datacenters are available - router returns empty
        when(router.selectDatacenterForRead(any())).thenReturn(Optional.empty());
        
        StepVerifier.create(operations.exists("key"))
                .expectNext(false)
                .verifyComplete();
    }
}
