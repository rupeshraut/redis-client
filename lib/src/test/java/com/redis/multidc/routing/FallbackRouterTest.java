package com.redis.multidc.routing;

import com.redis.multidc.config.*;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.observability.MetricsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FallbackRouterTest {
    
    @Mock
    private DatacenterRouter primaryRouter;
    
    @Mock
    private MetricsCollector metricsCollector;
    
    private DatacenterConfiguration configuration;
    private FallbackRouter fallbackRouter;
    
    @BeforeEach
    void setUp() {
        List<DatacenterEndpoint> datacenters = List.of(
            DatacenterEndpoint.builder()
                .id("us-east-1")
                .region("us-east")
                .host("redis-us-east.example.com")
                .port(6379)
                .priority(1)
                .weight(0.5)
                .build(),
            DatacenterEndpoint.builder()
                .id("us-west-2")
                .region("us-west")
                .host("redis-us-west.example.com")
                .port(6379)
                .priority(2)
                .weight(0.3)
                .build(),
            DatacenterEndpoint.builder()
                .id("eu-central-1")
                .region("eu-central")
                .host("redis-eu.example.com")
                .port(6379)
                .priority(3)
                .weight(0.2)
                .build()
        );
        
        configuration = DatacenterConfiguration.builder()
            .datacenters(datacenters)
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .fallbackConfiguration(FallbackConfiguration.defaultConfig())
            .build();
    }
    
    @Test
    void testNextAvailableFallbackStrategy() {
        // Configure NEXT_AVAILABLE fallback
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.NEXT_AVAILABLE)
            .fallbackTimeout(Duration.ofSeconds(5))
            .maxFallbackAttempts(3)
            .build();
        
        configuration = DatacenterConfiguration.builder()
            .datacenters(configuration.getDatacenters())
            .localDatacenter("us-east-1")
            .routingStrategy(RoutingStrategy.LATENCY_BASED)
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        fallbackRouter = new FallbackRouter(configuration, primaryRouter, metricsCollector);
        
        // Mock available datacenters
        List<DatacenterInfo> availableDatacenters = List.of(
            DatacenterInfo.builder()
                .id("us-west-2")
                .region("us-west")
                .host("redis-us-west.example.com")
                .port(6379)
                .healthy(true)
                .latencyMs(50)
                .build(),
            DatacenterInfo.builder()
                .id("eu-central-1")
                .region("eu-central")
                .host("redis-eu.example.com")
                .port(6379)
                .healthy(true)
                .latencyMs(100)
                .build()
        );
        
        when(primaryRouter.getAllDatacenterInfo()).thenReturn(availableDatacenters);
        
        // Test fallback selection
        Optional<String> result = fallbackRouter.selectDatacenterWithFallback(
            DatacenterPreference.LOCAL_PREFERRED, true);
        
        assertTrue(result.isPresent());
        assertEquals("us-west-2", result.get()); // Should select lowest latency
    }
    
    @Test
    void testTryAllFallbackStrategy() {
        // Configure TRY_ALL fallback with specific order
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.TRY_ALL)
            .fallbackTimeout(Duration.ofSeconds(10))
            .fallbackDatacenterOrder(List.of("eu-central-1", "us-west-2", "us-east-1"))
            .build();
        
        configuration = DatacenterConfiguration.builder()
            .datacenters(configuration.getDatacenters())
            .localDatacenter("us-east-1")
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        fallbackRouter = new FallbackRouter(configuration, primaryRouter, metricsCollector);
        
        // Mock available datacenters
        List<DatacenterInfo> availableDatacenters = List.of(
            DatacenterInfo.builder()
                .id("us-west-2")
                .region("us-west")
                .healthy(true)
                .build(),
            DatacenterInfo.builder()
                .id("eu-central-1")
                .region("eu-central")
                .healthy(true)
                .build()
        );
        
        when(primaryRouter.getAllDatacenterInfo()).thenReturn(availableDatacenters);
        
        // Should select eu-central-1 as it's first in the configured order
        Optional<String> result = fallbackRouter.selectDatacenterWithFallback(
            DatacenterPreference.LOCAL_PREFERRED, true);
        
        assertTrue(result.isPresent());
        assertEquals("eu-central-1", result.get());
    }
    
    @Test
    void testLocalOnlyFallbackStrategy() {
        // Configure LOCAL_ONLY fallback
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.LOCAL_ONLY)
            .build();
        
        configuration = DatacenterConfiguration.builder()
            .datacenters(configuration.getDatacenters())
            .localDatacenter("us-east-1")
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        fallbackRouter = new FallbackRouter(configuration, primaryRouter, metricsCollector);
        
        // Mock local and remote datacenters
        List<DatacenterInfo> availableDatacenters = List.of(
            DatacenterInfo.builder()
                .id("us-east-1")
                .region("us-east")
                .local(true)
                .healthy(true)
                .build(),
            DatacenterInfo.builder()
                .id("us-west-2")
                .region("us-west")
                .local(false)
                .healthy(true)
                .build()
        );
        
        when(primaryRouter.getAllDatacenterInfo()).thenReturn(availableDatacenters);
        
        // Should only select local datacenter
        Optional<String> result = fallbackRouter.selectDatacenterWithFallback(
            DatacenterPreference.ANY_AVAILABLE, true);
        
        assertTrue(result.isPresent());
        assertEquals("us-east-1", result.get());
    }
    
    @Test
    void testRemoteOnlyFallbackStrategy() {
        // Configure REMOTE_ONLY fallback
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.REMOTE_ONLY)
            .build();
        
        configuration = DatacenterConfiguration.builder()
            .datacenters(configuration.getDatacenters())
            .localDatacenter("us-east-1")
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        fallbackRouter = new FallbackRouter(configuration, primaryRouter, metricsCollector);
        
        // Mock local and remote datacenters
        List<DatacenterInfo> availableDatacenters = List.of(
            DatacenterInfo.builder()
                .id("us-east-1")
                .region("us-east")
                .local(true)
                .healthy(true)
                .latencyMs(10)
                .build(),
            DatacenterInfo.builder()
                .id("us-west-2")
                .region("us-west")
                .local(false)
                .healthy(true)
                .latencyMs(50)
                .build(),
            DatacenterInfo.builder()
                .id("eu-central-1")
                .region("eu-central")
                .local(false)
                .healthy(true)
                .latencyMs(100)
                .build()
        );
        
        when(primaryRouter.getAllDatacenterInfo()).thenReturn(availableDatacenters);
        
        // Should select remote datacenter with lowest latency
        Optional<String> result = fallbackRouter.selectDatacenterWithFallback(
            DatacenterPreference.ANY_AVAILABLE, true);
        
        assertTrue(result.isPresent());
        assertEquals("us-west-2", result.get());
    }
    
    @Test
    void testBestEffortFallbackStrategy() {
        // Configure BEST_EFFORT fallback
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.BEST_EFFORT)
            .enableStaleReads(true)
            .staleReadTolerance(Duration.ofMinutes(5))
            .build();
        
        configuration = DatacenterConfiguration.builder()
            .datacenters(configuration.getDatacenters())
            .localDatacenter("us-east-1")
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        fallbackRouter = new FallbackRouter(configuration, primaryRouter, metricsCollector);
        
        // Mock mix of healthy and unhealthy datacenters
        List<DatacenterInfo> availableDatacenters = List.of(
            DatacenterInfo.builder()
                .id("us-east-1")
                .healthy(false) // Unhealthy
                .latencyMs(10)
                .build(),
            DatacenterInfo.builder()
                .id("us-west-2")
                .healthy(true)
                .latencyMs(50)
                .build()
        );
        
        when(primaryRouter.getAllDatacenterInfo()).thenReturn(availableDatacenters);
        
        // Should prefer healthy datacenter
        Optional<String> result = fallbackRouter.selectDatacenterWithFallback(
            DatacenterPreference.LOCAL_PREFERRED, true);
        
        assertTrue(result.isPresent());
        assertEquals("us-west-2", result.get());
    }
    
    @Test
    void testFailFastStrategy() {
        // Configure FAIL_FAST fallback
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.FAIL_FAST)
            .build();
        
        configuration = DatacenterConfiguration.builder()
            .datacenters(configuration.getDatacenters())
            .localDatacenter("us-east-1")
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        fallbackRouter = new FallbackRouter(configuration, primaryRouter, metricsCollector);
        
        // Should return empty result for FAIL_FAST
        Optional<String> result = fallbackRouter.selectDatacenterWithFallback(
            DatacenterPreference.LOCAL_PREFERRED, true);
        
        assertTrue(result.isEmpty());
    }
    
    @Test
    void testCustomFallbackStrategy() {
        // Configure CUSTOM fallback with logic
        FallbackConfiguration fallbackConfig = FallbackConfiguration.builder()
            .strategy(FallbackStrategy.CUSTOM)
            .customFallbackLogic(() -> "eu-central-1") // Always return EU
            .build();
        
        configuration = DatacenterConfiguration.builder()
            .datacenters(configuration.getDatacenters())
            .localDatacenter("us-east-1")
            .fallbackConfiguration(fallbackConfig)
            .build();
        
        fallbackRouter = new FallbackRouter(configuration, primaryRouter, metricsCollector);
        
        // Should use custom logic
        Optional<String> result = fallbackRouter.selectDatacenterWithFallback(
            DatacenterPreference.LOCAL_PREFERRED, true);
        
        assertTrue(result.isPresent());
        assertEquals("eu-central-1", result.get());
    }
}
