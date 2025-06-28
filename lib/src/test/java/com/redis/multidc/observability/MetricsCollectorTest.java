package com.redis.multidc.observability;

import static org.junit.jupiter.api.Assertions.*;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.model.DatacenterInfo;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;

/**
 * Comprehensive test coverage for MetricsCollector.
 */
@ExtendWith(MockitoExtension.class)
class MetricsCollectorTest {

    private MetricsCollector metricsCollector;
    private MeterRegistry meterRegistry;
    private DatacenterConfiguration configuration;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        
        List<DatacenterEndpoint> endpoints = List.of(
            DatacenterEndpoint.builder()
                .id("us-east-1")
                .region("us-east")
                .host("redis-us-east.example.com")
                .port(6379)
                .build(),
            DatacenterEndpoint.builder()
                .id("us-west-1")
                .region("us-west")
                .host("redis-us-west.example.com")
                .port(6379)
                .build()
        );

        configuration = DatacenterConfiguration.builder()
            .datacenters(endpoints)
            .localDatacenter("us-east-1")
            .build();

        metricsCollector = new MetricsCollector(configuration, meterRegistry);
    }

    @Test
    void testConstructorWithDefaultRegistry() {
        MetricsCollector defaultCollector = new MetricsCollector(configuration);
        assertNotNull(defaultCollector);
    }

    @Test
    void testRecordRequestSuccess() {
        Duration latency = Duration.ofMillis(50);
        
        metricsCollector.recordRequest("us-east-1", latency, true);
        
        // Verify global metrics are recorded
        assertEquals(1.0, meterRegistry.counter("redis.multidc.requests.total").count());
        assertEquals(0.0, meterRegistry.counter("redis.multidc.requests.failed").count());
        
        // Verify datacenter-specific metrics are recorded
        assertEquals(1.0, meterRegistry.counter("redis.multidc.datacenter.requests", "datacenter", "us-east-1").count());
        assertEquals(0.0, meterRegistry.counter("redis.multidc.datacenter.failures", "datacenter", "us-east-1").count());
    }

    @Test
    void testRecordRequestFailure() {
        Duration latency = Duration.ofMillis(100);
        
        metricsCollector.recordRequest("us-east-1", latency, false);
        
        // Verify global failure metrics are recorded
        assertEquals(1.0, meterRegistry.counter("redis.multidc.requests.total").count());
        assertEquals(1.0, meterRegistry.counter("redis.multidc.requests.failed").count());
        
        // Verify datacenter-specific failure metrics are recorded
        assertEquals(1.0, meterRegistry.counter("redis.multidc.datacenter.requests", "datacenter", "us-east-1").count());
        assertEquals(1.0, meterRegistry.counter("redis.multidc.datacenter.failures", "datacenter", "us-east-1").count());
    }

    @Test
    void testRecordMultipleRequests() {
        metricsCollector.recordRequest("us-east-1", Duration.ofMillis(50), true);
        metricsCollector.recordRequest("us-east-1", Duration.ofMillis(75), true);
        metricsCollector.recordRequest("us-east-1", Duration.ofMillis(100), false);
        
        // Verify global metrics
        assertEquals(3.0, meterRegistry.counter("redis.multidc.requests.total").count());
        assertEquals(1.0, meterRegistry.counter("redis.multidc.requests.failed").count());
        
        // Verify datacenter-specific metrics
        assertEquals(3.0, meterRegistry.counter("redis.multidc.datacenter.requests", "datacenter", "us-east-1").count());
        assertEquals(1.0, meterRegistry.counter("redis.multidc.datacenter.failures", "datacenter", "us-east-1").count());
    }

    @Test
    void testRecordRequestsForDifferentDatacenters() {
        metricsCollector.recordRequest("us-east-1", Duration.ofMillis(50), true);
        metricsCollector.recordRequest("us-west-1", Duration.ofMillis(75), true);
        
        // Verify datacenter-specific metrics
        assertEquals(1.0, meterRegistry.counter("redis.multidc.datacenter.requests", "datacenter", "us-east-1").count());
        assertEquals(1.0, meterRegistry.counter("redis.multidc.datacenter.requests", "datacenter", "us-west-1").count());
        
        // Verify global metrics
        assertEquals(2.0, meterRegistry.counter("redis.multidc.requests.total").count());
    }

    @Test
    void testRecordDatacenterUpdate() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("us-east-1")
            .region("us-east")
            .host("redis-us-east.example.com")
            .port(6379)
            .healthy(true)
            .local(true)
            .build();

        metricsCollector.recordDatacenterUpdate(info);
        
        // Should complete without throwing exceptions
        assertDoesNotThrow(() -> metricsCollector.recordDatacenterUpdate(info));
    }

    @Test
    void testRecordConnectionEvent() {
        metricsCollector.recordConnectionEvent("us-east-1", true);
        metricsCollector.recordConnectionEvent("us-east-1", false);
        
        // Should complete without exceptions
        assertDoesNotThrow(() -> metricsCollector.recordConnectionEvent("us-east-1", true));
    }

    @Test
    void testRecordCircuitBreakerEvent() {
        metricsCollector.recordCircuitBreakerEvent("us-east-1", "OPEN");
        
        // Should complete without exceptions
        assertDoesNotThrow(() -> metricsCollector.recordCircuitBreakerEvent("us-east-1", "OPEN"));
    }

    @Test
    void testRecordCacheOperation() {
        metricsCollector.recordCacheOperation("GET", true);
        metricsCollector.recordCacheOperation("SET", false);
        
        // Should complete without exceptions
        assertDoesNotThrow(() -> metricsCollector.recordCacheOperation("GET", true));
    }

    @Test
    void testRecordTombstoneOperation() {
        metricsCollector.recordTombstoneOperation("CREATE", "SOFT_DELETE");
        metricsCollector.recordTombstoneOperation("REMOVE", "CACHE_INVALIDATION");
        
        // Should complete without exceptions
        assertDoesNotThrow(() -> metricsCollector.recordTombstoneOperation("CREATE", "SOFT_DELETE"));
    }

    @Test
    void testGetMeterRegistry() {
        MeterRegistry registry = metricsCollector.getMeterRegistry();
        
        assertNotNull(registry);
        assertSame(meterRegistry, registry);
    }

    @Test
    void testConcurrentMetricsRecording() throws InterruptedException {
        int threadCount = 10;
        int requestsPerThread = 100;
        Thread[] threads = new Thread[threadCount];
        
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < requestsPerThread; j++) {
                    metricsCollector.recordRequest("us-east-1", Duration.ofMillis(50 + threadIndex), j % 2 == 0);
                }
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for completion
        for (Thread thread : threads) {
            thread.join();
        }
        
        // Verify total requests recorded
        double totalRequests = meterRegistry.counter("redis.multidc.datacenter.requests", "datacenter", "us-east-1").count();
        assertEquals(threadCount * requestsPerThread, totalRequests);
        
        // Verify global total as well
        double globalTotal = meterRegistry.counter("redis.multidc.requests.total").count();
        assertEquals(threadCount * requestsPerThread, globalTotal);
    }

    @Test
    void testMetricsWithNullDatacenter() {
        // Should handle null datacenter gracefully
        assertDoesNotThrow(() -> metricsCollector.recordRequest(null, Duration.ofMillis(50), true));
    }

    @Test
    void testMetricsWithZeroLatency() {
        assertDoesNotThrow(() -> metricsCollector.recordRequest("us-east-1", Duration.ZERO, true));
    }

    @Test
    void testMetricsWithNegativeLatency() {
        // Should handle negative latency gracefully (though it shouldn't happen in practice)
        assertDoesNotThrow(() -> metricsCollector.recordRequest("us-east-1", Duration.ofMillis(-10), true));
    }

    @Test
    void testDatacenterMetricsInitialization() {
        // Verify that datacenter metrics are properly initialized for all configured datacenters
        List<String> datacenterIds = configuration.getDatacenters().stream()
            .map(DatacenterEndpoint::getId)
            .toList();
        
        for (String datacenterId : datacenterIds) {
            // Should be able to record metrics for all configured datacenters
            assertDoesNotThrow(() -> metricsCollector.recordRequest(datacenterId, Duration.ofMillis(50), true));
        }
    }
}
