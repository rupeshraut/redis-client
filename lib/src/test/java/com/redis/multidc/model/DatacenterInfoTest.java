package com.redis.multidc.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Instant;

/**
 * Comprehensive tests for DatacenterInfo to improve mutation score
 */
class DatacenterInfoTest {

    @Test
    void testBuilderBasic() {
        String id = "us-east-1";
        String region = "us-east";
        String host = "redis.us-east-1.amazonaws.com";
        int port = 6379;
        
        DatacenterInfo info = DatacenterInfo.builder()
            .id(id)
            .region(region)
            .host(host)
            .port(port)
            .build();
        
        assertEquals(id, info.getId());
        assertEquals(region, info.getRegion());
        assertEquals(host, info.getHost());
        assertEquals(port, info.getPort());
        
        // Test defaults
        assertTrue(info.isHealthy());
        assertEquals(0L, info.getLatencyMs());
        assertNotNull(info.getLastHealthCheck());
        assertEquals(0, info.getActiveConnections());
        assertEquals(0L, info.getTotalRequests());
        assertEquals(0L, info.getFailedRequests());
        assertEquals(CircuitBreakerState.CLOSED, info.getCircuitBreakerState());
        assertFalse(info.isLocal());
    }

    @Test
    void testBuilderWithAllFields() {
        String id = "us-west-1";
        String region = "us-west";
        String host = "redis.us-west-1.amazonaws.com";
        int port = 6380;
        boolean healthy = false;
        long latencyMs = 150L;
        Instant lastHealthCheck = Instant.now().minusSeconds(30);
        int activeConnections = 10;
        long totalRequests = 1000L;
        long failedRequests = 50L;
        CircuitBreakerState circuitState = CircuitBreakerState.HALF_OPEN;
        boolean local = true;
        
        DatacenterInfo info = DatacenterInfo.builder()
            .id(id)
            .region(region)
            .host(host)
            .port(port)
            .healthy(healthy)
            .latencyMs(latencyMs)
            .lastHealthCheck(lastHealthCheck)
            .activeConnections(activeConnections)
            .totalRequests(totalRequests)
            .failedRequests(failedRequests)
            .circuitBreakerState(circuitState)
            .local(local)
            .build();
        
        assertEquals(id, info.getId());
        assertEquals(region, info.getRegion());
        assertEquals(host, info.getHost());
        assertEquals(port, info.getPort());
        assertEquals(healthy, info.isHealthy());
        assertEquals(latencyMs, info.getLatencyMs());
        assertEquals(lastHealthCheck, info.getLastHealthCheck());
        assertEquals(activeConnections, info.getActiveConnections());
        assertEquals(totalRequests, info.getTotalRequests());
        assertEquals(failedRequests, info.getFailedRequests());
        assertEquals(circuitState, info.getCircuitBreakerState());
        assertEquals(local, info.isLocal());
    }

    @Test
    void testSuccessRateWithNoRequests() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .totalRequests(0L)
            .failedRequests(0L)
            .build();
        
        assertEquals(100.0, info.getSuccessRate(), 0.001);
    }

    @Test
    void testSuccessRateWithAllSuccessfulRequests() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .totalRequests(100L)
            .failedRequests(0L)
            .build();
        
        assertEquals(100.0, info.getSuccessRate(), 0.001);
    }

    @Test
    void testSuccessRateWithSomeFailures() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .totalRequests(100L)
            .failedRequests(25L)
            .build();
        
        assertEquals(75.0, info.getSuccessRate(), 0.001);
    }

    @Test
    void testSuccessRateWithAllFailures() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .totalRequests(100L)
            .failedRequests(100L)
            .build();
        
        assertEquals(0.0, info.getSuccessRate(), 0.001);
    }

    @Test
    void testIsAvailableHealthyAndClosed() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .healthy(true)
            .circuitBreakerState(CircuitBreakerState.CLOSED)
            .build();
        
        assertTrue(info.isAvailable());
    }

    @Test
    void testIsAvailableHealthyAndHalfOpen() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .healthy(true)
            .circuitBreakerState(CircuitBreakerState.HALF_OPEN)
            .build();
        
        assertTrue(info.isAvailable());
    }

    @Test
    void testIsAvailableHealthyButOpen() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .healthy(true)
            .circuitBreakerState(CircuitBreakerState.OPEN)
            .build();
        
        assertFalse(info.isAvailable());
    }

    @Test
    void testIsAvailableUnhealthyAndClosed() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .healthy(false)
            .circuitBreakerState(CircuitBreakerState.CLOSED)
            .build();
        
        assertFalse(info.isAvailable());
    }

    @Test
    void testIsAvailableUnhealthyAndOpen() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .healthy(false)
            .circuitBreakerState(CircuitBreakerState.OPEN)
            .build();
        
        assertFalse(info.isAvailable());
    }

    @Test
    void testToString() {
        String id = "test-dc";
        String region = "test-region";
        String host = "test-host";
        int port = 6379;
        
        DatacenterInfo info = DatacenterInfo.builder()
            .id(id)
            .region(region)
            .host(host)
            .port(port)
            .healthy(true)
            .latencyMs(50L)
            .circuitBreakerState(CircuitBreakerState.CLOSED)
            .local(true)
            .totalRequests(100L)
            .failedRequests(5L)
            .build();
        
        String toString = info.toString();
        assertTrue(toString.contains(id));
        assertTrue(toString.contains(region));
        assertTrue(toString.contains(host));
        assertTrue(toString.contains(String.valueOf(port)));
        assertTrue(toString.contains("true")); // healthy
        assertTrue(toString.contains("50")); // latency
        assertTrue(toString.contains("95.0")); // success rate
        assertTrue(toString.contains("CLOSED"));
        assertTrue(toString.contains("DatacenterInfo"));
    }

    @Test
    void testAllCircuitBreakerStates() {
        for (CircuitBreakerState state : CircuitBreakerState.values()) {
            DatacenterInfo info = DatacenterInfo.builder()
                .id("test")
                .region("test")
                .host("localhost")
                .port(6379)
                .circuitBreakerState(state)
                .build();
            
            assertEquals(state, info.getCircuitBreakerState());
            
            // Test availability logic for each state
            if (state == CircuitBreakerState.OPEN) {
                assertFalse(info.isAvailable()); // Healthy but circuit open
            } else {
                assertTrue(info.isAvailable()); // Healthy and not open
            }
        }
    }

    @Test
    void testBoundaryConditions() {
        // Test edge cases for success rate calculation
        DatacenterInfo info1 = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .totalRequests(1L)
            .failedRequests(1L)
            .build();
        
        assertEquals(0.0, info1.getSuccessRate(), 0.001);
        
        DatacenterInfo info2 = DatacenterInfo.builder()
            .id("test")
            .region("test")
            .host("localhost")
            .port(6379)
            .totalRequests(1L)
            .failedRequests(0L)
            .build();
        
        assertEquals(100.0, info2.getSuccessRate(), 0.001);
    }

    @Test
    void testConstructorDirectly() {
        String id = "direct-test";
        String region = "direct-region";
        String host = "direct-host";
        int port = 6379;
        boolean healthy = true;
        long latencyMs = 100L;
        Instant lastHealthCheck = Instant.now();
        int activeConnections = 5;
        long totalRequests = 200L;
        long failedRequests = 10L;
        CircuitBreakerState circuitState = CircuitBreakerState.HALF_OPEN;
        boolean local = false;
        
        DatacenterInfo info = new DatacenterInfo(
            id, region, host, port, healthy, latencyMs, lastHealthCheck,
            activeConnections, totalRequests, failedRequests, circuitState, local
        );
        
        assertEquals(id, info.getId());
        assertEquals(region, info.getRegion());
        assertEquals(host, info.getHost());
        assertEquals(port, info.getPort());
        assertEquals(healthy, info.isHealthy());
        assertEquals(latencyMs, info.getLatencyMs());
        assertEquals(lastHealthCheck, info.getLastHealthCheck());
        assertEquals(activeConnections, info.getActiveConnections());
        assertEquals(totalRequests, info.getTotalRequests());
        assertEquals(failedRequests, info.getFailedRequests());
        assertEquals(circuitState, info.getCircuitBreakerState());
        assertEquals(local, info.isLocal());
    }
}
