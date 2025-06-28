package com.redis.multidc.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.observability.MetricsCollector;
import com.redis.multidc.routing.DatacenterRouter;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.StatefulRedisConnection;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class SyncOperationsImplSimpleTest {

    @Mock
    private DatacenterRouter router;
    
    @Mock
    private MetricsCollector metricsCollector;
    
    @Mock
    private StatefulRedisConnection<String, String> connection;
    
    @Mock
    private RedisCommands<String, String> syncCommands;
    
    private Map<String, StatefulRedisConnection<String, String>> connections;
    private SyncOperationsImpl syncOperations;

    @BeforeEach
    void setUp() {
        connections = new HashMap<>();
        connections.put("us-east-1", connection);
        
        when(connection.sync()).thenReturn(syncCommands);
        lenient().when(router.selectDatacenterForRead(any())).thenReturn(Optional.of("us-east-1"));
        lenient().when(router.selectDatacenterForWrite(any())).thenReturn(Optional.of("us-east-1"));

        syncOperations = new SyncOperationsImpl(router, connections, metricsCollector);
    }

    @Test
    void testGetSuccess() {
        when(syncCommands.get("key")).thenReturn("value");

        String result = syncOperations.get("key");

        assertEquals("value", result);
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(true));
    }

    @Test
    void testSetSuccess() {
        when(syncCommands.set("key", "value")).thenReturn("OK");

        syncOperations.set("key", "value");

        verify(syncCommands).set("key", "value");
        verify(metricsCollector).recordRequest(eq("us-east-1"), any(Duration.class), eq(true));
    }

    @Test
    void testDeleteSuccess() {
        when(syncCommands.del("key")).thenReturn(1L);

        boolean result = syncOperations.delete("key");

        assertTrue(result);
        verify(syncCommands).del("key");
    }

    @Test
    void testExistsSuccess() {
        when(syncCommands.exists("key")).thenReturn(1L);

        boolean result = syncOperations.exists("key");

        assertTrue(result);
        verify(syncCommands).exists("key");
    }

    @Test
    void testGetWithPreference() {
        when(syncCommands.get("key")).thenReturn("value");

        String result = syncOperations.get("key", DatacenterPreference.ANY_AVAILABLE);

        assertEquals("value", result);
        verify(router).selectDatacenterForRead(DatacenterPreference.ANY_AVAILABLE);
    }
}
