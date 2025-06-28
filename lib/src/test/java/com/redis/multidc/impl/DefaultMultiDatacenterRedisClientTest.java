package com.redis.multidc.impl;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DefaultMultiDatacenterRedisClientTest {

    private DefaultMultiDatacenterRedisClient client;
    private DatacenterConfiguration config;

    @BeforeEach
    void setUp() {
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
        
        client = new DefaultMultiDatacenterRedisClient(config);
    }

    @Test
    void testGetSyncOperations() {
        assertNotNull(client.sync());
        assertTrue(client.sync() instanceof SyncOperationsImpl);
    }

    @Test
    void testGetAsyncOperations() {
        assertNotNull(client.async());
        assertTrue(client.async() instanceof AsyncOperationsImpl);
    }

    @Test
    void testGetReactiveOperations() {
        assertNotNull(client.reactive());
        assertTrue(client.reactive() instanceof ReactiveOperationsImpl);
    }

    @Test
    void testGetDatacenters() {
        assertFalse(client.getDatacenters().isEmpty());
        assertEquals("dc1", client.getDatacenters().get(0).getId());
    }

    @Test
    void testClose() {
        // Should not throw exceptions
        assertDoesNotThrow(() -> client.close());
    }

    @Test
    void testMultipleCloses() {
        // Should be safe to call close multiple times
        assertDoesNotThrow(() -> {
            client.close();
            client.close();
        });
    }
}
