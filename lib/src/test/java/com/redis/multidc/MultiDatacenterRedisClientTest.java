package com.redis.multidc;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.RoutingStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic tests for the multi-datacenter Redis client.
 */
class MultiDatacenterRedisClientTest {
    
    @Test
    void testConfigurationBuilder() {
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("dc1")
                    .region("us-east")
                    .host("localhost")
                    .port(6379)
                    .build()
            ))
            .localDatacenter("dc1")
            .routingStrategy(RoutingStrategy.LOCAL_ONLY)
            .build();
        
        assertNotNull(config);
        assertEquals("dc1", config.getLocalDatacenterId());
        assertEquals(RoutingStrategy.LOCAL_ONLY, config.getRoutingStrategy());
        assertEquals(1, config.getDatacenters().size());
    }
    
    @Test
    void testDatacenterEndpointBuilder() {
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
            .id("test-dc")
            .region("test-region")
            .host("localhost")
            .port(6379)
            .ssl(false)
            .priority(1)
            .weight(1.0)
            .build();
        
        assertNotNull(endpoint);
        assertEquals("test-dc", endpoint.getId());
        assertEquals("test-region", endpoint.getRegion());
        assertEquals("localhost", endpoint.getHost());
        assertEquals(6379, endpoint.getPort());
        assertFalse(endpoint.isSsl());
        assertEquals(1, endpoint.getPriority());
        assertEquals(1.0, endpoint.getWeight());
    }
    
    @Test
    void testConnectionStringGeneration() {
        DatacenterEndpoint endpoint = DatacenterEndpoint.builder()
            .id("test")
            .host("localhost")
            .port(6379)
            .build();
        
        assertEquals("redis://localhost:6379", endpoint.getConnectionString());
        
        DatacenterEndpoint sslEndpoint = DatacenterEndpoint.builder()
            .id("test-ssl")
            .host("localhost")
            .port(6380)
            .ssl(true)
            .password("secret")
            .database(1)
            .build();
        
        assertEquals("rediss://:secret@localhost:6380/1", sslEndpoint.getConnectionString());
    }
    
    @Test
    void testConfigurationValidation() {
        // Test missing datacenters
        assertThrows(IllegalArgumentException.class, () -> 
            DatacenterConfiguration.builder()
                .localDatacenter("dc1")
                .build()
        );
        
        // Test missing local datacenter
        assertThrows(IllegalArgumentException.class, () -> 
            DatacenterConfiguration.builder()
                .datacenters(List.of(
                    DatacenterEndpoint.builder()
                        .id("dc1")
                        .host("localhost")
                        .build()
                ))
                .build()
        );
        
        // Test local datacenter not in list
        assertThrows(IllegalArgumentException.class, () -> 
            DatacenterConfiguration.builder()
                .datacenters(List.of(
                    DatacenterEndpoint.builder()
                        .id("dc1")
                        .host("localhost")
                        .build()
                ))
                .localDatacenter("dc2")
                .build()
        );
    }
    
    @Test
    void testEndpointValidation() {
        // Test missing ID
        assertThrows(IllegalArgumentException.class, () -> 
            DatacenterEndpoint.builder()
                .host("localhost")
                .build()
        );
        
        // Test missing host
        assertThrows(IllegalArgumentException.class, () -> 
            DatacenterEndpoint.builder()
                .id("test")
                .build()
        );
        
        // Test invalid port
        assertThrows(IllegalArgumentException.class, () -> 
            DatacenterEndpoint.builder()
                .id("test")
                .host("localhost")
                .port(-1)
                .build()
        );
    }
}
