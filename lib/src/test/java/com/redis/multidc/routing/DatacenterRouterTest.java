package com.redis.multidc.routing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.RoutingStrategy;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.observability.MetricsCollector;

import java.util.*;

@ExtendWith(MockitoExtension.class)
class DatacenterRouterTest {

    @Mock
    private MetricsCollector metricsCollector;
    
    @Mock
    private DatacenterConfiguration datacenterConfiguration;

    private DatacenterRouter datacenterRouter;

    @BeforeEach
    void setUp() {
        List<DatacenterEndpoint> endpoints = Arrays.asList(
            DatacenterEndpoint.builder()
                .id("us-east-1")
                .region("US East")
                .host("redis-1.example.com")
                .port(6379)
                .build(),
            DatacenterEndpoint.builder()
                .id("us-west-2")
                .region("US West")
                .host("redis-2.example.com")
                .port(6379)
                .build()
        );
        
        when(datacenterConfiguration.getDatacenters()).thenReturn(endpoints);
        when(datacenterConfiguration.getLocalDatacenterId()).thenReturn("us-east-1");
        lenient().when(datacenterConfiguration.getRoutingStrategy()).thenReturn(RoutingStrategy.LATENCY_BASED);
        
        datacenterRouter = new DatacenterRouter(datacenterConfiguration, metricsCollector);
    }

    @Test
    void testSelectDatacenterForRead_LocalPreferred() {
        Optional<String> result = datacenterRouter.selectDatacenterForRead(DatacenterPreference.LOCAL_PREFERRED);
        
        assertTrue(result.isPresent());
        assertEquals("us-east-1", result.get());
    }

    @Test
    void testSelectDatacenterForWrite_LocalOnly() {
        Optional<String> result = datacenterRouter.selectDatacenterForWrite(DatacenterPreference.LOCAL_ONLY);
        
        assertTrue(result.isPresent());
        assertEquals("us-east-1", result.get());
    }

    @Test
    void testSelectDatacenterForRead_AnyAvailable() {
        Optional<String> result = datacenterRouter.selectDatacenterForRead(DatacenterPreference.ANY_AVAILABLE);
        
        assertTrue(result.isPresent());
        assertTrue(Arrays.asList("us-east-1", "us-west-2").contains(result.get()));
    }

    @Test
    void testGetAllDatacenterInfo() {
        List<DatacenterInfo> result = datacenterRouter.getAllDatacenterInfo();
        
        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(info -> "us-east-1".equals(info.getId())));
        assertTrue(result.stream().anyMatch(info -> "us-west-2".equals(info.getId())));
    }

    @Test
    void testGetLocalDatacenter() {
        DatacenterInfo result = datacenterRouter.getLocalDatacenter();
        
        assertNotNull(result);
        assertEquals("us-east-1", result.getId());
        assertTrue(result.isLocal());
    }

    @Test
    void testUpdateDatacenterInfo() {
        DatacenterInfo info = DatacenterInfo.builder()
            .id("us-east-1")
            .region("US East")
            .host("redis-1.example.com")
            .port(6379)
            .local(true)
            .healthy(true)
            .build();

        datacenterRouter.updateDatacenterInfo(info);

        verify(metricsCollector).recordDatacenterUpdate(info);
    }
}
