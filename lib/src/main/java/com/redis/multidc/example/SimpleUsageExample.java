package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.model.DatacenterPreference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

/**
 * Simple working example demonstrating basic Redis Multi-Datacenter Client usage.
 * This example shows the core functionality that works with the current implementation.
 */
public class SimpleUsageExample {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleUsageExample.class);
    
    public static void main(String[] args) throws Exception {
        
        logger.info("Starting Redis Multi-Datacenter Client Example");
        
        // 1. Create datacenter configuration
        DatacenterConfiguration config = createDatacenterConfiguration();
        
        // 2. Create multi-datacenter client
        MultiDatacenterRedisClient client = createClient(config);
        
        try {
            // 3. Demonstrate basic operations
            demonstrateBasicOperations(client);
            
        } finally {
            // 4. Cleanup
            client.close();
        }
        
        logger.info("Example completed successfully");
    }
    
    /**
     * Creates a basic datacenter configuration.
     */
    private static DatacenterConfiguration createDatacenterConfiguration() {
        return DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(Arrays.asList(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .region("us-east")
                    .host("localhost")
                    .port(6379)
                    .priority(1)
                    .weight(1.0)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("us-west-1")
                    .region("us-west")
                    .host("localhost")
                    .port(6380)
                    .priority(2)
                    .weight(0.8)
                    .build()
            ))
            .connectionTimeout(Duration.ofSeconds(10))
            .requestTimeout(Duration.ofSeconds(5))
            .healthCheckInterval(Duration.ofSeconds(30))
            .resilienceConfig(com.redis.multidc.config.ResilienceConfig.defaultConfig())
            .build();
    }
    
    /**
     * Creates the multi-datacenter Redis client.
     */
    private static MultiDatacenterRedisClient createClient(DatacenterConfiguration config) {
        return MultiDatacenterRedisClientBuilder.create(config);
    }
    
    /**
     * Demonstrates basic Redis operations across datacenters.
     */
    private static void demonstrateBasicOperations(MultiDatacenterRedisClient client) {
        
        logger.info("=== Basic Operations Demonstration ===");
        
        // 1. Basic string operations
        logger.info("1. Testing string operations...");
        
        String key = "example:user:12345";
        String value = "user-data-example";
        
        // Store data in local datacenter
        client.sync().set(key, value, DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Stored: {} = {}", key, value);
        
        // Retrieve data (will use optimal datacenter)
        String retrieved = client.sync().get(key, DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Retrieved: {} = {}", key, retrieved);
        
        // 2. Datacenter-specific operations
        logger.info("2. Testing datacenter-specific operations...");
        
        // Force write to specific datacenter
        client.sync().set("test:local", "local-value", DatacenterPreference.LOCAL_ONLY);
        client.sync().set("test:remote", "remote-value", DatacenterPreference.REMOTE_ONLY);
        
        logger.info("Stored data with datacenter preferences");
        
        // 3. Hash operations
        logger.info("3. Testing hash operations...");
        
        String hashKey = "user:profile:12345";
        client.sync().hset(hashKey, "name", "John Doe", DatacenterPreference.LOCAL_PREFERRED);
        client.sync().hset(hashKey, "email", "john@example.com", DatacenterPreference.LOCAL_PREFERRED);
        
        String name = client.sync().hget(hashKey, "name", DatacenterPreference.LOCAL_PREFERRED);
        String email = client.sync().hget(hashKey, "email", DatacenterPreference.LOCAL_PREFERRED);
        
        logger.info("Hash data - Name: {}, Email: {}", name, email);
        
        // 4. Expiration operations
        logger.info("4. Testing expiration operations...");
        
        String tempKey = "temp:session:abc123";
        client.sync().set(tempKey, "session-data", Duration.ofMinutes(30), DatacenterPreference.LOCAL_PREFERRED);
        
        Duration ttl = client.sync().ttl(tempKey, DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Session TTL: {} seconds", ttl != null ? ttl.getSeconds() : "unknown");
        
        // 5. Health check demonstration
        logger.info("5. Checking datacenter health...");
        
        java.util.List<com.redis.multidc.model.DatacenterInfo> datacenters = client.getDatacenters();
        for (com.redis.multidc.model.DatacenterInfo dc : datacenters) {
            logger.info("Datacenter: {} - Status: {} - Latency: {}ms", 
                dc.getId(), 
                dc.isHealthy() ? "Healthy" : "Unhealthy",
                dc.getLatencyMs());
        }
        
        logger.info("=== Basic operations demonstration completed ===");
    }
}