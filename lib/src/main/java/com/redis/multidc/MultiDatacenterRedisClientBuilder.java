package com.redis.multidc;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.impl.DefaultMultiDatacenterRedisClient;

/**
 * Builder for creating MultiDatacenterRedisClient instances.
 * Provides a fluent API for configuring and creating Redis clients.
 */
public class MultiDatacenterRedisClientBuilder {
    
    /**
     * Create a new Redis client with the given configuration.
     * 
     * @param configuration The datacenter configuration
     * @return A new MultiDatacenterRedisClient instance
     */
    public static MultiDatacenterRedisClient create(DatacenterConfiguration configuration) {
        return new DefaultMultiDatacenterRedisClient(configuration);
    }
    
    /**
     * Start building a new Redis client configuration.
     * 
     * @return A new configuration builder
     */
    public static DatacenterConfiguration.Builder builder() {
        return DatacenterConfiguration.builder();
    }
}
