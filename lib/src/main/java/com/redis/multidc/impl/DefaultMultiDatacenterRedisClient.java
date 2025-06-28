package com.redis.multidc.impl;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.operations.AsyncOperations;
import com.redis.multidc.operations.ReactiveOperations;
import com.redis.multidc.operations.SyncOperations;
import com.redis.multidc.routing.DatacenterRouter;
import com.redis.multidc.routing.DatacenterHealthMonitor;
import com.redis.multidc.observability.MetricsCollector;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.resource.ClientResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default implementation of the multi-datacenter Redis client.
 * Manages connections to multiple Redis datacenters and provides
 * intelligent routing with fault tolerance.
 */
public class DefaultMultiDatacenterRedisClient implements MultiDatacenterRedisClient {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultMultiDatacenterRedisClient.class);
    
    private final DatacenterConfiguration configuration;
    private final Map<String, RedisClient> clients;
    private final Map<String, StatefulRedisConnection<String, String>> connections;
    private final Map<String, RedisReactiveCommands<String, String>> reactiveConnections;
    private final DatacenterRouter router;
    private final DatacenterHealthMonitor healthMonitor;
    private final MetricsCollector metricsCollector;
    private final SyncOperations syncOperations;
    private final AsyncOperations asyncOperations;
    private final ReactiveOperations reactiveOperations;
    private final Sinks.Many<DatacenterHealthEvent> healthEventSink;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    public DefaultMultiDatacenterRedisClient(DatacenterConfiguration configuration) {
        this.configuration = configuration;
        this.clients = new ConcurrentHashMap<>();
        this.connections = new ConcurrentHashMap<>();
        this.reactiveConnections = new ConcurrentHashMap<>();
        this.healthEventSink = Sinks.many().multicast().onBackpressureBuffer();
        
        try {
            // Initialize clients and connections for all datacenters
            initializeConnections();
            
            // Initialize core components
            this.metricsCollector = new MetricsCollector(configuration);
            this.router = new DatacenterRouter(configuration, metricsCollector);
            this.healthMonitor = new DatacenterHealthMonitor(configuration, connections);
            
            // Initialize operation interfaces
            this.syncOperations = new SyncOperationsImpl(router, connections, metricsCollector);
            this.asyncOperations = new AsyncOperationsImpl(router, connections, metricsCollector);
            this.reactiveOperations = new ReactiveOperationsImpl(reactiveConnections, router, metricsCollector, configuration);
            
            // Start health monitoring
            healthMonitor.start();
            
            logger.info("Multi-datacenter Redis client initialized with {} datacenters", 
                       configuration.getDatacenters().size());
            
        } catch (Exception e) {
            logger.error("Failed to initialize multi-datacenter Redis client", e);
            cleanup();
            throw new RuntimeException("Failed to initialize Redis client", e);
        }
    }
    
    private void initializeConnections() {
        ClientResources clientResources = ClientResources.builder()
            .build();
            
        for (var datacenter : configuration.getDatacenters()) {
            try {
                RedisURI uri = RedisURI.create(datacenter.getConnectionString());
                uri.setTimeout(configuration.getConnectionTimeout());
                
                RedisClient client = RedisClient.create(clientResources, uri);
                StatefulRedisConnection<String, String> connection = client.connect();
                RedisReactiveCommands<String, String> reactiveCommands = connection.reactive();
                
                clients.put(datacenter.getId(), client);
                connections.put(datacenter.getId(), connection);
                reactiveConnections.put(datacenter.getId(), reactiveCommands);
                
                logger.info("Connected to datacenter: {} at {}", datacenter.getId(), datacenter.getHost());
                
            } catch (Exception e) {
                logger.error("Failed to connect to datacenter: {}", datacenter.getId(), e);
                // Continue with other datacenters, but log the failure
            }
        }
        
        if (connections.isEmpty()) {
            throw new RuntimeException("Failed to connect to any datacenter");
        }
    }
    
    @Override
    public SyncOperations sync() {
        checkNotClosed();
        return syncOperations;
    }
    
    @Override
    public AsyncOperations async() {
        checkNotClosed();
        return asyncOperations;
    }
    
    @Override
    public ReactiveOperations reactive() {
        checkNotClosed();
        return reactiveOperations;
    }
    
    @Override
    public List<DatacenterInfo> getDatacenters() {
        checkNotClosed();
        return router.getAllDatacenterInfo();
    }
    
    @Override
    public DatacenterInfo getLocalDatacenter() {
        checkNotClosed();
        return router.getLocalDatacenter();
    }
    
    @Override
    public CompletableFuture<List<DatacenterInfo>> checkDatacenterHealth() {
        checkNotClosed();
        return healthMonitor.checkAllDatacenters();
    }
    
    @Override
    public CompletableFuture<Void> refreshDatacenterInfo() {
        checkNotClosed();
        return CompletableFuture.runAsync(() -> {
            router.refreshRoutingInfo();
            healthMonitor.forceHealthCheck();
        });
    }
    
    @Override
    public DatacenterConfiguration getConfiguration() {
        return configuration;
    }
    
    @Override
    public Disposable subscribeToHealthChanges(DatacenterHealthListener listener) {
        checkNotClosed();
        return healthEventSink.asFlux()
            .subscribe(event -> listener.onHealthChange(event.getDatacenterInfo(), event.isHealthy()));
    }
    
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing multi-datacenter Redis client...");
            cleanup();
        }
    }
    
    private void cleanup() {
        try {
            // Stop health monitoring
            if (healthMonitor != null) {
                healthMonitor.stop();
            }
            
            // Close all connections
            connections.values().forEach(connection -> {
                try {
                    connection.close();
                } catch (Exception e) {
                    logger.warn("Error closing connection", e);
                }
            });
            
            // Shutdown all clients
            clients.values().forEach(client -> {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    logger.warn("Error shutting down client", e);
                }
            });
            
            connections.clear();
            clients.clear();
            
            logger.info("Multi-datacenter Redis client closed successfully");
            
        } catch (Exception e) {
            logger.error("Error during cleanup", e);
        }
    }
    
    private void checkNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("Redis client has been closed");
        }
    }
    
    /**
     * Internal class for health change events.
     */
    private static class DatacenterHealthEvent {
        private final DatacenterInfo datacenterInfo;
        private final boolean healthy;
        
        public DatacenterHealthEvent(DatacenterInfo datacenterInfo, boolean healthy) {
            this.datacenterInfo = datacenterInfo;
            this.healthy = healthy;
        }
        
        public DatacenterInfo getDatacenterInfo() {
            return datacenterInfo;
        }
        
        public boolean isHealthy() {
            return healthy;
        }
    }
}
