package com.redis.multidc.pool.impl;

import com.redis.multidc.pool.ConnectionPool;
import com.redis.multidc.pool.ConnectionPoolConfig;
import com.redis.multidc.pool.ConnectionPoolException;
import com.redis.multidc.pool.ConnectionPoolMetrics;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.resource.ClientResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Production-ready Redis connection pool implementation.
 * Provides connection lifecycle management, validation, and metrics.
 */
public class DefaultConnectionPool implements ConnectionPool<String, String> {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionPool.class);
    
    private final String datacenterId;
    private final RedisClient redisClient;
    private final RedisURI redisUri;
    private final ConnectionPoolConfig config;
    private final BlockingQueue<PooledConnectionImpl> availableConnections;
    private final ConcurrentHashMap<PooledConnectionImpl, Boolean> allConnections;
    private final ScheduledExecutorService maintenanceExecutor;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    // Metrics
    private final AtomicLong totalConnectionsCreated = new AtomicLong(0);
    private final AtomicLong totalConnectionsDestroyed = new AtomicLong(0);
    private final AtomicLong totalConnectionsAcquired = new AtomicLong(0);
    private final AtomicLong totalConnectionsReturned = new AtomicLong(0);
    private final AtomicLong totalAcquisitionTimeouts = new AtomicLong(0);
    private final AtomicLong totalValidationFailures = new AtomicLong(0);
    private final AtomicLong peakActiveConnections = new AtomicLong(0);
    private final AtomicReference<Double> averageAcquisitionTime = new AtomicReference<>(0.0);
    private final AtomicReference<Double> averageConnectionAge = new AtomicReference<>(0.0);
    
    public DefaultConnectionPool(String datacenterId, RedisClient redisClient, 
                               RedisURI redisUri, ConnectionPoolConfig config) {
        this.datacenterId = datacenterId;
        this.redisClient = redisClient;
        this.redisUri = redisUri;
        this.config = config;
        this.availableConnections = new LinkedBlockingQueue<>();
        this.allConnections = new ConcurrentHashMap<>();
        this.maintenanceExecutor = Executors.newScheduledThreadPool(
            config.getMaintenanceCorePoolSize(),
            r -> new Thread(r, "connection-pool-maintenance-" + datacenterId)
        );
        
        initialize();
    }
    
    private void initialize() {
        // Create minimum connections
        for (int i = 0; i < config.getMinPoolSize(); i++) {
            try {
                PooledConnectionImpl connection = createConnection();
                availableConnections.offer(connection);
            } catch (Exception e) {
                logger.warn("Failed to create initial connection for datacenter {}: {}", 
                           datacenterId, e.getMessage());
            }
        }
        
        // Start maintenance task
        maintenanceExecutor.scheduleWithFixedDelay(
            this::maintainPool,
            config.getMaintenanceInterval().toMillis(),
            config.getMaintenanceInterval().toMillis(),
            TimeUnit.MILLISECONDS
        );
        
        logger.info("Connection pool initialized for datacenter {} with min={}, max={}", 
                   datacenterId, config.getMinPoolSize(), config.getMaxPoolSize());
    }
    
    @Override
    public CompletableFuture<PooledConnection<String, String>> acquireConnection() {
        return acquireConnection(config.getAcquisitionTimeout());
    }
    
    @Override
    public CompletableFuture<PooledConnection<String, String>> acquireConnection(Duration timeout) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(
                new ConnectionPoolException.PoolClosedException(datacenterId));
        }
        
        long startTime = System.currentTimeMillis();
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Try to get an existing connection first
                PooledConnectionImpl connection = availableConnections.poll();
                
                if (connection != null) {
                    if (isConnectionValid(connection)) {
                        markConnectionAsActive(connection);
                        recordAcquisition(startTime);
                        return connection;
                    } else {
                        // Invalid connection, destroy it
                        destroyConnection(connection);
                    }
                }
                
                // No valid connection available, try to create a new one
                if (allConnections.size() < config.getMaxPoolSize()) {
                    connection = createConnection();
                    markConnectionAsActive(connection);
                    recordAcquisition(startTime);
                    return connection;
                }
                
                // Pool is at capacity, wait for a connection
                connection = availableConnections.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
                if (connection == null) {
                    totalAcquisitionTimeouts.incrementAndGet();
                    throw new ConnectionPoolException.AcquisitionTimeoutException(datacenterId, timeout.toMillis());
                }
                
                if (isConnectionValid(connection)) {
                    markConnectionAsActive(connection);
                    recordAcquisition(startTime);
                    return connection;
                } else {
                    destroyConnection(connection);
                    throw new ConnectionPoolException.PoolExhaustedException(datacenterId, config.getMaxPoolSize());
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                totalAcquisitionTimeouts.incrementAndGet();
                throw new ConnectionPoolException("Connection acquisition interrupted", e);
            } catch (Exception e) {
                if (e instanceof ConnectionPoolException) {
                    throw e;
                }
                throw new ConnectionPoolException("Failed to acquire connection", e);
            }
        });
    }
    
    @Override
    public void returnConnection(PooledConnection<String, String> pooledConnection) {
        if (!(pooledConnection instanceof PooledConnectionImpl)) {
            throw new IllegalArgumentException("Invalid connection type");
        }
        
        PooledConnectionImpl connection = (PooledConnectionImpl) pooledConnection;
        
        if (!allConnections.containsKey(connection)) {
            // Connection doesn't belong to this pool
            return;
        }
        
        connection.updateLastUsedTime();
        
        if (config.isValidateOnReturn() && !isConnectionValid(connection)) {
            destroyConnection(connection);
            return;
        }
        
        if (closed.get()) {
            destroyConnection(connection);
            return;
        }
        
        // Return to available pool
        availableConnections.offer(connection);
        totalConnectionsReturned.incrementAndGet();
    }
    
    @Override
    public ConnectionPoolMetrics getMetrics() {
        int activeConnections = allConnections.size() - availableConnections.size();
        return ConnectionPoolMetrics.builder()
            .datacenterId(datacenterId)
            .maxPoolSize(config.getMaxPoolSize())
            .currentPoolSize(allConnections.size())
            .activeConnections(activeConnections)
            .idleConnections(availableConnections.size())
            .totalConnectionsCreated(totalConnectionsCreated.get())
            .totalConnectionsDestroyed(totalConnectionsDestroyed.get())
            .totalConnectionsAcquired(totalConnectionsAcquired.get())
            .totalConnectionsReturned(totalConnectionsReturned.get())
            .totalAcquisitionTimeouts(totalAcquisitionTimeouts.get())
            .totalValidationFailures(totalValidationFailures.get())
            .averageAcquisitionTime(averageAcquisitionTime.get())
            .averageConnectionAge(averageConnectionAge.get())
            .peakActiveConnections(peakActiveConnections.get())
            .build();
    }
    
    @Override
    public String getDatacenterId() {
        return datacenterId;
    }
    
    @Override
    public boolean isHealthy() {
        return !closed.get() && 
               (availableConnections.size() > 0 || allConnections.size() < config.getMaxPoolSize());
    }
    
    @Override
    public void maintainPool() {
        if (closed.get()) {
            return;
        }
        
        try {
            long currentTime = System.currentTimeMillis();
            
            // Remove expired connections
            allConnections.entrySet().removeIf(entry -> {
                PooledConnectionImpl connection = entry.getKey();
                boolean expired = isConnectionExpired(connection, currentTime);
                if (expired) {
                    availableConnections.remove(connection);
                    destroyConnection(connection);
                }
                return expired;
            });
            
            // Ensure minimum pool size
            while (allConnections.size() < config.getMinPoolSize()) {
                try {
                    PooledConnectionImpl connection = createConnection();
                    availableConnections.offer(connection);
                } catch (Exception e) {
                    logger.warn("Failed to create connection during maintenance for datacenter {}: {}", 
                               datacenterId, e.getMessage());
                    break;
                }
            }
            
            // Update metrics
            updateAverageConnectionAge();
            
        } catch (Exception e) {
            logger.error("Error during pool maintenance for datacenter {}", datacenterId, e);
        }
    }
    
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing connection pool for datacenter {}", datacenterId);
            
            // Stop maintenance
            maintenanceExecutor.shutdown();
            try {
                if (!maintenanceExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    maintenanceExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                maintenanceExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            // Close all connections
            allConnections.keySet().forEach(this::destroyConnection);
            allConnections.clear();
            availableConnections.clear();
            
            logger.info("Connection pool closed for datacenter {}", datacenterId);
        }
    }
    
    private PooledConnectionImpl createConnection() {
        try {
            StatefulRedisConnection<String, String> connection = redisClient.connect(redisUri);
            PooledConnectionImpl pooledConnection = new PooledConnectionImpl(connection, this);
            allConnections.put(pooledConnection, Boolean.TRUE);
            totalConnectionsCreated.incrementAndGet();
            return pooledConnection;
        } catch (Exception e) {
            throw new ConnectionPoolException("Failed to create connection for datacenter " + datacenterId, e);
        }
    }
    
    private void destroyConnection(PooledConnectionImpl connection) {
        try {
            allConnections.remove(connection);
            connection.forceClose();
            totalConnectionsDestroyed.incrementAndGet();
        } catch (Exception e) {
            logger.warn("Error destroying connection for datacenter {}: {}", datacenterId, e.getMessage());
        }
    }
    
    private boolean isConnectionValid(PooledConnectionImpl connection) {
        if (!config.isValidateOnAcquire()) {
            return connection.isValid();
        }
        
        try {
            // Perform ping validation
            CompletableFuture<String> pingFuture = connection.async().ping().toCompletableFuture();
            String result = pingFuture.get(config.getValidationTimeout().toMillis(), TimeUnit.MILLISECONDS);
            boolean valid = "PONG".equals(result) && connection.isValid();
            
            if (!valid) {
                totalValidationFailures.incrementAndGet();
            }
            
            return valid;
        } catch (Exception e) {
            totalValidationFailures.incrementAndGet();
            logger.debug("Connection validation failed for datacenter {}: {}", datacenterId, e.getMessage());
            return false;
        }
    }
    
    private boolean isConnectionExpired(PooledConnectionImpl connection, long currentTime) {
        long connectionAge = currentTime - connection.getCreationTime();
        long idleTime = currentTime - connection.getLastUsedTime();
        
        return connectionAge > config.getMaxConnectionAge().toMillis() ||
               idleTime > config.getIdleTimeout().toMillis();
    }
    
    private void markConnectionAsActive(PooledConnectionImpl connection) {
        connection.updateLastUsedTime();
        totalConnectionsAcquired.incrementAndGet();
        
        int currentActive = allConnections.size() - availableConnections.size();
        peakActiveConnections.updateAndGet(peak -> Math.max(peak, currentActive));
    }
    
    private void recordAcquisition(long startTime) {
        long acquisitionTime = System.currentTimeMillis() - startTime;
        averageAcquisitionTime.updateAndGet(avg -> (avg + acquisitionTime) / 2.0);
    }
    
    private void updateAverageConnectionAge() {
        if (allConnections.isEmpty()) {
            return;
        }
        
        long currentTime = System.currentTimeMillis();
        double totalAge = allConnections.keySet().stream()
            .mapToLong(conn -> currentTime - conn.getCreationTime())
            .average()
            .orElse(0.0);
        
        averageConnectionAge.set(totalAge);
    }
    
    /**
     * Implementation of pooled connection that automatically returns to pool when closed.
     */
    private static class PooledConnectionImpl implements PooledConnection<String, String> {
        
        private final StatefulRedisConnection<String, String> connection;
        private final DefaultConnectionPool pool;
        private final long creationTime;
        private volatile long lastUsedTime;
        private final AtomicBoolean returned = new AtomicBoolean(false);
        
        public PooledConnectionImpl(StatefulRedisConnection<String, String> connection, 
                                  DefaultConnectionPool pool) {
            this.connection = connection;
            this.pool = pool;
            this.creationTime = System.currentTimeMillis();
            this.lastUsedTime = creationTime;
        }
        
        @Override
        public StatefulRedisConnection<String, String> getConnection() {
            return connection;
        }
        
        @Override
        public RedisCommands<String, String> sync() {
            updateLastUsedTime();
            return connection.sync();
        }
        
        @Override
        public RedisAsyncCommands<String, String> async() {
            updateLastUsedTime();
            return connection.async();
        }
        
        @Override
        public RedisReactiveCommands<String, String> reactive() {
            updateLastUsedTime();
            return connection.reactive();
        }
        
        @Override
        public boolean isValid() {
            return connection.isOpen();
        }
        
        @Override
        public long getCreationTime() {
            return creationTime;
        }
        
        @Override
        public long getLastUsedTime() {
            return lastUsedTime;
        }
        
        public void updateLastUsedTime() {
            this.lastUsedTime = System.currentTimeMillis();
        }
        
        @Override
        public void close() {
            if (returned.compareAndSet(false, true)) {
                pool.returnConnection(this);
            }
        }
        
        public void forceClose() {
            try {
                connection.close();
            } catch (Exception e) {
                // Ignore errors during force close
            }
        }
    }
}
