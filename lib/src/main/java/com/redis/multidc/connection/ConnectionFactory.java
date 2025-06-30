package com.redis.multidc.connection;

import com.redis.multidc.config.DatacenterEndpoint;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.SslOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for creating and managing Lettuce Redis connections with optimized configuration.
 * Provides connection lifecycle management, SSL/TLS support, and performance optimization.
 */
public class ConnectionFactory implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(ConnectionFactory.class);
    
    private final ConnectionFactoryConfig config;
    private final ClientResources clientResources;
    private final ConcurrentMap<String, RedisClient> clients;
    private volatile boolean closed = false;
    
    public ConnectionFactory(ConnectionFactoryConfig config) {
        this.config = config;
        this.clientResources = createClientResources();
        this.clients = new ConcurrentHashMap<>();
        
        logger.info("ConnectionFactory initialized with configuration: {}", config);
    }
    
    /**
     * Creates a Redis connection for the specified datacenter endpoint.
     * 
     * @param endpoint the datacenter endpoint configuration
     * @return future with the established connection
     */
    public CompletableFuture<StatefulRedisConnection<String, String>> createConnection(DatacenterEndpoint endpoint) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("ConnectionFactory is closed"));
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                RedisClient client = getOrCreateClient(endpoint);
                RedisURI redisUri = buildRedisURI(endpoint);
                
                StatefulRedisConnection<String, String> connection = client.connect(redisUri);
                
                logger.debug("Created connection to datacenter: {} ({}:{})", 
                    endpoint.getId(), endpoint.getHost(), endpoint.getPort());
                
                return connection;
                
            } catch (Exception e) {
                logger.error("Failed to create connection to datacenter: {} ({}:{})", 
                    endpoint.getId(), endpoint.getHost(), endpoint.getPort(), e);
                throw new ConnectionCreationException("Failed to create connection to " + endpoint.getId(), e);
            }
        });
    }
    
    /**
     * Creates an async Redis connection for the specified datacenter endpoint.
     * 
     * @param endpoint the datacenter endpoint configuration
     * @return future with the established async connection
     */
    public CompletableFuture<StatefulRedisConnection<String, String>> createAsyncConnection(DatacenterEndpoint endpoint) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("ConnectionFactory is closed"));
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                RedisClient client = getOrCreateClient(endpoint);
                RedisURI redisUri = buildRedisURI(endpoint);
                
                // Use connectAsync for non-blocking connection establishment
                return client.connectAsync(io.lettuce.core.codec.StringCodec.UTF8, redisUri).get();
                
            } catch (Exception e) {
                logger.error("Failed to create async connection to datacenter: {} ({}:{})", 
                    endpoint.getId(), endpoint.getHost(), endpoint.getPort(), e);
                throw new ConnectionCreationException("Failed to create async connection to " + endpoint.getId(), e);
            }
        });
    }
    
    /**
     * Validates a connection to ensure it's still usable.
     * 
     * @param connection the connection to validate
     * @return future with validation result
     */
    public CompletableFuture<Boolean> validateConnection(StatefulRedisConnection<String, String> connection) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (!connection.isOpen()) {
                    return false;
                }
                
                // Perform ping validation with timeout
                CompletableFuture<String> pingFuture = connection.async().ping().toCompletableFuture();
                String result = pingFuture.get(config.getValidationTimeout().toMillis(), 
                    java.util.concurrent.TimeUnit.MILLISECONDS);
                
                return "PONG".equals(result);
                
            } catch (Exception e) {
                logger.debug("Connection validation failed", e);
                return false;
            }
        });
    }
    
    /**
     * Gets or creates a Redis client for the specified endpoint.
     */
    private RedisClient getOrCreateClient(DatacenterEndpoint endpoint) {
        return clients.computeIfAbsent(endpoint.getId(), id -> {
            RedisClient client = RedisClient.create(clientResources);
            client.setOptions(buildClientOptions(endpoint));
            
            logger.debug("Created Redis client for datacenter: {}", endpoint.getId());
            return client;
        });
    }
    
    /**
     * Builds the Redis URI for the endpoint.
     */
    private RedisURI buildRedisURI(DatacenterEndpoint endpoint) {
        RedisURI.Builder uriBuilder = RedisURI.builder()
            .withHost(endpoint.getHost())
            .withPort(endpoint.getPort())
            .withTimeout(config.getConnectionTimeout());
        
        // Add authentication if configured
        if (config.getPassword() != null) {
            uriBuilder.withPassword(config.getPassword().toCharArray());
        }
        
        if (config.getUsername() != null) {
            uriBuilder.withAuthentication(config.getUsername(), config.getPassword().toCharArray());
        }
        
        // Add database selection if configured
        if (config.getDatabase() != null) {
            uriBuilder.withDatabase(config.getDatabase());
        }
        
        // Configure SSL/TLS if enabled
        if (config.isSslEnabled()) {
            uriBuilder.withSsl(true);
            if (config.isVerifyPeer()) {
                uriBuilder.withVerifyPeer(true);
            }
        }
        
        return uriBuilder.build();
    }
    
    /**
     * Builds client options with performance and reliability settings.
     */
    private ClientOptions buildClientOptions(DatacenterEndpoint endpoint) {
        ClientOptions.Builder optionsBuilder = ClientOptions.builder()
            .autoReconnect(config.isAutoReconnect())
            .cancelCommandsOnReconnectFailure(config.isCancelCommandsOnReconnectFailure())
            .pingBeforeActivateConnection(config.isPingBeforeActivateConnection())
            .suspendReconnectOnProtocolFailure(config.isSuspendReconnectOnProtocolFailure());
        
        // Configure socket options
        SocketOptions socketOptions = SocketOptions.builder()
            .connectTimeout(config.getConnectionTimeout())
            .keepAlive(config.isKeepAlive())
            .tcpNoDelay(config.isTcpNoDelay())
            .build();
        optionsBuilder.socketOptions(socketOptions);
        
        // Configure timeout options
        TimeoutOptions timeoutOptions = TimeoutOptions.builder()
            .fixedTimeout(config.getCommandTimeout())
            .build();
        optionsBuilder.timeoutOptions(timeoutOptions);
        
        // Configure SSL options if enabled
        if (config.isSslEnabled()) {
            SslOptions.Builder sslBuilder = SslOptions.builder();
            
            // Note: Some SSL options may not be directly available in all Lettuce versions
            // This is a simplified SSL configuration
            
            if (config.getKeystore() != null) {
                sslBuilder.keystore(config.getKeystore(), config.getKeystorePassword() != null ? 
                    config.getKeystorePassword().toCharArray() : new char[0]);
            }
            
            if (config.getTruststore() != null) {
                if (config.getTruststorePassword() != null) {
                    sslBuilder.truststore(config.getTruststore(), config.getTruststorePassword());
                } else {
                    sslBuilder.truststore(config.getTruststore(), "");
                }
            }
            
            optionsBuilder.sslOptions(sslBuilder.build());
        }
        
        return optionsBuilder.build();
    }
    
    /**
     * Creates optimized client resources.
     */
    private ClientResources createClientResources() {
        DefaultClientResources.Builder resourcesBuilder = DefaultClientResources.builder()
            .ioThreadPoolSize(config.getIoThreadPoolSize())
            .computationThreadPoolSize(config.getComputationThreadPoolSize());
        
        // Configure DNS resolver settings
        if (config.getDnsResolverTimeout() != null) {
            // Note: This would require additional DNS resolver configuration
            // which is more complex in Lettuce. For now, we'll use defaults.
        }
        
        return resourcesBuilder.build();
    }
    
    /**
     * Gets the current configuration.
     * 
     * @return the connection factory configuration
     */
    public ConnectionFactoryConfig getConfig() {
        return config;
    }
    
    /**
     * Gets the number of active clients.
     * 
     * @return number of active Redis clients
     */
    public int getActiveClientCount() {
        return clients.size();
    }
    
    @Override
    public void close() {
        if (closed) {
            return;
        }
        
        logger.info("Closing ConnectionFactory with {} active clients", clients.size());
        closed = true;
        
        // Close all Redis clients
        clients.values().forEach(client -> {
            try {
                client.shutdown();
            } catch (Exception e) {
                logger.warn("Error closing Redis client", e);
            }
        });
        clients.clear();
        
        // Shutdown client resources
        try {
            clientResources.shutdown();
        } catch (Exception e) {
            logger.warn("Error shutting down client resources", e);
        }
        
        logger.info("ConnectionFactory closed");
    }
    
    /**
     * Exception thrown when connection creation fails.
     */
    public static class ConnectionCreationException extends RuntimeException {
        public ConnectionCreationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    /**
     * Configuration for the connection factory.
     */
    public static class ConnectionFactoryConfig {
        private final Duration connectionTimeout;
        private final Duration commandTimeout;
        private final Duration validationTimeout;
        private final String username;
        private final String password;
        private final Integer database;
        private final boolean autoReconnect;
        private final boolean cancelCommandsOnReconnectFailure;
        private final boolean pingBeforeActivateConnection;
        private final boolean suspendReconnectOnProtocolFailure;
        private final boolean keepAlive;
        private final boolean tcpNoDelay;
        private final boolean sslEnabled;
        private final boolean verifyPeer;
        private final io.netty.handler.ssl.SslProvider sslProvider;
        private final java.io.File keystore;
        private final String keystorePassword;
        private final java.io.File truststore;
        private final String truststorePassword;
        private final int ioThreadPoolSize;
        private final int computationThreadPoolSize;
        private final Duration dnsResolverTimeout;
        
        private ConnectionFactoryConfig(Builder builder) {
            this.connectionTimeout = builder.connectionTimeout;
            this.commandTimeout = builder.commandTimeout;
            this.validationTimeout = builder.validationTimeout;
            this.username = builder.username;
            this.password = builder.password;
            this.database = builder.database;
            this.autoReconnect = builder.autoReconnect;
            this.cancelCommandsOnReconnectFailure = builder.cancelCommandsOnReconnectFailure;
            this.pingBeforeActivateConnection = builder.pingBeforeActivateConnection;
            this.suspendReconnectOnProtocolFailure = builder.suspendReconnectOnProtocolFailure;
            this.keepAlive = builder.keepAlive;
            this.tcpNoDelay = builder.tcpNoDelay;
            this.sslEnabled = builder.sslEnabled;
            this.verifyPeer = builder.verifyPeer;
            this.sslProvider = builder.sslProvider;
            this.keystore = builder.keystore;
            this.keystorePassword = builder.keystorePassword;
            this.truststore = builder.truststore;
            this.truststorePassword = builder.truststorePassword;
            this.ioThreadPoolSize = builder.ioThreadPoolSize;
            this.computationThreadPoolSize = builder.computationThreadPoolSize;
            this.dnsResolverTimeout = builder.dnsResolverTimeout;
        }
        
        public static ConnectionFactoryConfig defaultConfig() {
            return builder().build();
        }
        
        public static Builder builder() {
            return new Builder();
        }
        
        // Getters
        public Duration getConnectionTimeout() { return connectionTimeout; }
        public Duration getCommandTimeout() { return commandTimeout; }
        public Duration getValidationTimeout() { return validationTimeout; }
        public String getUsername() { return username; }
        public String getPassword() { return password; }
        public Integer getDatabase() { return database; }
        public boolean isAutoReconnect() { return autoReconnect; }
        public boolean isCancelCommandsOnReconnectFailure() { return cancelCommandsOnReconnectFailure; }
        public boolean isPingBeforeActivateConnection() { return pingBeforeActivateConnection; }
        public boolean isSuspendReconnectOnProtocolFailure() { return suspendReconnectOnProtocolFailure; }
        public boolean isKeepAlive() { return keepAlive; }
        public boolean isTcpNoDelay() { return tcpNoDelay; }
        public boolean isSslEnabled() { return sslEnabled; }
        public boolean isVerifyPeer() { return verifyPeer; }
        public io.netty.handler.ssl.SslProvider getSslProvider() { return sslProvider; }
        public java.io.File getKeystore() { return keystore; }
        public String getKeystorePassword() { return keystorePassword; }
        public java.io.File getTruststore() { return truststore; }
        public String getTruststorePassword() { return truststorePassword; }
        public int getIoThreadPoolSize() { return ioThreadPoolSize; }
        public int getComputationThreadPoolSize() { return computationThreadPoolSize; }
        public Duration getDnsResolverTimeout() { return dnsResolverTimeout; }
        
        @Override
        public String toString() {
            return String.format("ConnectionFactoryConfig{connectionTimeout=%s, sslEnabled=%s, ioThreads=%d}", 
                connectionTimeout, sslEnabled, ioThreadPoolSize);
        }
        
        public static class Builder {
            private Duration connectionTimeout = Duration.ofSeconds(10);
            private Duration commandTimeout = Duration.ofSeconds(5);
            private Duration validationTimeout = Duration.ofSeconds(3);
            private String username;
            private String password;
            private Integer database;
            private boolean autoReconnect = true;
            private boolean cancelCommandsOnReconnectFailure = false;
            private boolean pingBeforeActivateConnection = true;
            private boolean suspendReconnectOnProtocolFailure = false;
            private boolean keepAlive = true;
            private boolean tcpNoDelay = true;
            private boolean sslEnabled = false;
            private boolean verifyPeer = true;
            private io.netty.handler.ssl.SslProvider sslProvider;
            private java.io.File keystore;
            private String keystorePassword;
            private java.io.File truststore;
            private String truststorePassword;
            private int ioThreadPoolSize = Runtime.getRuntime().availableProcessors();
            private int computationThreadPoolSize = Runtime.getRuntime().availableProcessors();
            private Duration dnsResolverTimeout;
            
            public Builder connectionTimeout(Duration connectionTimeout) {
                this.connectionTimeout = connectionTimeout;
                return this;
            }
            
            public Builder commandTimeout(Duration commandTimeout) {
                this.commandTimeout = commandTimeout;
                return this;
            }
            
            public Builder validationTimeout(Duration validationTimeout) {
                this.validationTimeout = validationTimeout;
                return this;
            }
            
            public Builder authentication(String username, String password) {
                this.username = username;
                this.password = password;
                return this;
            }
            
            public Builder database(int database) {
                this.database = database;
                return this;
            }
            
            public Builder autoReconnect(boolean autoReconnect) {
                this.autoReconnect = autoReconnect;
                return this;
            }
            
            public Builder ssl(boolean enabled, boolean verifyPeer) {
                this.sslEnabled = enabled;
                this.verifyPeer = verifyPeer;
                return this;
            }
            
            public Builder keystore(java.io.File keystore, String password) {
                this.keystore = keystore;
                this.keystorePassword = password;
                return this;
            }
            
            public Builder truststore(java.io.File truststore, String password) {
                this.truststore = truststore;
                this.truststorePassword = password;
                return this;
            }
            
            public Builder threadPoolSizes(int ioThreads, int computationThreads) {
                this.ioThreadPoolSize = ioThreads;
                this.computationThreadPoolSize = computationThreads;
                return this;
            }
            
            public ConnectionFactoryConfig build() {
                return new ConnectionFactoryConfig(this);
            }
        }
    }
}
