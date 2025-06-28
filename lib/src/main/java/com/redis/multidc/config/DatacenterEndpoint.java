package com.redis.multidc.config;

import com.redis.multidc.pool.ConnectionPoolConfig;
import java.util.List;

/**
 * Datacenter endpoint configuration containing connection details
 * and metadata for a Redis datacenter.
 */
public class DatacenterEndpoint {
    
    private final String id;
    private final String region;
    private final String host;
    private final int port;
    private final String password;
    private final int database;
    private final boolean ssl;
    
    // SSL Certificate Configuration
    private final String truststorePath;
    private final String truststorePassword;
    private final String truststoreType;
    private final String keystorePath;
    private final String keystorePassword;
    private final String keystoreType;
    private final boolean verifyHostname;
    private final boolean verifyPeer;
    private final List<String> cipherSuites;
    private final List<String> protocols;
    private final String clientCertificatePath;
    private final String clientPrivateKeyPath;
    
    private final int priority;
    private final double weight;
    private final boolean readOnly;
    private final ConnectionPoolConfig poolConfig;
    
    private DatacenterEndpoint(Builder builder) {
        this.id = builder.id;
        this.region = builder.region;
        this.host = builder.host;
        this.port = builder.port;
        this.password = builder.password;
        this.database = builder.database;
        this.ssl = builder.ssl;
        
        // SSL Certificate Configuration
        this.truststorePath = builder.truststorePath;
        this.truststorePassword = builder.truststorePassword;
        this.truststoreType = builder.truststoreType;
        this.keystorePath = builder.keystorePath;
        this.keystorePassword = builder.keystorePassword;
        this.keystoreType = builder.keystoreType;
        this.verifyHostname = builder.verifyHostname;
        this.verifyPeer = builder.verifyPeer;
        this.cipherSuites = builder.cipherSuites != null ? List.copyOf(builder.cipherSuites) : List.of();
        this.protocols = builder.protocols != null ? List.copyOf(builder.protocols) : List.of();
        this.clientCertificatePath = builder.clientCertificatePath;
        this.clientPrivateKeyPath = builder.clientPrivateKeyPath;
        
        this.priority = builder.priority;
        this.weight = builder.weight;
        this.readOnly = builder.readOnly;
        this.poolConfig = builder.poolConfig != null ? builder.poolConfig : ConnectionPoolConfig.defaultConfig();
    }
    
    public String getId() {
        return id;
    }
    
    public String getRegion() {
        return region;
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public String getPassword() {
        return password;
    }
    
    public int getDatabase() {
        return database;
    }
    
    public boolean isSsl() {
        return ssl;
    }
    
    // SSL Certificate Configuration getters
    
    public String getTruststorePath() {
        return truststorePath;
    }
    
    public String getTruststorePassword() {
        return truststorePassword;
    }
    
    public String getTruststoreType() {
        return truststoreType;
    }
    
    public String getKeystorePath() {
        return keystorePath;
    }
    
    public String getKeystorePassword() {
        return keystorePassword;
    }
    
    public String getKeystoreType() {
        return keystoreType;
    }
    
    public boolean isVerifyHostname() {
        return verifyHostname;
    }
    
    public boolean isVerifyPeer() {
        return verifyPeer;
    }
    
    public List<String> getCipherSuites() {
        return cipherSuites;
    }
    
    public List<String> getProtocols() {
        return protocols;
    }
    
    public String getClientCertificatePath() {
        return clientCertificatePath;
    }
    
    public String getClientPrivateKeyPath() {
        return clientPrivateKeyPath;
    }
    
    public int getPriority() {
        return priority;
    }
    
    public double getWeight() {
        return weight;
    }
    
    public boolean isReadOnly() {
        return readOnly;
    }
    
    public ConnectionPoolConfig getPoolConfig() {
        return poolConfig;
    }
    
    /**
     * Returns true if this endpoint has SSL certificate configuration.
     */
    public boolean hasSslCertificateConfig() {
        return ssl && (truststorePath != null || keystorePath != null || 
                      clientCertificatePath != null || !cipherSuites.isEmpty());
    }
    
    /**
     * Returns true if mutual TLS (mTLS) is configured.
     */
    public boolean isMutualTlsEnabled() {
        return ssl && clientCertificatePath != null && clientPrivateKeyPath != null;
    }

    public String getConnectionString() {
        StringBuilder sb = new StringBuilder();
        if (ssl) {
            sb.append("rediss://");
        } else {
            sb.append("redis://");
        }
        if (password != null && !password.isEmpty()) {
            sb.append(":").append(password).append("@");
        }
        sb.append(host).append(":").append(port);
        if (database > 0) {
            sb.append("/").append(database);
        }
        return sb.toString();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String id;
        private String region;
        private String host;
        private int port = 6379;
        private String password;
        private int database = 0;
        private boolean ssl = false;
        
        // SSL Certificate Configuration
        private String truststorePath;
        private String truststorePassword;
        private String truststoreType;
        private String keystorePath;
        private String keystorePassword;
        private String keystoreType;
        private boolean verifyHostname = false;
        private boolean verifyPeer = false;
        private List<String> cipherSuites;
        private List<String> protocols;
        private String clientCertificatePath;
        private String clientPrivateKeyPath;
        
        private int priority = 0;
        private double weight = 1.0;
        private boolean readOnly = false;
        private ConnectionPoolConfig poolConfig;
        
        public Builder id(String id) {
            this.id = id;
            return this;
        }
        
        public Builder region(String region) {
            this.region = region;
            return this;
        }
        
        public Builder host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder password(String password) {
            this.password = password;
            return this;
        }
        
        public Builder database(int database) {
            this.database = database;
            return this;
        }
        
        public Builder ssl(boolean ssl) {
            this.ssl = ssl;
            return this;
        }
        

        
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }
        
        public Builder weight(double weight) {
            this.weight = weight;
            return this;
        }
        
        public Builder readOnly(boolean readOnly) {
            this.readOnly = readOnly;
            return this;
        }
        
        /**
         * Sets the connection pool configuration for this datacenter.
         * 
         * @param poolConfig connection pool configuration
         * @return this builder
         */
        public Builder poolConfig(ConnectionPoolConfig poolConfig) {
            this.poolConfig = poolConfig;
            return this;
        }
        
        /**
         * Convenience method to set basic connection pool size.
         * 
         * @param poolSize maximum number of connections in the pool
         * @return this builder
         */
        public Builder connectionPoolSize(int poolSize) {
            this.poolConfig = ConnectionPoolConfig.builder()
                .maxPoolSize(poolSize)
                .minPoolSize(Math.max(1, poolSize / 4))
                .build();
            return this;
        }
        
        /**
         * Convenience method to configure connection pool for high throughput.
         * 
         * @return this builder with high-throughput pool configuration
         */
        public Builder highThroughputPool() {
            this.poolConfig = ConnectionPoolConfig.builder()
                .highThroughput()
                .build();
            return this;
        }
        
        /**
         * Convenience method to configure connection pool for low latency.
         * 
         * @return this builder with low-latency pool configuration
         */
        public Builder lowLatencyPool() {
            this.poolConfig = ConnectionPoolConfig.builder()
                .lowLatency()
                .build();
            return this;
        }
        
        /**
         * Convenience method to configure connection pool for resource-constrained environments.
         * 
         * @return this builder with resource-constrained pool configuration
         */
        public Builder resourceConstrainedPool() {
            this.poolConfig = ConnectionPoolConfig.builder()
                .resourceConstrained()
                .build();
            return this;
        }
        
        // SSL Certificate Configuration Methods
        
        /**
         * Sets the path to the truststore file containing trusted CA certificates.
         * 
         * @param truststorePath path to the truststore file
         * @return this builder
         */
        public Builder truststorePath(String truststorePath) {
            this.truststorePath = truststorePath;
            return this;
        }
        
        /**
         * Sets the password for the truststore.
         * 
         * @param truststorePassword password for the truststore
         * @return this builder
         */
        public Builder truststorePassword(String truststorePassword) {
            this.truststorePassword = truststorePassword;
            return this;
        }
        
        /**
         * Sets the truststore type (e.g., "JKS", "PKCS12").
         * 
         * @param truststoreType type of the truststore (default: "JKS")
         * @return this builder
         */
        public Builder truststoreType(String truststoreType) {
            this.truststoreType = truststoreType;
            return this;
        }
        
        /**
         * Sets the path to the keystore file containing client certificates.
         * 
         * @param keystorePath path to the keystore file
         * @return this builder
         */
        public Builder keystorePath(String keystorePath) {
            this.keystorePath = keystorePath;
            return this;
        }
        
        /**
         * Sets the password for the keystore.
         * 
         * @param keystorePassword password for the keystore
         * @return this builder
         */
        public Builder keystorePassword(String keystorePassword) {
            this.keystorePassword = keystorePassword;
            return this;
        }
        
        /**
         * Sets the keystore type (e.g., "JKS", "PKCS12").
         * 
         * @param keystoreType type of the keystore (default: "JKS")
         * @return this builder
         */
        public Builder keystoreType(String keystoreType) {
            this.keystoreType = keystoreType;
            return this;
        }
        
        /**
         * Sets whether to verify the server's hostname against its certificate.
         * 
         * @param verifyHostname true to verify hostname (recommended for security)
         * @return this builder
         */
        public Builder verifyHostname(boolean verifyHostname) {
            this.verifyHostname = verifyHostname;
            return this;
        }
        
        /**
         * Sets whether to verify the peer's certificate.
         * 
         * @param verifyPeer true to verify peer certificate (recommended for security)
         * @return this builder
         */
        public Builder verifyPeer(boolean verifyPeer) {
            this.verifyPeer = verifyPeer;
            return this;
        }
        
        /**
         * Sets the allowed cipher suites for SSL connections.
         * 
         * @param cipherSuites list of allowed cipher suites
         * @return this builder
         */
        public Builder cipherSuites(List<String> cipherSuites) {
            this.cipherSuites = cipherSuites;
            return this;
        }
        
        /**
         * Sets the allowed TLS protocols.
         * 
         * @param protocols list of allowed protocols (e.g., "TLSv1.3", "TLSv1.2")
         * @return this builder
         */
        public Builder protocols(List<String> protocols) {
            this.protocols = protocols;
            return this;
        }
        
        /**
         * Sets the path to the client certificate file for mutual TLS.
         * 
         * @param clientCertificatePath path to the client certificate file
         * @return this builder
         */
        public Builder clientCertificatePath(String clientCertificatePath) {
            this.clientCertificatePath = clientCertificatePath;
            return this;
        }
        
        /**
         * Sets the path to the client private key file for mutual TLS.
         * 
         * @param clientPrivateKeyPath path to the client private key file
         * @return this builder
         */
        public Builder clientPrivateKeyPath(String clientPrivateKeyPath) {
            this.clientPrivateKeyPath = clientPrivateKeyPath;
            return this;
        }
        
        /**
         * Convenience method to configure basic SSL with hostname verification.
         * 
         * @return this builder with SSL enabled and basic security settings
         */
        public Builder enableSslWithDefaults() {
            this.ssl = true;
            this.verifyHostname = true;
            this.verifyPeer = true;
            this.truststoreType = "JKS";
            this.keystoreType = "JKS";
            return this;
        }
        
        /**
         * Convenience method to configure mutual TLS (mTLS).
         * 
         * @param clientCertPath path to client certificate
         * @param clientKeyPath path to client private key
         * @param truststorePath path to truststore with CA certificates
         * @param truststorePassword password for the truststore
         * @return this builder configured for mTLS
         */
        public Builder enableMutualTls(String clientCertPath, String clientKeyPath, String truststorePath, String truststorePassword) {
            this.ssl = true;
            this.verifyHostname = true;
            this.verifyPeer = true;
            this.clientCertificatePath = clientCertPath;
            this.clientPrivateKeyPath = clientKeyPath;
            this.truststorePath = truststorePath;
            this.truststorePassword = truststorePassword;
            return this;
        }
        
        /**
         * Convenience method to configure mutual TLS (mTLS) without password.
         * Use this when the truststore doesn't require a password or when
         * using PEM files instead of JKS.
         * 
         * @param clientCertPath path to client certificate
         * @param clientKeyPath path to client private key
         * @return this builder configured for mTLS
         */
        public Builder enableMutualTlsWithoutPassword(String clientCertPath, String clientKeyPath) {
            this.ssl = true;
            this.verifyHostname = true;
            this.verifyPeer = true;
            this.clientCertificatePath = clientCertPath;
            this.clientPrivateKeyPath = clientKeyPath;
            return this;
        }

        public DatacenterEndpoint build() {
            if (id == null || id.trim().isEmpty()) {
                throw new IllegalArgumentException("Datacenter ID is required");
            }
            if (host == null || host.trim().isEmpty()) {
                throw new IllegalArgumentException("Host is required");
            }
            if (port <= 0 || port > 65535) {
                throw new IllegalArgumentException("Port must be between 1 and 65535");
            }
            
            // SSL Certificate validation
            if (ssl) {
                // Validate mutual TLS configuration
                if ((clientCertificatePath != null && clientPrivateKeyPath == null) ||
                    (clientCertificatePath == null && clientPrivateKeyPath != null)) {
                    throw new IllegalArgumentException(
                        "Both client certificate path and client private key path must be provided for mutual TLS");
                }
                
                // Validate truststore configuration
                if (truststorePath != null && truststorePassword == null) {
                    throw new IllegalArgumentException(
                        "Truststore password is required when truststore path is provided");
                }
                
                // Validate keystore configuration
                if (keystorePath != null && keystorePassword == null) {
                    throw new IllegalArgumentException(
                        "Keystore password is required when keystore path is provided");
                }
                
                // Validate protocol list
                if (protocols != null && !protocols.isEmpty()) {
                    for (String protocol : protocols) {
                        if (protocol == null || protocol.trim().isEmpty()) {
                            throw new IllegalArgumentException("Protocol cannot be null or empty");
                        }
                    }
                }
                
                // Validate cipher suites
                if (cipherSuites != null && !cipherSuites.isEmpty()) {
                    for (String cipher : cipherSuites) {
                        if (cipher == null || cipher.trim().isEmpty()) {
                            throw new IllegalArgumentException("Cipher suite cannot be null or empty");
                        }
                    }
                }
            }
            
            return new DatacenterEndpoint(this);
        }
    }
    
    @Override
    public String toString() {
        return String.format("DatacenterEndpoint{id='%s', region='%s', host='%s', port=%d, ssl=%s, priority=%d, weight=%.1f, readOnly=%s, poolConfig=%s}",
            id, region, host, port, ssl, priority, weight, readOnly, poolConfig);
    }
}
