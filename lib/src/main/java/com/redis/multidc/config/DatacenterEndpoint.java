package com.redis.multidc.config;

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
    private final int priority;
    private final double weight;
    private final boolean readOnly;
    
    private DatacenterEndpoint(Builder builder) {
        this.id = builder.id;
        this.region = builder.region;
        this.host = builder.host;
        this.port = builder.port;
        this.password = builder.password;
        this.database = builder.database;
        this.ssl = builder.ssl;
        this.priority = builder.priority;
        this.weight = builder.weight;
        this.readOnly = builder.readOnly;
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
    
    public int getPriority() {
        return priority;
    }
    
    public double getWeight() {
        return weight;
    }
    
    public boolean isReadOnly() {
        return readOnly;
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
        private int priority = 0;
        private double weight = 1.0;
        private boolean readOnly = false;
        
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
            return new DatacenterEndpoint(this);
        }
    }
    
    @Override
    public String toString() {
        return String.format("DatacenterEndpoint{id='%s', region='%s', host='%s', port=%d, ssl=%s, priority=%d, weight=%.1f, readOnly=%s}",
            id, region, host, port, ssl, priority, weight, readOnly);
    }
}
