package com.redis.multidc.pool;

/**
 * Exception thrown when connection pool operations fail.
 */
public class ConnectionPoolException extends RuntimeException {
    
    public ConnectionPoolException(String message) {
        super(message);
    }
    
    public ConnectionPoolException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Exception thrown when unable to acquire a connection within the specified timeout.
     */
    public static class AcquisitionTimeoutException extends ConnectionPoolException {
        public AcquisitionTimeoutException(String datacenterId, long timeoutMs) {
            super(String.format("Failed to acquire connection for datacenter '%s' within %dms", 
                               datacenterId, timeoutMs));
        }
    }
    
    /**
     * Exception thrown when the connection pool is exhausted.
     */
    public static class PoolExhaustedException extends ConnectionPoolException {
        public PoolExhaustedException(String datacenterId, int maxSize) {
            super(String.format("Connection pool exhausted for datacenter '%s' (max size: %d)", 
                               datacenterId, maxSize));
        }
    }
    
    /**
     * Exception thrown when attempting to use a closed pool.
     */
    public static class PoolClosedException extends ConnectionPoolException {
        public PoolClosedException(String datacenterId) {
            super(String.format("Connection pool for datacenter '%s' is closed", datacenterId));
        }
    }
    
    /**
     * Exception thrown when connection validation fails.
     */
    public static class ConnectionValidationException extends ConnectionPoolException {
        public ConnectionValidationException(String datacenterId, Throwable cause) {
            super(String.format("Connection validation failed for datacenter '%s'", datacenterId), cause);
        }
    }
}
