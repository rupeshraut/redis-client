package com.redis.multidc.routing;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.FallbackConfiguration;
import com.redis.multidc.config.FallbackStrategy;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.observability.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Implements fallback routing strategies when primary datacenter selection fails.
 * Provides resilient datacenter selection with configurable fallback behaviors.
 */
public class FallbackRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(FallbackRouter.class);
    
    private final DatacenterConfiguration configuration;
    private final DatacenterRouter primaryRouter;
    private final MetricsCollector metricsCollector;
    private final FallbackConfiguration fallbackConfig;
    private final BlockingQueue<FailedOperation> operationQueue;
    private final ScheduledExecutorService retryExecutor;
    
    public FallbackRouter(DatacenterConfiguration configuration, 
                         DatacenterRouter primaryRouter, 
                         MetricsCollector metricsCollector) {
        this.configuration = configuration;
        this.primaryRouter = primaryRouter;
        this.metricsCollector = metricsCollector;
        this.fallbackConfig = configuration.getFallbackConfiguration();
        this.operationQueue = new LinkedBlockingQueue<>(fallbackConfig.getQueueSize());
        this.retryExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "fallback-retry-" + Thread.currentThread().getId());
            t.setDaemon(true);
            return t;
        });
        
        // Start retry processor for queued operations
        if (fallbackConfig.getStrategy() == FallbackStrategy.QUEUE_AND_RETRY) {
            startRetryProcessor();
        }
    }
    
    /**
     * Attempts to select a datacenter using the primary router, falling back to
     * the configured fallback strategy if primary selection fails.
     */
    public Optional<String> selectDatacenterWithFallback(DatacenterPreference preference, boolean isRead) {
        Instant startTime = Instant.now();
        
        try {
            // Try primary routing first
            Optional<String> primaryResult = isRead 
                ? primaryRouter.selectDatacenterForRead(preference)
                : primaryRouter.selectDatacenterForWrite(preference);
                
            if (primaryResult.isPresent()) {
                metricsCollector.recordRoutingSuccess("primary", Duration.between(startTime, Instant.now()));
                return primaryResult;
            }
            
            // Primary routing failed, apply fallback strategy
            logger.debug("Primary routing failed for preference {}, applying fallback strategy: {}", 
                        preference, fallbackConfig.getStrategy());
            
            Optional<String> fallbackResult = applyFallbackStrategy(preference, isRead, startTime);
            
            if (fallbackResult.isPresent()) {
                metricsCollector.recordRoutingSuccess("fallback", Duration.between(startTime, Instant.now()));
            } else {
                metricsCollector.recordRoutingFailure("fallback", Duration.between(startTime, Instant.now()));
            }
            
            return fallbackResult;
            
        } catch (Exception e) {
            logger.error("Error during fallback routing", e);
            metricsCollector.recordRoutingFailure("error", Duration.between(startTime, Instant.now()));
            return Optional.empty();
        }
    }
    
    private Optional<String> applyFallbackStrategy(DatacenterPreference preference, boolean isRead, Instant startTime) {
        return switch (fallbackConfig.getStrategy()) {
            case FAIL_FAST -> {
                logger.warn("Fail-fast strategy enabled, operation will fail");
                yield Optional.empty();
            }
            case NEXT_AVAILABLE -> selectNextAvailable(preference, isRead);
            case TRY_ALL -> tryAllDatacenters(preference, isRead);
            case LOCAL_ONLY -> selectLocalFallback();
            case REMOTE_ONLY -> selectRemoteFallback();
            case BEST_EFFORT -> selectBestEffort(preference, isRead);
            case QUEUE_AND_RETRY -> queueForRetry(preference, isRead, startTime);
            case CUSTOM -> applyCustomFallbackLogic();
        };
    }
    
    private Optional<String> selectNextAvailable(DatacenterPreference preference, boolean isRead) {
        List<DatacenterInfo> availableDatacenters = getAvailableDatacenters();
        
        if (availableDatacenters.isEmpty()) {
            return Optional.empty();
        }
        
        // Sort by priority and latency
        availableDatacenters.sort((a, b) -> {
            // First by health status
            if (a.isHealthy() != b.isHealthy()) {
                return Boolean.compare(b.isHealthy(), a.isHealthy());
            }
            // Then by latency
            return Long.compare(a.getLatencyMs(), b.getLatencyMs());
        });
        
        return Optional.of(availableDatacenters.get(0).getId());
    }
    
    private Optional<String> tryAllDatacenters(DatacenterPreference preference, boolean isRead) {
        List<String> datacenterOrder = fallbackConfig.getFallbackDatacenterOrder();
        List<DatacenterInfo> availableDatacenters = getAvailableDatacenters();
        
        // If specific order is configured, use it; otherwise use priority order
        if (!datacenterOrder.isEmpty()) {
            for (String datacenterId : datacenterOrder) {
                if (availableDatacenters.stream().anyMatch(dc -> dc.getId().equals(datacenterId))) {
                    return Optional.of(datacenterId);
                }
            }
        }
        
        // Fallback to next available
        return selectNextAvailable(preference, isRead);
    }
    
    private Optional<String> selectLocalFallback() {
        return primaryRouter.getAllDatacenterInfo().stream()
            .filter(DatacenterInfo::isLocal)
            .filter(DatacenterInfo::isHealthy)
            .map(DatacenterInfo::getId)
            .findFirst();
    }
    
    private Optional<String> selectRemoteFallback() {
        return primaryRouter.getAllDatacenterInfo().stream()
            .filter(dc -> !dc.isLocal())
            .filter(DatacenterInfo::isHealthy)
            .min((a, b) -> Long.compare(a.getLatencyMs(), b.getLatencyMs()))
            .map(DatacenterInfo::getId);
    }
    
    private Optional<String> selectBestEffort(DatacenterPreference preference, boolean isRead) {
        // For best effort, prefer any healthy datacenter
        // If none are healthy, use the least unhealthy one
        List<DatacenterInfo> allDatacenters = primaryRouter.getAllDatacenterInfo();
        
        Optional<DatacenterInfo> healthyDc = allDatacenters.stream()
            .filter(DatacenterInfo::isHealthy)
            .min((a, b) -> Long.compare(a.getLatencyMs(), b.getLatencyMs()));
            
        if (healthyDc.isPresent()) {
            return Optional.of(healthyDc.get().getId());
        }
        
        // No healthy datacenters, try the most recently healthy one
        return allDatacenters.stream()
            .filter(DatacenterInfo::isAvailable) // Still reachable but not healthy
            .min((a, b) -> Long.compare(a.getLatencyMs(), b.getLatencyMs()))
            .map(DatacenterInfo::getId);
    }
    
    private Optional<String> queueForRetry(DatacenterPreference preference, boolean isRead, Instant startTime) {
        FailedOperation operation = new FailedOperation(preference, isRead, startTime);
        
        try {
            boolean queued = operationQueue.offer(operation, 
                fallbackConfig.getQueueTimeout().toMillis(), 
                TimeUnit.MILLISECONDS);
                
            if (queued) {
                logger.debug("Operation queued for retry: {}", operation);
                // For immediate response, try best effort
                return selectBestEffort(preference, isRead);
            } else {
                logger.warn("Failed to queue operation, queue is full");
                return selectBestEffort(preference, isRead);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while queueing operation", e);
            return selectBestEffort(preference, isRead);
        }
    }
    
    private Optional<String> applyCustomFallbackLogic() {
        if (fallbackConfig.getCustomFallbackLogic() != null) {
            try {
                String result = fallbackConfig.getCustomFallbackLogic().get();
                return Optional.ofNullable(result);
            } catch (Exception e) {
                logger.error("Custom fallback logic failed", e);
                return selectNextAvailable(DatacenterPreference.ANY_AVAILABLE, true);
            }
        }
        return Optional.empty();
    }
    
    private List<DatacenterInfo> getAvailableDatacenters() {
        return primaryRouter.getAllDatacenterInfo().stream()
            .filter(DatacenterInfo::isAvailable)
            .collect(Collectors.toList());
    }
    
    private void startRetryProcessor() {
        retryExecutor.scheduleWithFixedDelay(() -> {
            try {
                processQueuedOperations();
            } catch (Exception e) {
                logger.error("Error processing queued operations", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }
    
    private void processQueuedOperations() {
        List<FailedOperation> operations = new ArrayList<>();
        operationQueue.drainTo(operations, 100); // Process up to 100 operations at once
        
        for (FailedOperation operation : operations) {
            if (operation.isExpired(fallbackConfig.getFallbackTimeout())) {
                logger.debug("Operation expired: {}", operation);
                continue;
            }
            
            Optional<String> result = isRead(operation) 
                ? primaryRouter.selectDatacenterForRead(operation.preference())
                : primaryRouter.selectDatacenterForWrite(operation.preference());
                
            if (result.isPresent()) {
                logger.debug("Queued operation succeeded: {}", operation);
                metricsCollector.recordQueuedOperationSuccess();
            } else {
                // Requeue if not expired and under retry limit
                FailedOperation retryOperation = operation.incrementRetries();
                if (retryOperation.retryCount() < fallbackConfig.getMaxFallbackAttempts()) {
                    try {
                        operationQueue.offer(retryOperation, 100, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }
    
    private boolean isRead(FailedOperation operation) {
        return operation.isRead();
    }
    
    public void shutdown() {
        retryExecutor.shutdown();
        try {
            if (!retryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                retryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    private record FailedOperation(
        DatacenterPreference preference, 
        boolean isRead, 
        Instant timestamp,
        int retryCount
    ) {
        
        public FailedOperation(DatacenterPreference preference, boolean isRead, Instant timestamp) {
            this(preference, isRead, timestamp, 0);
        }
        
        public boolean isExpired(Duration timeout) {
            return Duration.between(timestamp, Instant.now()).compareTo(timeout) > 0;
        }
        
        public FailedOperation incrementRetries() {
            return new FailedOperation(preference, isRead, timestamp, retryCount + 1);
        }
    }
}
