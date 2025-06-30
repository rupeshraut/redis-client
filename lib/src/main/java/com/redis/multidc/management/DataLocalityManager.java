package com.redis.multidc.management;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.routing.DatacenterRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Intelligent data placement and access optimization for multi-datacenter Redis deployments.
 * Manages data locality, access patterns, and automatic migration based on usage analytics.
 */
public class DataLocalityManager implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(DataLocalityManager.class);
    
    private static final String LOCALITY_PREFIX = "__locality:";
    private static final String ACCESS_PATTERN_PREFIX = "__access:";
    private static final String MIGRATION_PREFIX = "__migration:";
    
    private final MultiDatacenterRedisClient client;
    private final DatacenterRouter router;
    private final DatacenterConfiguration datacenterConfig;
    private final DataLocalityConfig config;
    private final ScheduledExecutorService analyticsExecutor;
    private final Map<String, AccessPattern> accessPatterns;
    private final Map<String, DataPlacement> dataPlacements;
    
    public DataLocalityManager(MultiDatacenterRedisClient client, 
                             DatacenterRouter router,
                             DatacenterConfiguration datacenterConfig,
                             DataLocalityConfig config) {
        this.client = client;
        this.router = router;
        this.datacenterConfig = datacenterConfig;
        this.config = config;
        this.analyticsExecutor = Executors.newScheduledThreadPool(2);
        this.accessPatterns = new ConcurrentHashMap<>();
        this.dataPlacements = new ConcurrentHashMap<>();
        
        // Start periodic analytics and optimization
        analyticsExecutor.scheduleAtFixedRate(
            this::analyzeAccessPatterns,
            config.getAnalysisInterval().toMinutes(),
            config.getAnalysisInterval().toMinutes(),
            TimeUnit.MINUTES
        );
        
        analyticsExecutor.scheduleAtFixedRate(
            this::optimizeDataPlacement,
            config.getOptimizationInterval().toMinutes(),
            config.getOptimizationInterval().toMinutes(),
            TimeUnit.MINUTES
        );
        
        logger.info("DataLocalityManager initialized with analysis interval: {}, optimization interval: {}", 
            config.getAnalysisInterval(), config.getOptimizationInterval());
    }
    
    /**
     * Records a data access for locality analysis.
     * 
     * @param key the accessed key
     * @param datacenterId the datacenter where the access occurred
     * @param operation the type of operation (GET, SET, etc.)
     * @param latency the operation latency in milliseconds
     */
    public void recordAccess(String key, String datacenterId, String operation, long latency) {
        AccessPattern pattern = accessPatterns.computeIfAbsent(key, k -> new AccessPattern(k));
        pattern.recordAccess(datacenterId, operation, latency);
        
        logger.trace("Recorded access: key={}, datacenter={}, operation={}, latency={}ms", 
            key, datacenterId, operation, latency);
    }
    
    /**
     * Gets the optimal datacenter for reading a specific key based on access patterns.
     * 
     * @param key the key to analyze
     * @return future with optimal datacenter for reads
     */
    public CompletableFuture<DatacenterInfo> getOptimalReadDatacenter(String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                AccessPattern pattern = accessPatterns.get(key);
                if (pattern == null) {
                    // No access pattern data, use router's default logic
                    return router.selectDatacenter(DatacenterPreference.LOCAL_PREFERRED, false);
                }
                
                // Find datacenter with lowest average read latency
                String optimalDatacenterId = pattern.getOptimalReadDatacenter();
                if (optimalDatacenterId != null) {
                    return datacenterConfig.getDatacenters().stream()
                        .filter(dc -> dc.getId().equals(optimalDatacenterId))
                        .map(dc -> createDatacenterInfo(dc))
                        .findFirst()
                        .orElse(router.selectDatacenter(DatacenterPreference.LOCAL_PREFERRED, false));
                }
                
                return router.selectDatacenter(DatacenterPreference.LOCAL_PREFERRED, false);
                
            } catch (Exception e) {
                logger.error("Error determining optimal read datacenter for key: {}", key, e);
                return router.selectDatacenter(DatacenterPreference.LOCAL_PREFERRED);
            }
        });
    }
    
    /**
     * Gets the optimal datacenter for writing a specific key.
     * 
     * @param key the key to analyze
     * @return future with optimal datacenter for writes
     */
    public CompletableFuture<DatacenterInfo> getOptimalWriteDatacenter(String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                AccessPattern pattern = accessPatterns.get(key);
                if (pattern == null) {
                    return router.selectDatacenter(DatacenterPreference.LOCAL_PREFERRED, false);
                }
                
                // Find datacenter with most write activity
                String optimalDatacenterId = pattern.getOptimalWriteDatacenter();
                if (optimalDatacenterId != null) {
                    return datacenterConfig.getDatacenters().stream()
                        .filter(dc -> dc.getId().equals(optimalDatacenterId))
                        .map(dc -> createDatacenterInfo(dc))
                        .findFirst()
                        .orElse(router.selectDatacenter(DatacenterPreference.LOCAL_PREFERRED, false));
                }
                
                return router.selectDatacenter(DatacenterPreference.LOCAL_PREFERRED, false);
                
            } catch (Exception e) {
                logger.error("Error determining optimal write datacenter for key: {}", key, e);
                return router.selectDatacenter(DatacenterPreference.LOCAL_PREFERRED);
            }
        });
    }
    
    /**
     * Suggests data migration based on access patterns.
     * 
     * @param key the key to analyze for migration
     * @return future with migration recommendation
     */
    public CompletableFuture<MigrationRecommendation> analyzeMigrationOpportunity(String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                AccessPattern pattern = accessPatterns.get(key);
                if (pattern == null || pattern.getTotalAccesses() < config.getMinAccessesForMigration()) {
                    return new MigrationRecommendation(key, false, null, null, "Insufficient access data");
                }
                
                // Analyze if migration would be beneficial
                String currentPrimaryDatacenter = getCurrentPrimaryDatacenter(key);
                String optimalDatacenter = pattern.getOptimalReadDatacenter();
                
                if (currentPrimaryDatacenter != null && optimalDatacenter != null && 
                    !currentPrimaryDatacenter.equals(optimalDatacenter)) {
                    
                    double currentLatency = pattern.getAverageLatency(currentPrimaryDatacenter);
                    double optimalLatency = pattern.getAverageLatency(optimalDatacenter);
                    
                    if (currentLatency - optimalLatency > config.getMigrationLatencyThreshold().toMillis()) {
                        return new MigrationRecommendation(
                            key, 
                            true, 
                            currentPrimaryDatacenter, 
                            optimalDatacenter,
                            String.format("Potential latency improvement: %.2fms -> %.2fms", 
                                currentLatency, optimalLatency)
                        );
                    }
                }
                
                return new MigrationRecommendation(key, false, currentPrimaryDatacenter, 
                    optimalDatacenter, "No significant improvement expected");
                
            } catch (Exception e) {
                logger.error("Error analyzing migration opportunity for key: {}", key, e);
                return new MigrationRecommendation(key, false, null, null, "Analysis error: " + e.getMessage());
            }
        });
    }
    
    /**
     * Performs data migration from one datacenter to another.
     * 
     * @param key the key to migrate
     * @param fromDatacenter source datacenter
     * @param toDatacenter target datacenter
     * @return future indicating migration success
     */
    public CompletableFuture<Boolean> migrateData(String key, String fromDatacenter, String toDatacenter) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                logger.info("Starting data migration: key={}, from={}, to={}", key, fromDatacenter, toDatacenter);
                
                // Get data from source datacenter
                String value = client.sync().get(key, DatacenterPreference.SPECIFIC_DATACENTER);
                if (value == null) {
                    logger.warn("Key {} not found in source datacenter {}", key, fromDatacenter);
                    return false;
                }
                
                // Get TTL from source (simplified - returns seconds as long)
                Long ttlSeconds = client.sync().ttl(key, DatacenterPreference.SPECIFIC_DATACENTER);
                
                // Set data in target datacenter
                client.sync().set(key, value, DatacenterPreference.SPECIFIC_DATACENTER);
                if (ttlSeconds != null && ttlSeconds > 0) {
                    client.sync().expire(key, Duration.ofSeconds(ttlSeconds), DatacenterPreference.SPECIFIC_DATACENTER);
                }
                
                // Update data placement tracking
                DataPlacement placement = new DataPlacement(key, toDatacenter, fromDatacenter, Instant.now());
                dataPlacements.put(key, placement);
                
                // Store migration metadata (simplified - no ALL_DATACENTERS enum)
                String migrationKey = MIGRATION_PREFIX + key;
                client.sync().set(migrationKey, placement.toJson(), DatacenterPreference.LOCAL_PREFERRED);
                client.sync().expire(migrationKey, Duration.ofDays(7), DatacenterPreference.LOCAL_PREFERRED);
                
                logger.info("Data migration completed: key={}, from={}, to={}", key, fromDatacenter, toDatacenter);
                return true;
                
            } catch (Exception e) {
                logger.error("Data migration failed: key={}, from={}, to={}", key, fromDatacenter, toDatacenter, e);
                return false;
            }
        });
    }
    
    /**
     * Gets data locality statistics for monitoring.
     * 
     * @return locality statistics
     */
    public LocalityStats getLocalityStats() {
        int totalKeys = accessPatterns.size();
        int optimizedKeys = 0;
        int migrationCandidates = 0;
        double averageLatency = 0.0;
        
        for (AccessPattern pattern : accessPatterns.values()) {
            if (pattern.isOptimized()) {
                optimizedKeys++;
            }
            if (pattern.isMigrationCandidate(config.getMigrationLatencyThreshold())) {
                migrationCandidates++;
            }
            averageLatency += pattern.getOverallAverageLatency();
        }
        
        if (totalKeys > 0) {
            averageLatency /= totalKeys;
        }
        
        return new LocalityStats(
            totalKeys,
            optimizedKeys,
            migrationCandidates,
            dataPlacements.size(),
            averageLatency
        );
    }
    
    /**
     * Analyzes access patterns and updates optimization recommendations.
     */
    private void analyzeAccessPatterns() {
        logger.debug("Starting access pattern analysis");
        
        try {
            int analyzedKeys = 0;
            for (Map.Entry<String, AccessPattern> entry : accessPatterns.entrySet()) {
                String key = entry.getKey();
                AccessPattern pattern = entry.getValue();
                
                if (pattern.shouldAnalyze(config.getMinAccessesForAnalysis())) {
                    pattern.analyze();
                    analyzedKeys++;
                }
            }
            
            logger.debug("Access pattern analysis completed, analyzed {} keys", analyzedKeys);
            
        } catch (Exception e) {
            logger.error("Error during access pattern analysis", e);
        }
    }
    
    /**
     * Performs automatic data placement optimization.
     */
    private void optimizeDataPlacement() {
        logger.debug("Starting data placement optimization");
        
        try {
            int optimizationCandidates = 0;
            
            for (AccessPattern pattern : accessPatterns.values()) {
                if (config.isAutoMigrationEnabled() && 
                    pattern.isMigrationCandidate(config.getMigrationLatencyThreshold())) {
                    
                    String key = pattern.getKey();
                    String currentDatacenter = getCurrentPrimaryDatacenter(key);
                    String optimalDatacenter = pattern.getOptimalReadDatacenter();
                    
                    if (currentDatacenter != null && optimalDatacenter != null && 
                        !currentDatacenter.equals(optimalDatacenter)) {
                        
                        // Schedule migration
                        migrateData(key, currentDatacenter, optimalDatacenter);
                        optimizationCandidates++;
                    }
                }
            }
            
            logger.debug("Data placement optimization completed, {} migrations scheduled", optimizationCandidates);
            
        } catch (Exception e) {
            logger.error("Error during data placement optimization", e);
        }
    }
    
    /**
     * Gets the current primary datacenter for a key.
     */
    private String getCurrentPrimaryDatacenter(String key) {
        DataPlacement placement = dataPlacements.get(key);
        if (placement != null) {
            return placement.getPrimaryDatacenter();
        }
        
        // Default to first datacenter as local
        return datacenterConfig.getDatacenters().isEmpty() ? 
            "default" : datacenterConfig.getDatacenters().get(0).getId();
    }
    
    /**
     * Creates a DatacenterInfo from DatacenterEndpoint with default values.
     */
    private DatacenterInfo createDatacenterInfo(DatacenterEndpoint endpoint) {
        return new DatacenterInfo(
            endpoint.getId(),
            endpoint.getRegion(),
            endpoint.getHost(),
            endpoint.getPort(),
            true, // healthy
            0L,   // latency
            Instant.now(), // lastHealthCheck
            0,    // errorCount
            0L,   // totalRequests
            0L,   // totalErrors
            io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED, // circuitBreakerState
            true  // available
        );
    }
    
    @Override
    public void close() {
        logger.info("Closing DataLocalityManager");
        
        analyticsExecutor.shutdown();
        try {
            if (!analyticsExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                analyticsExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            analyticsExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        accessPatterns.clear();
        dataPlacements.clear();
        logger.info("DataLocalityManager closed");
    }
    
    /**
     * Tracks access patterns for a specific key.
     */
    private static class AccessPattern {
        private final String key;
        private final Map<String, DatacenterAccess> datacenterAccesses;
        private volatile boolean analyzed = false;
        
        public AccessPattern(String key) {
            this.key = key;
            this.datacenterAccesses = new ConcurrentHashMap<>();
        }
        
        public void recordAccess(String datacenterId, String operation, long latency) {
            DatacenterAccess access = datacenterAccesses.computeIfAbsent(datacenterId, 
                k -> new DatacenterAccess(k));
            access.recordAccess(operation, latency);
        }
        
        public String getOptimalReadDatacenter() {
            return datacenterAccesses.entrySet().stream()
                .filter(entry -> entry.getValue().getReadCount() > 0)
                .min(Comparator.comparingDouble(entry -> entry.getValue().getAverageReadLatency()))
                .map(Map.Entry::getKey)
                .orElse(null);
        }
        
        public String getOptimalWriteDatacenter() {
            return datacenterAccesses.entrySet().stream()
                .max(Comparator.comparingInt(entry -> entry.getValue().getWriteCount()))
                .map(Map.Entry::getKey)
                .orElse(null);
        }
        
        public double getAverageLatency(String datacenterId) {
            DatacenterAccess access = datacenterAccesses.get(datacenterId);
            return access != null ? access.getAverageLatency() : Double.MAX_VALUE;
        }
        
        public double getOverallAverageLatency() {
            return datacenterAccesses.values().stream()
                .mapToDouble(DatacenterAccess::getAverageLatency)
                .average()
                .orElse(0.0);
        }
        
        public int getTotalAccesses() {
            return datacenterAccesses.values().stream()
                .mapToInt(DatacenterAccess::getTotalAccesses)
                .sum();
        }
        
        public boolean shouldAnalyze(int minAccesses) {
            return getTotalAccesses() >= minAccesses;
        }
        
        public boolean isOptimized() {
            return analyzed;
        }
        
        public boolean isMigrationCandidate(Duration latencyThreshold) {
            if (datacenterAccesses.size() < 2) return false;
            
            double minLatency = datacenterAccesses.values().stream()
                .mapToDouble(DatacenterAccess::getAverageLatency)
                .min()
                .orElse(0.0);
            
            double maxLatency = datacenterAccesses.values().stream()
                .mapToDouble(DatacenterAccess::getAverageLatency)
                .max()
                .orElse(0.0);
            
            return (maxLatency - minLatency) > latencyThreshold.toMillis();
        }
        
        public void analyze() {
            this.analyzed = true;
        }
        
        public String getKey() {
            return key;
        }
    }
    
    /**
     * Tracks access statistics for a specific datacenter.
     */
    private static class DatacenterAccess {
        private final String datacenterId;
        private int readCount = 0;
        private int writeCount = 0;
        private long totalLatency = 0;
        private int totalAccesses = 0;
        
        public DatacenterAccess(String datacenterId) {
            this.datacenterId = datacenterId;
        }
        
        public synchronized void recordAccess(String operation, long latency) {
            if ("GET".equals(operation) || "MGET".equals(operation)) {
                readCount++;
            } else if ("SET".equals(operation) || "MSET".equals(operation)) {
                writeCount++;
            }
            
            totalLatency += latency;
            totalAccesses++;
        }
        
        public double getAverageReadLatency() {
            return readCount > 0 ? (double) totalLatency / readCount : Double.MAX_VALUE;
        }
        
        public double getAverageLatency() {
            return totalAccesses > 0 ? (double) totalLatency / totalAccesses : 0.0;
        }
        
        public int getReadCount() {
            return readCount;
        }
        
        public int getWriteCount() {
            return writeCount;
        }
        
        public int getTotalAccesses() {
            return totalAccesses;
        }
    }
    
    /**
     * Represents data placement information.
     */
    public static class DataPlacement {
        private final String key;
        private final String primaryDatacenter;
        private final String previousDatacenter;
        private final Instant migrationTime;
        
        public DataPlacement(String key, String primaryDatacenter, String previousDatacenter, Instant migrationTime) {
            this.key = key;
            this.primaryDatacenter = primaryDatacenter;
            this.previousDatacenter = previousDatacenter;
            this.migrationTime = migrationTime;
        }
        
        public String getKey() {
            return key;
        }
        
        public String getPrimaryDatacenter() {
            return primaryDatacenter;
        }
        
        public String getPreviousDatacenter() {
            return previousDatacenter;
        }
        
        public Instant getMigrationTime() {
            return migrationTime;
        }
        
        public String toJson() {
            return String.format("{\"key\":\"%s\",\"primary\":\"%s\",\"previous\":\"%s\",\"migrationTime\":\"%s\"}", 
                key, primaryDatacenter, previousDatacenter, migrationTime);
        }
    }
    
    /**
     * Migration recommendation result.
     */
    public static class MigrationRecommendation {
        private final String key;
        private final boolean recommended;
        private final String currentDatacenter;
        private final String targetDatacenter;
        private final String reason;
        
        public MigrationRecommendation(String key, boolean recommended, String currentDatacenter, 
                                     String targetDatacenter, String reason) {
            this.key = key;
            this.recommended = recommended;
            this.currentDatacenter = currentDatacenter;
            this.targetDatacenter = targetDatacenter;
            this.reason = reason;
        }
        
        public String getKey() {
            return key;
        }
        
        public boolean isRecommended() {
            return recommended;
        }
        
        public String getCurrentDatacenter() {
            return currentDatacenter;
        }
        
        public String getTargetDatacenter() {
            return targetDatacenter;
        }
        
        public String getReason() {
            return reason;
        }
        
        @Override
        public String toString() {
            return String.format("MigrationRecommendation{key='%s', recommended=%s, %s -> %s, reason='%s'}", 
                key, recommended, currentDatacenter, targetDatacenter, reason);
        }
    }
    
    /**
     * Data locality statistics.
     */
    public static class LocalityStats {
        private final int totalKeys;
        private final int optimizedKeys;
        private final int migrationCandidates;
        private final int migratedKeys;
        private final double averageLatency;
        
        public LocalityStats(int totalKeys, int optimizedKeys, int migrationCandidates, 
                           int migratedKeys, double averageLatency) {
            this.totalKeys = totalKeys;
            this.optimizedKeys = optimizedKeys;
            this.migrationCandidates = migrationCandidates;
            this.migratedKeys = migratedKeys;
            this.averageLatency = averageLatency;
        }
        
        public int getTotalKeys() {
            return totalKeys;
        }
        
        public int getOptimizedKeys() {
            return optimizedKeys;
        }
        
        public int getMigrationCandidates() {
            return migrationCandidates;
        }
        
        public int getMigratedKeys() {
            return migratedKeys;
        }
        
        public double getAverageLatency() {
            return averageLatency;
        }
        
        public double getOptimizationRatio() {
            return totalKeys > 0 ? (double) optimizedKeys / totalKeys : 0.0;
        }
        
        @Override
        public String toString() {
            return String.format("LocalityStats{total=%d, optimized=%d (%.1f%%), candidates=%d, migrated=%d, avgLatency=%.2fms}", 
                totalKeys, optimizedKeys, getOptimizationRatio() * 100, migrationCandidates, migratedKeys, averageLatency);
        }
    }
    
    /**
     * Configuration for data locality management.
     */
    public static class DataLocalityConfig {
        private final Duration analysisInterval;
        private final Duration optimizationInterval;
        private final Duration migrationLatencyThreshold;
        private final int minAccessesForAnalysis;
        private final int minAccessesForMigration;
        private final boolean autoMigrationEnabled;
        
        public DataLocalityConfig(Duration analysisInterval, Duration optimizationInterval, 
                                Duration migrationLatencyThreshold, int minAccessesForAnalysis, 
                                int minAccessesForMigration, boolean autoMigrationEnabled) {
            this.analysisInterval = analysisInterval;
            this.optimizationInterval = optimizationInterval;
            this.migrationLatencyThreshold = migrationLatencyThreshold;
            this.minAccessesForAnalysis = minAccessesForAnalysis;
            this.minAccessesForMigration = minAccessesForMigration;
            this.autoMigrationEnabled = autoMigrationEnabled;
        }
        
        public static DataLocalityConfig defaultConfig() {
            return new DataLocalityConfig(
                Duration.ofMinutes(15),
                Duration.ofHours(1),
                Duration.ofMillis(50),
                10,
                50,
                false
            );
        }
        
        public static Builder builder() {
            return new Builder();
        }
        
        public Duration getAnalysisInterval() {
            return analysisInterval;
        }
        
        public Duration getOptimizationInterval() {
            return optimizationInterval;
        }
        
        public Duration getMigrationLatencyThreshold() {
            return migrationLatencyThreshold;
        }
        
        public int getMinAccessesForAnalysis() {
            return minAccessesForAnalysis;
        }
        
        public int getMinAccessesForMigration() {
            return minAccessesForMigration;
        }
        
        public boolean isAutoMigrationEnabled() {
            return autoMigrationEnabled;
        }
        
        public static class Builder {
            private Duration analysisInterval = Duration.ofMinutes(15);
            private Duration optimizationInterval = Duration.ofHours(1);
            private Duration migrationLatencyThreshold = Duration.ofMillis(50);
            private int minAccessesForAnalysis = 10;
            private int minAccessesForMigration = 50;
            private boolean autoMigrationEnabled = false;
            
            public Builder analysisInterval(Duration analysisInterval) {
                this.analysisInterval = analysisInterval;
                return this;
            }
            
            public Builder optimizationInterval(Duration optimizationInterval) {
                this.optimizationInterval = optimizationInterval;
                return this;
            }
            
            public Builder migrationLatencyThreshold(Duration migrationLatencyThreshold) {
                this.migrationLatencyThreshold = migrationLatencyThreshold;
                return this;
            }
            
            public Builder minAccessesForAnalysis(int minAccessesForAnalysis) {
                this.minAccessesForAnalysis = minAccessesForAnalysis;
                return this;
            }
            
            public Builder minAccessesForMigration(int minAccessesForMigration) {
                this.minAccessesForMigration = minAccessesForMigration;
                return this;
            }
            
            public Builder autoMigrationEnabled(boolean autoMigrationEnabled) {
                this.autoMigrationEnabled = autoMigrationEnabled;
                return this;
            }
            
            public DataLocalityConfig build() {
                return new DataLocalityConfig(analysisInterval, optimizationInterval, 
                    migrationLatencyThreshold, minAccessesForAnalysis, minAccessesForMigration, autoMigrationEnabled);
            }
        }
    }
}
