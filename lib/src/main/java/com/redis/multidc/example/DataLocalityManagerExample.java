package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.management.DataLocalityManager;
import com.redis.multidc.management.DataLocalityManager.DataLocalityConfig;
import com.redis.multidc.management.DataLocalityManager.MigrationRecommendation;
// Note: AccessPattern and LocalityOptimization are internal classes - we'll work with the public API
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.routing.DatacenterRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Comprehensive example demonstrating DataLocalityManager features:
 * - Intelligent data placement optimization
 * - Access pattern analysis and recommendations
 * - Automatic data migration based on usage patterns
 * - Geographic optimization for reduced latency
 * - Cost optimization through smart routing
 * 
 * This example shows enterprise-grade data locality management for 
 * global multi-datacenter Redis deployments.
 */
public class DataLocalityManagerExample {
    
    private static final Logger logger = LoggerFactory.getLogger(DataLocalityManagerExample.class);
    
    public static void main(String[] args) {
        
        // Configure global multi-datacenter setup
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(Arrays.asList(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .host("redis-us-east-1.example.com")
                    .port(6379)
                    .region("North America")
                    .build(),
                DatacenterEndpoint.builder()
                    .id("eu-west-1")
                    .host("redis-eu-west-1.example.com")
                    .port(6379)
                    .region("Europe")
                    .build(),
                DatacenterEndpoint.builder()
                    .id("ap-southeast-1")
                    .host("redis-ap-southeast-1.example.com")
                    .port(6379)
                    .region("Asia Pacific")
                    .build(),
                DatacenterEndpoint.builder()
                    .id("ap-northeast-1")
                    .host("redis-ap-northeast-1.example.com")
                    .port(6379)
                    .region("Asia Pacific")
                    .build()
            ))
            .build();
        
        MultiDatacenterRedisClient redisClient = MultiDatacenterRedisClientBuilder.create(config);
        try (MultiDatacenterRedisClient client = redisClient) {
            
            DatacenterRouter router = new DatacenterRouter(config, new com.redis.multidc.observability.MetricsCollector(config));
            
            // Configure DataLocalityManager for intelligent optimization
            DataLocalityConfig localityConfig = DataLocalityConfig.builder()
                .autoMigrationEnabled(true)                // Allow automatic migrations
                .analysisInterval(Duration.ofMinutes(15))  // Analysis frequency
                .migrationLatencyThreshold(Duration.ofMillis(50)) // Migration threshold
                .build();
            
            try (DataLocalityManager localityManager = new DataLocalityManager(
                    client, router, config, localityConfig)) {
                
                // === DATA PLACEMENT OPTIMIZATION ===
                logger.info("=== Demonstrating Data Placement Optimization ===");
                demonstrateDataPlacement(client, localityManager);
                
                // === ACCESS PATTERN ANALYSIS ===
                logger.info("=== Demonstrating Access Pattern Analysis ===");
                demonstrateAccessPatternAnalysis(client, localityManager);
                
                // === AUTOMATIC DATA MIGRATION ===
                logger.info("=== Demonstrating Automatic Data Migration ===");
                demonstrateDataMigration(client, localityManager);
                
                // === GEOGRAPHIC OPTIMIZATION ===
                logger.info("=== Demonstrating Geographic Optimization ===");
                demonstrateGeographicOptimization(client, localityManager);
                
                // === COST OPTIMIZATION ===
                logger.info("=== Demonstrating Cost Optimization ===");
                demonstrateCostOptimization(client, localityManager);
                
                logger.info("DataLocalityManager example completed successfully!");
                
            } finally {
                client.close();
            }
        } catch (Exception e) {
            logger.error("Example failed", e);
        }
    }
    
    /**
     * Demonstrates intelligent data placement based on access patterns.
     */
    private static void demonstrateDataPlacement(MultiDatacenterRedisClient client,
                                               DataLocalityManager localityManager) throws Exception {
        
        // Simulate different types of data with different access patterns
        String[] globalKeys = {
            "config:app:global-settings",     // Global configuration - accessed worldwide
            "user:session:us:12345",          // US user session - primarily US access
            "product:catalog:eu:electronics", // EU product catalog - primarily EU access
            "analytics:daily:ap:20231201"     // APAC analytics - primarily APAC access
        };
        
        // Store initial data
        for (String key : globalKeys) {
            String value = "data-for-" + key + "-" + System.currentTimeMillis();
            client.sync().set(key, value, DatacenterPreference.LOCAL_PREFERRED);
            logger.info("Stored data for key: {}", key);
        }
        
        // Simulate access patterns to build analytics
        simulateAccessPatterns(client, localityManager, globalKeys);
        
        // Get optimal read datacenters based on access patterns
        for (String key : globalKeys) {
            CompletableFuture<Optional<String>> optimalDc = localityManager.getOptimalReadDatacenter(key);
            Optional<String> datacenter = optimalDc.get(3, TimeUnit.SECONDS);
            
            if (datacenter.isPresent()) {
                logger.info("Optimal read datacenter for {}: {}", key, datacenter.get());
            } else {
                logger.info("No specific optimization needed for key: {}", key);
            }
        }
        
        // Get optimal write datacenters
        for (String key : globalKeys) {
            CompletableFuture<Optional<String>> optimalDc = localityManager.getOptimalWriteDatacenter(key);
            Optional<String> datacenter = optimalDc.get(3, TimeUnit.SECONDS);
            
            if (datacenter.isPresent()) {
                logger.info("Optimal write datacenter for {}: {}", key, datacenter.get());
            }
        }
    }
    
    /**
     * Demonstrates access pattern analysis and insights.
     */
    private static void demonstrateAccessPatternAnalysis(MultiDatacenterRedisClient client,
                                                       DataLocalityManager localityManager) throws Exception {
        
        String analyticsKey = "user:behavior:europe:shopping-cart";
        
        // Store data and simulate European access pattern
        client.sync().set(analyticsKey, "{\"items\":[],\"total\":0}", DatacenterPreference.LOCAL_PREFERRED);
        
        // Record access patterns (simulate different geographic sources)
        localityManager.recordAccess(analyticsKey, "eu-west-1", "READ", 10);
        localityManager.recordAccess(analyticsKey, "eu-west-1", "READ", 12);
        localityManager.recordAccess(analyticsKey, "eu-west-1", "WRITE", 15);
        localityManager.recordAccess(analyticsKey, "us-east-1", "READ", 80);  // Some US access (higher latency)
        localityManager.recordAccess(analyticsKey, "eu-west-1", "READ", 8);
        
        logger.info("Recorded access patterns for: {}", analyticsKey);
        
        // The DataLocalityManager tracks access patterns internally
        // In a real implementation, you would have methods to query these patterns
        logger.info("Access patterns are being tracked internally by DataLocalityManager");
        logger.info("In production, these would be used for migration and optimization decisions");
    }
    
    /**
     * Demonstrates automatic data migration based on usage patterns.
     */
    private static void demonstrateDataMigration(MultiDatacenterRedisClient client,
                                               DataLocalityManager localityManager) throws Exception {
        
        String migrationKey = "session:user:tokyo:98765";
        
        // Initially store in US datacenter (suboptimal for Tokyo user)
        client.sync().set(migrationKey, "{\"userId\":\"98765\",\"location\":\"Tokyo\"}", 
                         DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Stored Tokyo user session in US datacenter (suboptimal)");
        
        // Simulate heavy access from Asia-Pacific region
        for (int i = 0; i < 20; i++) {
            localityManager.recordAccess(migrationKey, "ap-northeast-1", "READ", 25);
            localityManager.recordAccess(migrationKey, "ap-southeast-1", "READ", 30);
            if (i % 5 == 0) {
                localityManager.recordAccess(migrationKey, "ap-northeast-1", "WRITE", 35);
            }
        }
        
        logger.info("Simulated heavy APAC access for Tokyo user session");
        
        // Analyze migration opportunity
        CompletableFuture<MigrationRecommendation> migrationAnalysis = 
            localityManager.analyzeMigrationOpportunity(migrationKey);
        
        MigrationRecommendation recommendation = migrationAnalysis.get(5, TimeUnit.SECONDS);
        
        if (recommendation != null) {
            logger.info("Migration recommendation for {}:", migrationKey);
            logger.info("  - Recommended: {}", recommendation.isRecommended());
            logger.info("  - Current datacenter: {}", recommendation.getCurrentDatacenter());
            logger.info("  - Target datacenter: {}", recommendation.getTargetDatacenter());
            logger.info("  - Reason: {}", recommendation.getReason());
            
            if (recommendation.isRecommended()) {
                // Perform the migration
                String currentDatacenter = recommendation.getCurrentDatacenter();
                String targetDatacenter = recommendation.getTargetDatacenter();
                
                CompletableFuture<Boolean> migrationResult = localityManager.migrateData(
                    migrationKey, currentDatacenter, targetDatacenter);
                
                boolean migrated = migrationResult.get(10, TimeUnit.SECONDS);
                
                if (migrated) {
                    logger.info("Successfully migrated {} from {} to {}", 
                               migrationKey, currentDatacenter, targetDatacenter);
                } else {
                    logger.error("Failed to migrate {}", migrationKey);
                }
            }
        }
        
        // The migration tracking is handled internally
        logger.info("Migration status is tracked internally by DataLocalityManager");
    }
    
    /**
     * Demonstrates geographic optimization for global applications.
     */
    private static void demonstrateGeographicOptimization(MultiDatacenterRedisClient client,
                                                        DataLocalityManager localityManager) throws Exception {
        
        // Simulate global content delivery scenario
        String[] contentKeys = {
            "cdn:content:homepage:v1.2",
            "cdn:content:product-images:electronics",
            "cdn:content:static-assets:css-bundle"
        };
        
        for (String key : contentKeys) {
            // Store content globally (using ANY_AVAILABLE as a workaround)
            String content = "content-data-for-" + key;
            client.sync().set(key, content, DatacenterPreference.ANY_AVAILABLE);
            logger.info("Deployed content: {}", key);
        }
        
        // Simulate global access patterns
        for (String key : contentKeys) {
            // Simulate different regional access patterns with latency
            simulateRegionalAccess(localityManager, key, "us-east-1", 50);      // High US traffic
            simulateRegionalAccess(localityManager, key, "eu-west-1", 30);      // Medium EU traffic  
            simulateRegionalAccess(localityManager, key, "ap-southeast-1", 25); // Some APAC traffic
            simulateRegionalAccess(localityManager, key, "ap-northeast-1", 15); // Some Japan traffic
        }
        
        // Log that geographic optimization would analyze these patterns
        logger.info("Geographic optimization analysis would be based on recorded access patterns");
        logger.info("In production, this data would drive content placement decisions");
    }
    
    /**
     * Demonstrates cost optimization features.
     */
    private static void demonstrateCostOptimization(MultiDatacenterRedisClient client,
                                                  DataLocalityManager localityManager) throws Exception {
        
        String expensiveKey = "analytics:historical:large-dataset";
        
        // Store large dataset in local datacenter initially
        String largeData = "large-dataset-content-" + "x".repeat(1000);
        client.sync().set(expensiveKey, largeData, DatacenterPreference.LOCAL_PREFERRED);
        logger.info("Stored large dataset in local datacenter");
        
        // Simulate cost optimization analysis
        logger.info("Cost optimization analysis would consider:");
        logger.info("  - Storage costs per datacenter");
        logger.info("  - Network transfer costs");
        logger.info("  - Access patterns and frequency");
        logger.info("  - Regional pricing differences");
        
        // In a real implementation, cost optimization would analyze these factors
        logger.info("Based on access patterns, cost-optimal placement would be determined");
        logger.info("Migration recommendations would minimize total cost of ownership");
    }
    
    /**
     * Helper method to simulate access patterns from different regions.
     */
    private static void simulateAccessPatterns(MultiDatacenterRedisClient client,
                                             DataLocalityManager localityManager,
                                             String[] keys) {
        // Simulate realistic access patterns with latency
        for (String key : keys) {
            if (key.contains("us:")) {
                // US-focused access pattern
                for (int i = 0; i < 15; i++) {
                    localityManager.recordAccess(key, "us-east-1", i % 4 == 0 ? "WRITE" : "READ", 10);
                }
                // Some global access
                localityManager.recordAccess(key, "eu-west-1", "READ", 120);
                localityManager.recordAccess(key, "ap-southeast-1", "READ", 180);
                
            } else if (key.contains("eu:")) {
                // Europe-focused access pattern
                for (int i = 0; i < 12; i++) {
                    localityManager.recordAccess(key, "eu-west-1", i % 5 == 0 ? "WRITE" : "READ", 8);
                }
                localityManager.recordAccess(key, "us-east-1", "READ", 100);
                
            } else if (key.contains("ap:")) {
                // Asia-Pacific focused access pattern
                for (int i = 0; i < 10; i++) {
                    localityManager.recordAccess(key, "ap-southeast-1", i % 3 == 0 ? "WRITE" : "READ", 15);
                }
                for (int i = 0; i < 8; i++) {
                    localityManager.recordAccess(key, "ap-northeast-1", "READ", 12);
                }
                
            } else {
                // Global access pattern
                localityManager.recordAccess(key, "us-east-1", "READ", 10);
                localityManager.recordAccess(key, "eu-west-1", "READ", 15);
                localityManager.recordAccess(key, "ap-southeast-1", "READ", 20);
                localityManager.recordAccess(key, "ap-northeast-1", "READ", 18);
            }
        }
    }
    
    /**
     * Helper method to simulate regional access patterns.
     */
    private static void simulateRegionalAccess(DataLocalityManager localityManager,
                                             String key, String datacenter, int accessCount) {
        for (int i = 0; i < accessCount; i++) {
            // Simulate different latencies based on datacenter
            long latency = switch (datacenter) {
                case "us-east-1" -> 10 + (i % 5);
                case "eu-west-1" -> 15 + (i % 8); 
                case "ap-southeast-1" -> 25 + (i % 10);
                case "ap-northeast-1" -> 30 + (i % 12);
                default -> 50;
            };
            localityManager.recordAccess(key, datacenter, i % 10 == 0 ? "WRITE" : "READ", latency);
        }
    }
}
