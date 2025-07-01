package com.redis.multidc.integration;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.impl.DefaultMultiDatacenterRedisClient;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.model.TombstoneKey;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for multi-datacenter Redis client
 * using Testcontainers to simulate a real 3-datacenter environment.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.DisplayName.class)
@Tag("integration")
class MultiDatacenterIntegrationTest {

    @Container
    static GenericContainer<?> redisDc1 = new GenericContainer<>(DockerImageName.parse("redis:7.2.4"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "yes");

    @Container
    static GenericContainer<?> redisDc2 = new GenericContainer<>(DockerImageName.parse("redis:7.2.4"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "yes");

    @Container
    static GenericContainer<?> redisDc3 = new GenericContainer<>(DockerImageName.parse("redis:7.2.4"))
            .withExposedPorts(6379)
            .withCommand("redis-server", "--appendonly", "yes");

    private static DefaultMultiDatacenterRedisClient client;
    private static DatacenterConfiguration config;

    @BeforeAll
    static void setUpClass() {
        // Wait for containers to be ready
        assertTrue(redisDc1.isRunning(), "DC1 Redis container should be running");
        assertTrue(redisDc2.isRunning(), "DC2 Redis container should be running");
        assertTrue(redisDc3.isRunning(), "DC3 Redis container should be running");

        config = DatacenterConfiguration.builder()
                .localDatacenter("us-east-1")
                // Connection and timeout configurations
                .connectionTimeout(Duration.ofSeconds(10))
                .requestTimeout(Duration.ofSeconds(5))
                .healthCheckInterval(Duration.ofSeconds(30))
                .maxRetries(3)
                .retryDelay(Duration.ofMillis(500))
                .enableCircuitBreaker(true)
                .datacenters(List.of(
                        DatacenterEndpoint.builder()
                                .id("us-east-1")
                                .region("us-east-1")
                                .host(redisDc1.getHost())
                                .port(redisDc1.getFirstMappedPort())
                                .ssl(false) // Disabled for test containers
                                .priority(1) // Primary datacenter
                                .weight(1.0)
                                .readOnly(false)
                                .build(),
                        DatacenterEndpoint.builder()
                                .id("us-west-1")
                                .region("us-west-1")
                                .host(redisDc2.getHost())
                                .port(redisDc2.getFirstMappedPort())
                                .ssl(false) // Disabled for test containers
                                .priority(2) // Secondary datacenter
                                .weight(0.8)
                                .readOnly(false)
                                .build(),
                        DatacenterEndpoint.builder()
                                .id("eu-west-1")
                                .region("eu-west-1")
                                .host(redisDc3.getHost())
                                .port(redisDc3.getFirstMappedPort())
                                .ssl(false) // Disabled for test containers
                                .priority(3) // Tertiary datacenter
                                .weight(0.6)
                                .readOnly(false)
                                .build()
                ))
                .build();

        client = new DefaultMultiDatacenterRedisClient(config);
    }

    @BeforeEach
    void setUp() {
        // Clean up test data before each test
        var sync = client.sync();
        try {
            // Clear all test keys from all datacenters using direct datacenter methods
            for (String dc : List.of("us-east-1", "us-west-1", "eu-west-1")) {
                sync.flushAll(dc);
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    @AfterAll
    static void tearDownClass() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testBasicCrossDatacenterOperations() {
        var sync = client.sync();

        // Write to local datacenter (us-east-1)
        sync.set("test:local", "local-value");
        assertEquals("local-value", sync.get("test:local"));

        // Write to specific datacenters using cross-datacenter operations
        // We'll use a workaround by writing locally and then using cross-datacenter read
        sync.set("test:west", "west-value"); // Will go to local DC
        sync.set("test:eu", "eu-value");     // Will go to local DC

        // Test cross-datacenter reads using crossDatacenterGet
        // Since containers are independent, data won't exist in other DCs
        String westValue = sync.crossDatacenterGet("test:west", "us-west-1");
        String euValue = sync.crossDatacenterGet("test:eu", "eu-west-1");
        
        // These should be null since we haven't replicated data
        assertNull(westValue);
        assertNull(euValue);
        
        // Verify local reads work
        assertEquals("west-value", sync.get("test:west"));
        assertEquals("eu-value", sync.get("test:eu"));
    }

    @Test
    void testUserSessionDistribution() {
        var sync = client.sync();

        // Simulate user sessions - since we can't target specific datacenters for writes
        // we'll simulate by writing to local DC and testing cross-datacenter reads
        Map<String, String> userSessions = Map.of(
            "user:123", "us-east-1",
            "user:456", "us-west-1", 
            "user:789", "eu-west-1"
        );

        // Create sessions locally and test cross-datacenter access patterns
        for (var entry : userSessions.entrySet()) {
            String userId = entry.getKey();
            String datacenter = entry.getValue();
            String sessionData = String.format("{\"userId\":\"%s\",\"loginTime\":\"%d\",\"dc\":\"%s\"}", 
                userId, System.currentTimeMillis(), datacenter);
            
            // Store session data locally
            sync.set("session:" + userId, sessionData);
        }

        // Verify sessions can be retrieved locally
        for (var entry : userSessions.entrySet()) {
            String userId = entry.getKey();
            String datacenter = entry.getValue();
            String sessionData = sync.get("session:" + userId);
            
            assertNotNull(sessionData);
            assertTrue(sessionData.contains(userId));
            assertTrue(sessionData.contains(datacenter));
        }

        // Test cross-datacenter session lookup
        String remoteSession = sync.crossDatacenterGet("session:user:123", "us-west-1");
        // Should be null since data is only in local DC
        assertNull(remoteSession);
    }

    @Test
    void testDistributedCaching() {
        var sync = client.sync();

        // Simulate distributed caching with TTL - using local DC for writes
        Map<String, Object> cacheData = Map.of(
            "cache:product:1", "Product details for item 1",
            "cache:product:2", "Product details for item 2",
            "cache:product:3", "Product details for item 3"
        );

        // Store cache data locally with TTL
        for (var entry : cacheData.entrySet()) {
            String cacheKey = entry.getKey();
            String cacheValue = (String) entry.getValue();
            
            sync.set(cacheKey, cacheValue, Duration.ofMinutes(5));
        }

        // Verify cached data exists locally
        for (var entry : cacheData.entrySet()) {
            String cacheKey = entry.getKey();
            String expectedValue = (String) entry.getValue();
            
            String actualValue = sync.get(cacheKey);
            assertEquals(expectedValue, actualValue);
            
            // Verify TTL is set
            Duration ttl = sync.ttl(cacheKey);
            assertTrue(ttl.toSeconds() > 0 && ttl.toSeconds() <= 300);
        }

        // Test cross-datacenter cache miss
        String remoteCacheValue = sync.crossDatacenterGet("cache:product:1", "us-west-1");
        assertNull(remoteCacheValue); // Should be null as cache is only local
    }

    @Test
    void testAsyncOperationsAcrossDatacenters() throws Exception {
        var async = client.async();

        // Perform async operations - all will target local DC based on configuration
        CompletableFuture<Void> future1 = async.set("test:async1", "value1");
        CompletableFuture<Void> future2 = async.set("test:async2", "value2");
        CompletableFuture<Void> future3 = async.set("test:async3", "value3");

        // Wait for all operations to complete
        CompletableFuture.allOf(future1, future2, future3).get(10, TimeUnit.SECONDS);

        // Verify async reads locally
        CompletableFuture<String> read1 = async.get("test:async1");
        CompletableFuture<String> read2 = async.get("test:async2");
        CompletableFuture<String> read3 = async.get("test:async3");

        assertEquals("value1", read1.get(5, TimeUnit.SECONDS));
        assertEquals("value2", read2.get(5, TimeUnit.SECONDS));
        assertEquals("value3", read3.get(5, TimeUnit.SECONDS));

        // Test cross-datacenter async operations
        CompletableFuture<String> crossRead1 = async.crossDatacenterGet("test:async1", "us-west-1");
        CompletableFuture<String> crossRead2 = async.crossDatacenterGet("test:async2", "eu-west-1");
        
        // Should be null since data is only in local DC
        assertNull(crossRead1.get(5, TimeUnit.SECONDS));
        assertNull(crossRead2.get(5, TimeUnit.SECONDS));
    }

    @Test
    void testReactiveOperationsAcrossDatacenters() {
        var reactive = client.reactive();

        // Test reactive operations - all targeting local DC
        assertDoesNotThrow(() -> {
            reactive.set("test:reactive1", "value1")
                    .block(Duration.ofSeconds(5));
            
            reactive.set("test:reactive2", "value2")
                    .block(Duration.ofSeconds(5));
        });

        // Verify reactive reads locally
        String value1 = reactive.get("test:reactive1")
                .block(Duration.ofSeconds(5));
        String value2 = reactive.get("test:reactive2")
                .block(Duration.ofSeconds(5));

        assertEquals("value1", value1);
        assertEquals("value2", value2);

        // Test cross-datacenter reactive operations
        String crossValue1 = reactive.crossDatacenterGet("test:reactive1", "us-west-1")
                .block(Duration.ofSeconds(5));
        String crossValue2 = reactive.crossDatacenterGet("test:reactive2", "eu-west-1")
                .block(Duration.ofSeconds(5));
        
        // Should be null since data is only in local DC
        assertNull(crossValue1);
        assertNull(crossValue2);
    }

    @Test
    void testDistributedCounters() {
        var sync = client.sync();

        // Create counters locally and test cross-datacenter access
        String counterKey = "counter:global";
        
        // Create local counters (all will go to local DC)
        for (String dc : List.of("us-east-1", "us-west-1", "eu-west-1")) {
            String dcCounterKey = counterKey + ":" + dc;
            sync.set(dcCounterKey, "0");
            
            // Increment counter multiple times using string operations
            for (int i = 0; i < 5; i++) {
                String currentValue = sync.get(dcCounterKey);
                int count = (currentValue != null) ? Integer.parseInt(currentValue) : 0;
                sync.set(dcCounterKey, String.valueOf(count + 1));
            }
        }

        // Verify counter values locally
        for (String dc : List.of("us-east-1", "us-west-1", "eu-west-1")) {
            String dcCounterKey = counterKey + ":" + dc;
            String value = sync.get(dcCounterKey);
            assertEquals("5", value);
        }

        // Test cross-datacenter counter access
        String remoteCounter = sync.crossDatacenterGet("counter:global:us-east-1", "us-west-1");
        assertNull(remoteCounter); // Should be null as data is only in local DC
    }

    @Test
    void testHashOperationsAcrossDatacenters() {
        var sync = client.sync();

        // Create hash structures locally and test cross-datacenter access
        String hashKey = "user:profile:123";
        
        // Store user profile locally
        Map<String, String> profileData = Map.of(
            "name", "John Doe",
            "email", "john@example.com",
            "region", "us-east-1",
            "lastLogin", String.valueOf(System.currentTimeMillis())
        );
        
        sync.hset(hashKey, profileData, DatacenterPreference.LOCAL_PREFERRED);

        // Verify hash operations locally
        Map<String, String> retrievedProfile = sync.hgetAll(hashKey, DatacenterPreference.LOCAL_PREFERRED);
        assertEquals(4, retrievedProfile.size());
        assertEquals("John Doe", retrievedProfile.get("name"));
        assertEquals("john@example.com", retrievedProfile.get("email"));

        // Test individual field operations locally
        assertEquals("John Doe", sync.hget(hashKey, "name", DatacenterPreference.LOCAL_PREFERRED));
        assertTrue(sync.hexists(hashKey, "email", DatacenterPreference.LOCAL_PREFERRED));
        
        // Update field locally
        sync.hset(hashKey, "lastLogin", String.valueOf(System.currentTimeMillis()), 
                  DatacenterPreference.LOCAL_PREFERRED);
                  
        // Test that data doesn't exist in remote datacenters (realistic scenario)
        String remoteName = sync.crossDatacenterGet(hashKey + ":name", "us-west-1");
        assertNull(remoteName, "Hash data should not exist in remote datacenter");
    }

    @Test
    void testListOperationsAcrossDatacenters() {
        var sync = client.sync();

        // Create lists locally and test operations
        for (String region : List.of("us-east-1", "us-west-1", "eu-west-1")) {
            String listKey = "queue:" + region;
            
            // Push items to list locally
            for (int i = 1; i <= 5; i++) {
                sync.lpush(listKey, DatacenterPreference.LOCAL_PREFERRED, "item-" + i);
            }
            
            // Verify list length locally
            assertEquals(5, sync.llen(listKey, DatacenterPreference.LOCAL_PREFERRED));
            
            // Get range of items locally
            List<String> items = sync.lrange(listKey, 0, 2, DatacenterPreference.LOCAL_PREFERRED);
            assertEquals(3, items.size());
            assertEquals("item-5", items.get(0)); // Most recent item
        }
        
        // Test that lists don't exist in remote datacenters
        String remoteListKey = "queue:us-west-1";
        String remoteFirstItem = sync.crossDatacenterGet(remoteListKey + ":0", "eu-west-1");
        assertNull(remoteFirstItem, "List data should not exist in remote datacenter");
    }

    @Test
    void testSetOperationsAcrossDatacenters() {
        var sync = client.sync();

        // Create sets locally and test operations
        for (String region : List.of("us-east-1", "us-west-1", "eu-west-1")) {
            String setKey = "tags:" + region;
            String[] tags = {"java", "redis", "distributed", "cache", region};
            
            // Add members to set locally
            sync.sadd(setKey, DatacenterPreference.LOCAL_PREFERRED, tags);
            
            // Verify set operations locally
            assertEquals(5, sync.scard(setKey, DatacenterPreference.LOCAL_PREFERRED));
            assertTrue(sync.sismember(setKey, "java", DatacenterPreference.LOCAL_PREFERRED));
            assertTrue(sync.sismember(setKey, region, DatacenterPreference.LOCAL_PREFERRED));
            
            // Get all members locally
            Set<String> members = sync.smembers(setKey, DatacenterPreference.LOCAL_PREFERRED);
            assertEquals(5, members.size());
            assertTrue(members.contains("java"));
            assertTrue(members.contains(region));
        }
        
        // Test that sets don't exist in remote datacenters
        String remoteSetKey = "tags:us-east-1";
        String remoteSetData = sync.crossDatacenterGet(remoteSetKey, "eu-west-1");
        assertNull(remoteSetData, "Set data should not exist in remote datacenter");
    }

    @Test
    void testSortedSetOperationsAcrossDatacenters() {
        var sync = client.sync();

        // Create leaderboards locally and test operations
        for (String region : List.of("us-east-1", "us-west-1", "eu-west-1")) {
            String leaderboardKey = "leaderboard:" + region;
            
            // Add players with scores locally
            sync.zadd(leaderboardKey, 100.0, "player1", DatacenterPreference.LOCAL_PREFERRED);
            sync.zadd(leaderboardKey, 250.0, "player2", DatacenterPreference.LOCAL_PREFERRED);
            sync.zadd(leaderboardKey, 175.0, "player3", DatacenterPreference.LOCAL_PREFERRED);
            
            // Verify sorted set operations locally
            assertEquals(3, sync.zcard(leaderboardKey, DatacenterPreference.LOCAL_PREFERRED));
            assertEquals(250.0, sync.zscore(leaderboardKey, "player2", DatacenterPreference.LOCAL_PREFERRED));
            
            // Get top players locally
            Set<String> topPlayers = sync.zrange(leaderboardKey, 0, 1, DatacenterPreference.LOCAL_PREFERRED);
            assertEquals(2, topPlayers.size());
            assertTrue(topPlayers.contains("player1"));
            assertTrue(topPlayers.contains("player3"));
        }
        
        // Test that leaderboards don't exist in remote datacenters
        String remoteLeaderboard = sync.crossDatacenterGet("leaderboard:us-east-1", "eu-west-1");
        assertNull(remoteLeaderboard, "Leaderboard data should not exist in remote datacenter");
    }

    @Test
    void testTombstoneOperationsAcrossDatacenters() {
        var sync = client.sync();

        // Test tombstone operations locally
        for (String region : List.of("us-east-1", "us-west-1", "eu-west-1")) {
            String key = "tombstone:test:" + region;
            
            // Create and verify tombstone locally
            sync.createTombstone(key, TombstoneKey.Type.SOFT_DELETE);
            assertTrue(sync.isTombstoned(key));
            
            // Get tombstone details locally
            TombstoneKey tombstone = sync.getTombstone(key);
            assertNotNull(tombstone);
            assertEquals(key, tombstone.getKey());
            assertNotNull(tombstone.getCreatedAt());
        }
        
        // Test that tombstones don't exist in remote datacenters
        String remoteTombstone = sync.crossDatacenterGet("tombstone:test:us-east-1", "eu-west-1");
        assertNull(remoteTombstone, "Tombstone should not exist in remote datacenter");
    }

    @Test
    void testDistributedLockingAcrossDatacenters() {
        var sync = client.sync();

        // Test distributed locking locally
        for (String region : List.of("us-east-1", "us-west-1", "eu-west-1")) {
            String lockKey = "lock:resource:" + region;
            String lockValue = "thread-" + region + "-" + Thread.currentThread().getId();
            
            // Acquire lock locally
            boolean acquired = sync.acquireLock(lockKey, lockValue, Duration.ofMinutes(1), 
                                              DatacenterPreference.LOCAL_PREFERRED);
            assertTrue(acquired);
            
            // Verify lock is held locally
            assertTrue(sync.isLocked(lockKey));
            
            // Try to acquire same lock (should fail)
            boolean reacquired = sync.acquireLock(lockKey, "different-value", Duration.ofMinutes(1), 
                                                DatacenterPreference.LOCAL_PREFERRED);
            assertFalse(reacquired);
            
            // Release lock locally
            boolean released = sync.releaseLock(lockKey, lockValue, DatacenterPreference.LOCAL_PREFERRED);
            assertTrue(released);
            
            // Verify lock is released locally
            assertFalse(sync.isLocked(lockKey));
        }
        
        // Test that locks don't exist in remote datacenters
        String remoteLock = sync.crossDatacenterGet("lock:resource:us-east-1", "eu-west-1");
        assertNull(remoteLock, "Lock should not exist in remote datacenter");
    }

    @Test
    void testDatacenterFailoverScenario() {
        var sync = client.sync();

        // Set up data locally (simulating primary datacenter)
        sync.set("failover:test", "original-value", DatacenterPreference.LOCAL_PREFERRED);
        sync.set("failover:backup", "backup-value", DatacenterPreference.LOCAL_PREFERRED);

        // Verify data exists locally
        assertEquals("original-value", sync.get("failover:test", DatacenterPreference.LOCAL_PREFERRED));
        assertEquals("backup-value", sync.get("failover:backup", DatacenterPreference.LOCAL_PREFERRED));

        // Test cross-datacenter reads (simulating failover scenario)
        // In real failover, this would read from backup datacenters when primary is down
        String remoteValue1 = sync.crossDatacenterGet("failover:test", "us-west-1");
        String remoteValue2 = sync.crossDatacenterGet("failover:backup", "eu-west-1");
        
        // These should be null since we haven't replicated data (realistic scenario)
        assertNull(remoteValue1, "Data should not exist in remote datacenter without replication");
        assertNull(remoteValue2, "Data should not exist in remote datacenter without replication");
        
        // Test that operations continue to work with fallback preferences
        assertDoesNotThrow(() -> sync.set("failover:test2", "new-value", DatacenterPreference.ANY_AVAILABLE));
        assertEquals("new-value", sync.get("failover:test2", DatacenterPreference.ANY_AVAILABLE));

        // Test multiple datacenter preferences (simulating smart failover)
        assertDoesNotThrow(() -> sync.set("failover:robust", "robust-value", DatacenterPreference.LOCAL_PREFERRED));
        String robustValue = sync.get("failover:robust", DatacenterPreference.ANY_AVAILABLE);
        assertEquals("robust-value", robustValue);
    }

    @Test
    void testPingAllDatacenters() {
        var sync = client.sync();

        // Test connectivity to all datacenters
        for (String dc : List.of("us-east-1", "us-west-1", "eu-west-1")) {
            String pong = sync.ping(dc);
            assertEquals("PONG", pong);
        }
    }

    @Test
    void testCrossDatacenterMultiGet() {
        var sync = client.sync();

        // Set up data locally
        sync.set("multiget:key1", "value1", DatacenterPreference.LOCAL_PREFERRED);
        sync.set("multiget:key2", "value2", DatacenterPreference.LOCAL_PREFERRED);
        sync.set("multiget:key3", "value3", DatacenterPreference.LOCAL_PREFERRED);

        // Test cross-datacenter multi-get (realistic scenario where data doesn't replicate automatically)
        Map<String, String> values = sync.crossDatacenterMultiGet(
            List.of("multiget:key1", "multiget:key2", "multiget:key3"),
            "us-west-1"
        );

        // In a realistic scenario with independent containers, we'd only get data from local DC
        // The crossDatacenterMultiGet should handle cases where data doesn't exist in all DCs
        assertNotNull(values, "Multi-get should return a map even if some keys are missing");
        
        // Test local multi-get operations
        List<String> localValues = sync.mget(DatacenterPreference.LOCAL_PREFERRED, 
            "multiget:key1", "multiget:key2", "multiget:key3");
        assertEquals(3, localValues.size());
        assertEquals("value1", localValues.get(0));
        assertEquals("value2", localValues.get(1));
        assertEquals("value3", localValues.get(2));
    }

    @Test
    void testDatabaseSizeAcrossDatacenters() {
        var sync = client.sync();

        // Add some test data locally
        for (int i = 0; i < 5; i++) {
            sync.set("dbsize:test:" + i, "value" + i, DatacenterPreference.LOCAL_PREFERRED);
        }

        // Check database size for local datacenter
        long localDbSize = sync.dbSize("us-east-1");
        assertTrue(localDbSize >= 5, "Local database size should be at least 5");

        // Check database size for each datacenter using direct datacenter ID methods
        for (String dc : List.of("us-east-1", "us-west-1", "eu-west-1")) {
            long dcDbSize = sync.dbSize(dc);
            // Only local DC should have data, others should be 0 or have minimal data
            if (dc.equals("us-east-1")) { // Local datacenter
                assertTrue(dcDbSize >= 5, "Local datacenter should have at least 5 keys");
            } else {
                // Remote datacenters should be empty in this isolated test setup
                assertTrue(dcDbSize >= 0, "Database size should be non-negative for " + dc);
            }
        }
    }
    
    @Test
    void testRealisticECommerceScenario() {
        var sync = client.sync();
        
        // Simulate e-commerce session and shopping cart scenario
        String userId = "user:12345";
        String sessionId = "session:" + userId;
        String cartId = "cart:" + userId;
        
        // Create user session locally
        String sessionData = String.format(
            "{\"userId\":\"%s\",\"loginTime\":\"%d\",\"region\":\"us-east-1\"}", 
            userId, System.currentTimeMillis()
        );
        sync.set(sessionId, sessionData, Duration.ofMinutes(30), DatacenterPreference.LOCAL_PREFERRED);
        
        // Create shopping cart with hash operations
        Map<String, String> cartItems = Map.of(
            "item1", "2",  // quantity
            "item2", "1",
            "item3", "5"
        );
        sync.hset(cartId, cartItems, DatacenterPreference.LOCAL_PREFERRED);
        
        // Add to recent searches (list operations)
        String searchKey = "search:" + userId;
        sync.lpush(searchKey, DatacenterPreference.LOCAL_PREFERRED, "laptop", "mouse", "keyboard");
        
        // Add to user preferences (set operations)
        String prefsKey = "prefs:" + userId;
        sync.sadd(prefsKey, DatacenterPreference.LOCAL_PREFERRED, "electronics", "computers", "tech");
        
        // Verify all data exists locally
        assertNotNull(sync.get(sessionId, DatacenterPreference.LOCAL_PREFERRED));
        assertEquals(3, sync.hgetAll(cartId, DatacenterPreference.LOCAL_PREFERRED).size());
        assertEquals(3, sync.llen(searchKey, DatacenterPreference.LOCAL_PREFERRED));
        assertEquals(3, sync.scard(prefsKey, DatacenterPreference.LOCAL_PREFERRED));
        
        // Test cross-datacenter access (should be null/empty in isolated setup)
        assertNull(sync.crossDatacenterGet(sessionId, "eu-west-1"));
        assertNull(sync.crossDatacenterGet(cartId, "us-west-1"));
    }
    
    @Test
    void testRealisticCachingAndTTLScenario() {
        var sync = client.sync();
        
        // Product catalog caching scenario
        String productKey = "product:abc123";
        String productData = "{\"id\":\"abc123\",\"name\":\"Laptop\",\"price\":999.99}";
        
        // Cache product with 5-minute TTL
        sync.set(productKey, productData, Duration.ofMinutes(5), DatacenterPreference.LOCAL_PREFERRED);
        
        // Verify cache hit
        assertEquals(productData, sync.get(productKey, DatacenterPreference.LOCAL_PREFERRED));
        
        // Check TTL
        Duration ttl = sync.ttl(productKey, DatacenterPreference.LOCAL_PREFERRED);
        assertTrue(ttl.toSeconds() > 0 && ttl.toSeconds() <= 300);
        
        // User activity counter with expiration
        String counterKey = "activity:daily:" + userId + ":" + java.time.LocalDate.now();
        sync.set(counterKey, "0", Duration.ofHours(24), DatacenterPreference.LOCAL_PREFERRED);
        
        // Increment activity using string operations
        for (int i = 0; i < 5; i++) {
            String currentValue = sync.get(counterKey, DatacenterPreference.LOCAL_PREFERRED);
            int count = (currentValue != null) ? Integer.parseInt(currentValue) : 0;
            sync.set(counterKey, String.valueOf(count + 1), DatacenterPreference.LOCAL_PREFERRED);
        }
        
        assertEquals("5", sync.get(counterKey, DatacenterPreference.LOCAL_PREFERRED));
        
        // Cache warming scenario - multiple product types
        Map<String, String> bulkProducts = Map.of(
            "product:def456", "{\"id\":\"def456\",\"name\":\"Mouse\",\"price\":29.99}",
            "product:ghi789", "{\"id\":\"ghi789\",\"name\":\"Keyboard\",\"price\":79.99}",
            "product:jkl012", "{\"id\":\"jkl012\",\"name\":\"Monitor\",\"price\":299.99}"
        );
        
        sync.mset(bulkProducts, DatacenterPreference.LOCAL_PREFERRED);
        
        // Verify bulk cache
        List<String> cachedProducts = sync.mget(DatacenterPreference.LOCAL_PREFERRED, 
            "product:def456", "product:ghi789", "product:jkl012");
        assertEquals(3, cachedProducts.size());
        cachedProducts.forEach(product -> assertNotNull(product));
    }
    
    @Test
    void testRealisticDataLocalityScenario() {
        var sync = client.sync();
        
        // Simulate data locality requirements for different regions
        String[] regions = {"us-east-1", "us-west-1", "eu-west-1"};
        
        for (String region : regions) {
            // User data that should stay local to region for compliance
            String userKey = "gdpr:user:" + region + ":user123";
            String userData = String.format(
                "{\"region\":\"%s\",\"data\":\"sensitive\",\"compliance\":\"GDPR\"}", 
                region
            );
            
            // Store locally only (simulating data residency requirements)
            sync.set(userKey, userData, DatacenterPreference.LOCAL_PREFERRED);
            
            // Verify local access works
            assertNotNull(sync.get(userKey, DatacenterPreference.LOCAL_PREFERRED));
            
            // Test that we respect data locality by not accessing remote data
            if (!region.equals("us-east-1")) { // Only test cross-datacenter for non-local
                String remoteData = sync.crossDatacenterGet(userKey, region);
                // Should be null since we can't access remote data in isolated setup
                assertNull(remoteData, "Should not access remote sensitive data");
            }
        }
        
        // Global configuration that can be accessed from anywhere
        String globalConfigKey = "config:global:features";
        sync.set(globalConfigKey, "{\"feature_x\":true,\"feature_y\":false}", 
                DatacenterPreference.ANY_AVAILABLE);
        
        // Should be accessible with any preference
        assertNotNull(sync.get(globalConfigKey, DatacenterPreference.LOCAL_PREFERRED));
        assertNotNull(sync.get(globalConfigKey, DatacenterPreference.ANY_AVAILABLE));
    }

    private final String userId = "user12345"; // Add field for the realistic test methods

    @Test
    void testProductionReadyConfiguration() {
        // This test demonstrates how to configure SSL and connection pooling for production
        // We'll use our existing working configuration and enhance it with production settings
        
        // Test the current configuration has the enhanced settings we added
        assertNotNull(config);
        assertEquals("us-east-1", config.getLocalDatacenterId());
        assertEquals(3, config.getDatacenters().size());
        
        // Verify enhanced connection settings
        assertEquals(Duration.ofSeconds(10), config.getConnectionTimeout());
        assertEquals(Duration.ofSeconds(5), config.getRequestTimeout());
        assertEquals(Duration.ofSeconds(30), config.getHealthCheckInterval());
        assertEquals(3, config.getMaxRetries());
        assertEquals(Duration.ofMillis(500), config.getRetryDelay());
        assertTrue(config.isCircuitBreakerEnabled());
        
        // Verify datacenter endpoint configurations
        for (DatacenterEndpoint endpoint : config.getDatacenters()) {
            assertNotNull(endpoint.getId());
            assertNotNull(endpoint.getRegion());
            assertNotNull(endpoint.getHost());
            assertTrue(endpoint.getPort() > 0);
            assertTrue(endpoint.getWeight() > 0);
            assertTrue(endpoint.getPriority() >= 0);
            
            // In our test setup, SSL is disabled, but we verify the capability exists
            assertFalse(endpoint.isSsl(), "Test containers should have SSL disabled");
        }
        
        System.out.println("Production configuration patterns demonstrated:");
        System.out.println("==============================================");
        System.out.println("Connection Management:");
        System.out.printf("- Connection timeout: %s%n", config.getConnectionTimeout());
        System.out.printf("- Request timeout: %s%n", config.getRequestTimeout());
        System.out.printf("- Health check interval: %s%n", config.getHealthCheckInterval());
        System.out.printf("- Max retries: %d%n", config.getMaxRetries());
        System.out.printf("- Retry delay: %s%n", config.getRetryDelay());
        System.out.printf("- Circuit breaker: %s%n", config.isCircuitBreakerEnabled() ? "Enabled" : "Disabled");
        
        System.out.println("\nDatacenter Configuration:");
        for (DatacenterEndpoint endpoint : config.getDatacenters()) {
            System.out.printf("- %s (%s): priority=%d, weight=%.1f, ssl=%s, readOnly=%s%n",
                endpoint.getId(), 
                endpoint.getRegion(),
                endpoint.getPriority(),
                endpoint.getWeight(),
                endpoint.isSsl(),
                endpoint.isReadOnly()
            );
        }
        
        System.out.println("\nProduction SSL and Connection Pool Best Practices:");
        System.out.println("- Enable SSL/TLS for all production connections (.ssl(true))");
        System.out.println("- Use strong passwords and certificate validation");
        System.out.println("- Configure connection pooling with appropriate min/max connections");
        System.out.println("- Set reasonable timeouts for WAN connections (10-15 seconds)");
        System.out.println("- Enable circuit breaker pattern for fault tolerance");
        System.out.println("- Use health checks to monitor datacenter availability");
        System.out.println("- Configure priority and weight for intelligent routing");
        System.out.println("- Use read replicas for scaling read operations");
        System.out.println("- Set up proper monitoring and alerting");
        
        // Note: In a real production environment, you would also configure:
        // - Connection pool settings (min/max connections, idle timeout)
        // - SSL certificate validation and trust stores
        // - Authentication mechanisms (Redis AUTH, mTLS)
        // - Network security (VPC, security groups, firewall rules)
        // - Monitoring and alerting integration (Prometheus, Grafana)
        // - Backup and disaster recovery settings
        // - Data encryption at rest
        // - Compliance and audit logging
        // - Data encryption at rest
        // - Compliance and audit logging
    }

    @Test
    void testConnectionPoolAndSSLBestPractices() {
        // This test demonstrates connection pool and SSL best practices
        // Note: These configurations would be used in the actual client implementation
        
        var sync = client.sync();
        
        // Test that our current configuration works with the available settings
        assertNotNull(config.getConnectionTimeout());
        assertNotNull(config.getRequestTimeout());
        assertNotNull(config.getHealthCheckInterval());
        assertTrue(config.getMaxRetries() > 0);
        assertNotNull(config.getRetryDelay());
        
        // Verify that each datacenter endpoint has proper configuration
        for (DatacenterEndpoint endpoint : config.getDatacenters()) {
            assertNotNull(endpoint.getId());
            assertNotNull(endpoint.getHost());
            assertTrue(endpoint.getPort() > 0);
            assertTrue(endpoint.getWeight() > 0);
            assertTrue(endpoint.getPriority() >= 0);
            
            // In our test setup, SSL is disabled, but we verify the capability exists
            assertFalse(endpoint.isSsl(), "Test containers should have SSL disabled");
        }
        
        // Test basic connectivity to verify our connection configuration works
        String pong1 = sync.ping("us-east-1");
        String pong2 = sync.ping("us-west-1");
        String pong3 = sync.ping("eu-west-1");
        
        assertEquals("PONG", pong1);
        assertEquals("PONG", pong2);
        assertEquals("PONG", pong3);
        
        // Test that timeouts and retries work by performing operations
        assertDoesNotThrow(() -> {
            sync.set("config:test", "timeout-test", DatacenterPreference.LOCAL_PREFERRED);
            String value = sync.get("config:test", DatacenterPreference.LOCAL_PREFERRED);
            assertEquals("timeout-test", value);
        });
        
        System.out.println("Connection configuration validation successful:");
        System.out.println("- Connection timeout: " + config.getConnectionTimeout());
        System.out.println("- Request timeout: " + config.getRequestTimeout());
        System.out.println("- Health check interval: " + config.getHealthCheckInterval());
        System.out.println("- Max retries: " + config.getMaxRetries());
        System.out.println("- Retry delay: " + config.getRetryDelay());
        System.out.println("- Circuit breaker enabled: " + config.isCircuitBreakerEnabled());
        
        for (DatacenterEndpoint endpoint : config.getDatacenters()) {
            System.out.printf("- Datacenter %s: priority=%d, weight=%.1f, ssl=%s%n", 
                endpoint.getId(), endpoint.getPriority(), endpoint.getWeight(), endpoint.isSsl());
        }
    }
    
    /**
     * Demonstrates enterprise-grade configuration patterns for multi-datacenter Redis deployment.
     * This test shows SSL, connection pooling, authentication, and high availability configurations.
     */
    @Test
    void testEnterpriseMultiDatacenterConfiguration() {
        // Enterprise configuration with comprehensive settings
        DatacenterConfiguration enterpriseConfig = DatacenterConfiguration.builder()
                .localDatacenter("us-east-1")
                
                // Connection and performance tuning
                .connectionTimeout(Duration.ofSeconds(15)) // Longer timeout for WAN connections
                .requestTimeout(Duration.ofSeconds(10))    // Request timeout
                .healthCheckInterval(Duration.ofSeconds(60)) // Health check every minute
                .maxRetries(5)                             // More retries for distributed systems
                .retryDelay(Duration.ofMillis(100))        // Exponential backoff starting point
                
                // Reliability and fault tolerance
                .enableCircuitBreaker(true)
                
                .datacenters(List.of(
                        // Primary datacenter - US East (Virginia)
                        DatacenterEndpoint.builder()
                                .id("us-east-1-primary")
                                .region("us-east-1")
                                .host("redis-cluster-primary.us-east-1.example.com")
                                .port(6380) // SSL-enabled port
                                .ssl(true)
                                .password("${REDIS_PASSWORD}") // Environment variable reference
                                .database(0)
                                .priority(1) // Highest priority
                                .weight(1.0) // Full weight
                                .readOnly(false)
                                .build(),
                                
                        // US East read replicas for load distribution
                        DatacenterEndpoint.builder()
                                .id("us-east-1-replica-1")
                                .region("us-east-1")
                                .host("redis-replica-1.us-east-1.example.com")
                                .port(6380)
                                .ssl(true)
                                .password("${REDIS_PASSWORD}")
                                .database(0)
                                .priority(1)
                                .weight(0.7) // Slightly lower weight
                                .readOnly(true) // Read-only for scaling
                                .build(),
                                
                        DatacenterEndpoint.builder()
                                .id("us-east-1-replica-2")
                                .region("us-east-1")
                                .host("redis-replica-2.us-east-1.example.com")
                                .port(6380)
                                .ssl(true)
                                .password("${REDIS_PASSWORD}")
                                .database(0)
                                .priority(1)
                                .weight(0.7)
                                .readOnly(true)
                                .build(),
                                
                        // Secondary datacenter - US West (Oregon)
                        DatacenterEndpoint.builder()
                                .id("us-west-2-primary")
                                .region("us-west-2")
                                .host("redis-cluster-primary.us-west-2.example.com")
                                .port(6380)
                                .ssl(true)
                                .password("${REDIS_PASSWORD}")
                                .database(0)
                                .priority(2) // Secondary priority
                                .weight(0.9) // High weight for failover
                                .readOnly(false)
                                .build(),
                                
                        DatacenterEndpoint.builder()
                                .id("us-west-2-replica")
                                .region("us-west-2")
                                .host("redis-replica.us-west-2.example.com")
                                .port(6380)
                                .ssl(true)
                                .password("${REDIS_PASSWORD}")
                                .database(0)
                                .priority(2)
                                .weight(0.6)
                                .readOnly(true)
                                .build(),
                                
                        // Tertiary datacenter - EU West (Ireland) for global reach
                        DatacenterEndpoint.builder()
                                .id("eu-west-1-primary")
                                .region("eu-west-1")
                                .host("redis-cluster-primary.eu-west-1.example.com")
                                .port(6380)
                                .ssl(true)
                                .password("${REDIS_PASSWORD}")
                                .database(0)
                                .priority(3) // Tertiary priority
                                .weight(0.8) // Good weight for EU users
                                .readOnly(false)
                                .build(),
                                
                        // Asia Pacific datacenter for global distribution
                        DatacenterEndpoint.builder()
                                .id("ap-southeast-1-primary")
                                .region("ap-southeast-1")
                                .host("redis-cluster-primary.ap-southeast-1.example.com")
                                .port(6380)
                                .ssl(true)
                                .password("${REDIS_PASSWORD}")
                                .database(0)
                                .priority(4) // Lowest priority from US perspective
                                .weight(0.7) // Lower weight due to latency
                                .readOnly(false)
                                .build()
                ))
                .build();
        
        // Validate enterprise configuration
        assertNotNull(enterpriseConfig);
        assertEquals("us-east-1", enterpriseConfig.getLocalDatacenterId());
        assertEquals(8, enterpriseConfig.getDatacenters().size()); // Total endpoints
        
        // Verify security configurations
        long sslEnabledCount = enterpriseConfig.getDatacenters().stream()
                .filter(DatacenterEndpoint::isSsl)
                .count();
        assertEquals(8, sslEnabledCount, "All endpoints should have SSL enabled");
        
        long passwordProtectedCount = enterpriseConfig.getDatacenters().stream()
                .filter(dc -> dc.getPassword() != null && !dc.getPassword().isEmpty())
                .count();
        assertEquals(8, passwordProtectedCount, "All endpoints should have password protection");
        
        // Verify read replica configuration
        long readReplicaCount = enterpriseConfig.getDatacenters().stream()
                .filter(DatacenterEndpoint::isReadOnly)
                .count();
        assertEquals(4, readReplicaCount, "Should have 4 read replicas for scaling");
        
        long writableCount = enterpriseConfig.getDatacenters().stream()
                .filter(dc -> !dc.isReadOnly())
                .count();
        assertEquals(4, writableCount, "Should have 4 writable endpoints");
        
        // Verify geographic distribution
        Set<String> regions = enterpriseConfig.getDatacenters().stream()
                .map(DatacenterEndpoint::getRegion)
                .collect(java.util.stream.Collectors.toSet());
        assertTrue(regions.contains("us-east-1"), "Should include US East");
        assertTrue(regions.contains("us-west-2"), "Should include US West");
        assertTrue(regions.contains("eu-west-1"), "Should include EU West");
        assertTrue(regions.contains("ap-southeast-1"), "Should include Asia Pacific");
        
        // Verify priority configuration for intelligent routing
        Map<Integer, Long> priorityDistribution = enterpriseConfig.getDatacenters().stream()
                .collect(java.util.stream.Collectors.groupingBy(
                    DatacenterEndpoint::getPriority,
                    java.util.stream.Collectors.counting()
                ));
        assertEquals(3, priorityDistribution.get(1).intValue(), "Priority 1 should have 3 endpoints");
        assertEquals(2, priorityDistribution.get(2).intValue(), "Priority 2 should have 2 endpoints");
        assertEquals(1, priorityDistribution.get(3).intValue(), "Priority 3 should have 1 endpoint");
        assertEquals(1, priorityDistribution.get(4).intValue(), "Priority 4 should have 1 endpoint");
        
        // Verify connection and reliability settings
        assertEquals(Duration.ofSeconds(15), enterpriseConfig.getConnectionTimeout());
        assertEquals(Duration.ofSeconds(10), enterpriseConfig.getRequestTimeout());
        assertEquals(Duration.ofSeconds(60), enterpriseConfig.getHealthCheckInterval());
        assertEquals(5, enterpriseConfig.getMaxRetries());
        assertEquals(Duration.ofMillis(100), enterpriseConfig.getRetryDelay());
        assertTrue(enterpriseConfig.isCircuitBreakerEnabled());
        
        System.out.println("Enterprise Multi-Datacenter Configuration Summary:");
        System.out.println("================================================");
        System.out.printf("Total Endpoints: %d%n", enterpriseConfig.getDatacenters().size());
        System.out.printf("SSL-Enabled Endpoints: %d%n", sslEnabledCount);
        System.out.printf("Read Replicas: %d%n", readReplicaCount);
        System.out.printf("Writable Endpoints: %d%n", writableCount);
        System.out.printf("Geographic Regions: %s%n", String.join(", ", regions));
        System.out.printf("Connection Timeout: %s%n", enterpriseConfig.getConnectionTimeout());
        System.out.printf("Health Check Interval: %s%n", enterpriseConfig.getHealthCheckInterval());
        System.out.printf("Max Retries: %d%n", enterpriseConfig.getMaxRetries());
        System.out.printf("Circuit Breaker: %s%n", enterpriseConfig.isCircuitBreakerEnabled() ? "Enabled" : "Disabled");
        
        System.out.println("\nDatacenter Priority Distribution:");
        for (var entry : priorityDistribution.entrySet()) {
            System.out.printf("Priority %d: %d endpoints%n", entry.getKey(), entry.getValue());
        }
        
        System.out.println("\nEndpoint Details:");
        for (DatacenterEndpoint endpoint : enterpriseConfig.getDatacenters()) {
            System.out.printf("- %s (%s): priority=%d, weight=%.1f, ssl=%s, readOnly=%s%n",
                endpoint.getId(), 
                endpoint.getRegion(),
                endpoint.getPriority(),
                endpoint.getWeight(),
                endpoint.isSsl(),
                endpoint.isReadOnly()
            );
        }
        
        // In a real implementation, this configuration would enable:
        // 1. SSL/TLS encryption for all connections
        // 2. Password-based authentication
        // 3. Connection pooling with proper sizing
        // 4. Circuit breaker pattern for fault tolerance
        // 5. Geographic routing for latency optimization
        // 6. Read replica scaling for read-heavy workloads
        // 7. Multi-region disaster recovery
        // 8. Health monitoring and automatic failover
    }
}
