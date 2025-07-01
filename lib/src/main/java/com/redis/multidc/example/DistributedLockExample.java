package com.redis.multidc.example;

import com.redis.multidc.MultiDatacenterRedisClient;
import com.redis.multidc.MultiDatacenterRedisClientBuilder;
import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.operations.AsyncOperations;
import com.redis.multidc.operations.ReactiveOperations;
import com.redis.multidc.operations.SyncOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive example demonstrating distributed locking scenarios
 * with the Redis Multi-Datacenter Client.
 * 
 * Features demonstrated:
 * - Basic distributed locks
 * - Lock timeout and renewal
 * - Lock contention scenarios
 * - Cross-datacenter locking strategies
 * - Lock safety and recovery
 * - High-concurrency locking patterns
 * - Lock monitoring and metrics
 */
public class DistributedLockExample {
    
    private static final Logger logger = LoggerFactory.getLogger(DistributedLockExample.class);
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
    private final AtomicInteger lockAttempts = new AtomicInteger(0);
    private final AtomicInteger lockSuccesses = new AtomicInteger(0);
    private final AtomicInteger lockFailures = new AtomicInteger(0);
    
    public static void main(String[] args) {
        DistributedLockExample example = new DistributedLockExample();
        try {
            example.runExample();
        } finally {
            example.shutdown();
        }
    }
    
    public void runExample() {
        logger.info("Starting Distributed Lock Example");
        
        // Configure datacenters
        DatacenterConfiguration config = DatacenterConfiguration.builder()
            .localDatacenter("us-east-1")
            .datacenters(List.of(
                DatacenterEndpoint.builder()
                    .id("us-east-1")
                    .host("localhost")
                    .port(6379)
                    .build(),
                DatacenterEndpoint.builder()
                    .id("us-west-2")
                    .host("localhost")
                    .port(6380)
                    .build()
            ))
            .build();
        
        // Create client
        try (MultiDatacenterRedisClient client = MultiDatacenterRedisClientBuilder.create(config)) {
            
            // Demonstrate various locking scenarios
            demonstrateBasicLocking(client.sync());
            demonstrateAsyncLocking(client.async());
            demonstrateReactiveLocking(client.reactive());
            demonstrateLockContention(client);
            demonstrateCrossDatacenterLocking(client);
            demonstrateLockRenewal(client);
            demonstrateLockSafetyAndRecovery(client);
            demonstrateHighConcurrencyLocking(client);
            
            // Report final statistics
            reportLockingStatistics();
            
            logger.info("Distributed Lock Example completed successfully");
            
        } catch (Exception e) {
            logger.error("Error in distributed lock example", e);
        }
    }
    
    private void demonstrateBasicLocking(SyncOperations sync) {
        logger.info("=== Basic Distributed Locking ===");
        
        String lockKey = "lock:basic:resource";
        String lockValue = "owner_" + Thread.currentThread().getId() + "_" + System.currentTimeMillis();
        Duration lockTimeout = Duration.ofSeconds(10);
        
        // Acquire lock
        boolean acquired = sync.acquireLock(lockKey, lockValue, lockTimeout);
        lockAttempts.incrementAndGet();
        
        if (acquired) {
            lockSuccesses.incrementAndGet();
            logger.info("✅ Lock acquired: {} = {}", lockKey, lockValue);
            
            try {
                // Simulate critical section work
                logger.info("🔒 Performing critical section work...");
                Thread.sleep(2000);
                
                // Check if lock is still held
                boolean stillLocked = sync.isLocked(lockKey);
                logger.info("Lock status check: {}", stillLocked ? "STILL_LOCKED" : "LOST");
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Critical section interrupted");
            } finally {
                // Release lock
                boolean released = sync.releaseLock(lockKey, lockValue);
                if (released) {
                    logger.info("🔓 Lock released successfully: {}", lockKey);
                } else {
                    logger.warn("⚠️ Failed to release lock (may have expired): {}", lockKey);
                }
            }
        } else {
            lockFailures.incrementAndGet();
            logger.warn("❌ Failed to acquire lock: {}", lockKey);
        }
    }
    
    private void demonstrateAsyncLocking(AsyncOperations async) {
        logger.info("=== Asynchronous Distributed Locking ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        String lockKey = "lock:async:resource";
        String lockValue = "async_owner_" + System.currentTimeMillis();
        Duration lockTimeout = Duration.ofSeconds(8);
        
        lockAttempts.incrementAndGet();
        
        async.acquireLock(lockKey, lockValue, lockTimeout)
            .thenCompose(acquired -> {
                if (acquired) {
                    lockSuccesses.incrementAndGet();
                    logger.info("✅ Async lock acquired: {} = {}", lockKey, lockValue);
                    
                    // Simulate async work
                    return CompletableFuture.runAsync(() -> {
                        try {
                            logger.info("🔒 Performing async critical section work...");
                            Thread.sleep(1500);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }).thenCompose(v -> async.isLocked(lockKey))
                    .thenCompose(stillLocked -> {
                        logger.info("Async lock status: {}", stillLocked ? "STILL_LOCKED" : "LOST");
                        return async.releaseLock(lockKey, lockValue);
                    });
                } else {
                    lockFailures.incrementAndGet();
                    logger.warn("❌ Failed to acquire async lock: {}", lockKey);
                    return CompletableFuture.completedFuture(false);
                }
            })
            .thenAccept(released -> {
                if (released) {
                    logger.info("🔓 Async lock released successfully: {}", lockKey);
                } else {
                    logger.warn("⚠️ Failed to release async lock: {}", lockKey);
                }
                latch.countDown();
            })
            .exceptionally(throwable -> {
                logger.error("Error in async locking", throwable);
                lockFailures.incrementAndGet();
                latch.countDown();
                return null;
            });
        
        await(latch);
    }
    
    private void demonstrateReactiveLocking(ReactiveOperations reactive) {
        logger.info("=== Reactive Distributed Locking ===");
        
        CountDownLatch latch = new CountDownLatch(1);
        
        String lockKey = "lock:reactive:resource";
        String lockValue = "reactive_owner_" + System.currentTimeMillis();
        Duration lockTimeout = Duration.ofSeconds(12);
        
        lockAttempts.incrementAndGet();
        
        reactive.acquireLock(lockKey, lockValue, lockTimeout)
            .flatMap(acquired -> {
                if (acquired) {
                    lockSuccesses.incrementAndGet();
                    logger.info("✅ Reactive lock acquired: {} = {}", lockKey, lockValue);
                    
                    // Simulate reactive work with automatic cleanup
                    return Mono.delay(Duration.ofSeconds(2))
                        .doOnNext(v -> logger.info("🔒 Performing reactive critical section work..."))
                        .then(reactive.isLocked(lockKey))
                        .doOnNext(stillLocked -> logger.info("Reactive lock status: {}", stillLocked ? "STILL_LOCKED" : "LOST"))
                        .then(reactive.releaseLock(lockKey, lockValue));
                } else {
                    lockFailures.incrementAndGet();
                    logger.warn("❌ Failed to acquire reactive lock: {}", lockKey);
                    return Mono.just(false);
                }
            })
            .doOnNext(released -> {
                if (released) {
                    logger.info("🔓 Reactive lock released successfully: {}", lockKey);
                } else {
                    logger.warn("⚠️ Failed to release reactive lock: {}", lockKey);
                }
            })
            .doOnSuccess(v -> latch.countDown())
            .doOnError(error -> {
                logger.error("Error in reactive locking", error);
                lockFailures.incrementAndGet();
                latch.countDown();
            })
            .subscribe();
        
        await(latch);
    }
    
    private void demonstrateLockContention(MultiDatacenterRedisClient client) {
        logger.info("=== Lock Contention Scenarios ===");
        
        String contentionLockKey = "lock:contention:resource";
        Duration lockTimeout = Duration.ofSeconds(5);
        int numCompetitors = 8;
        CountDownLatch contentionLatch = new CountDownLatch(numCompetitors);
        
        // Launch multiple competitors for the same lock
        for (int i = 1; i <= numCompetitors; i++) {
            final int competitorId = i;
            final String lockValue = "competitor_" + competitorId + "_" + System.currentTimeMillis();
            
            scheduler.submit(() -> {
                try {
                    lockAttempts.incrementAndGet();
                    
                    boolean acquired = client.sync().acquireLock(contentionLockKey, lockValue, lockTimeout);
                    
                    if (acquired) {
                        lockSuccesses.incrementAndGet();
                        logger.info("🏆 Competitor {} WON the lock: {}", competitorId, lockValue);
                        
                        // Hold the lock briefly
                        Thread.sleep(1000);
                        
                        boolean released = client.sync().releaseLock(contentionLockKey, lockValue);
                        if (released) {
                            logger.info("🔓 Competitor {} released lock", competitorId);
                        } else {
                            logger.warn("⚠️ Competitor {} failed to release lock", competitorId);
                        }
                    } else {
                        lockFailures.incrementAndGet();
                        logger.info("🚫 Competitor {} LOST the contention", competitorId);
                    }
                } catch (Exception e) {
                    logger.error("Competitor {} encountered error", competitorId, e);
                    lockFailures.incrementAndGet();
                } finally {
                    contentionLatch.countDown();
                }
            });
        }
        
        await(contentionLatch);
        
        // Check final lock state
        boolean finalLockState = client.sync().isLocked(contentionLockKey);
        logger.info("Final lock state for contention resource: {}", finalLockState ? "LOCKED" : "UNLOCKED");
    }
    
    private void demonstrateCrossDatacenterLocking(MultiDatacenterRedisClient client) {
        logger.info("=== Cross-Datacenter Locking ===");
        
        SyncOperations sync = client.sync();
        
        // Test locking in different datacenters
        String crossDcLockKey = "lock:crossdc:resource";
        String localLockValue = "local_owner_" + System.currentTimeMillis();
        String remoteLockValue = "remote_owner_" + System.currentTimeMillis();
        Duration lockTimeout = Duration.ofSeconds(10);
        
        // Try to acquire lock in local datacenter
        lockAttempts.incrementAndGet();
        boolean localAcquired = sync.acquireLock(crossDcLockKey, localLockValue, lockTimeout, DatacenterPreference.LOCAL_ONLY);
        
        if (localAcquired) {
            lockSuccesses.incrementAndGet();
            logger.info("✅ LOCAL datacenter lock acquired: {} = {}", crossDcLockKey, localLockValue);
            
            // Try to acquire the same lock in remote datacenter (should fail if properly implemented)
            lockAttempts.incrementAndGet();
            boolean remoteAcquired = sync.acquireLock(crossDcLockKey, remoteLockValue, lockTimeout, DatacenterPreference.REMOTE_ONLY);
            
            if (remoteAcquired) {
                lockSuccesses.incrementAndGet();
                logger.warn("⚠️ REMOTE datacenter lock also acquired - potential consistency issue!");
                sync.releaseLock(crossDcLockKey, remoteLockValue, DatacenterPreference.REMOTE_ONLY);
            } else {
                lockFailures.incrementAndGet();
                logger.info("✅ REMOTE datacenter correctly rejected lock acquisition");
            }
            
            // Release local lock
            boolean localReleased = sync.releaseLock(crossDcLockKey, localLockValue, DatacenterPreference.LOCAL_ONLY);
            logger.info("🔓 LOCAL lock released: {}", localReleased);
            
        } else {
            lockFailures.incrementAndGet();
            logger.warn("❌ Failed to acquire LOCAL datacenter lock");
        }
        
        // Now try remote first
        lockAttempts.incrementAndGet();
        boolean remoteFirst = sync.acquireLock(crossDcLockKey, remoteLockValue, lockTimeout, DatacenterPreference.REMOTE_ONLY);
        
        if (remoteFirst) {
            lockSuccesses.incrementAndGet();
            logger.info("✅ REMOTE datacenter lock acquired first: {} = {}", crossDcLockKey, remoteLockValue);
            
            // Brief work simulation
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            boolean remoteReleased = sync.releaseLock(crossDcLockKey, remoteLockValue, DatacenterPreference.REMOTE_ONLY);
            logger.info("🔓 REMOTE lock released: {}", remoteReleased);
        } else {
            lockFailures.incrementAndGet();
            logger.warn("❌ Failed to acquire REMOTE datacenter lock");
        }
    }
    
    private void demonstrateLockRenewal(MultiDatacenterRedisClient client) {
        logger.info("=== Lock Renewal Scenarios ===");
        
        ReactiveOperations reactive = client.reactive();
        CountDownLatch renewalLatch = new CountDownLatch(1);
        
        String renewalLockKey = "lock:renewal:resource";
        String lockValue = "renewal_owner_" + System.currentTimeMillis();
        Duration initialTimeout = Duration.ofSeconds(6);
        
        lockAttempts.incrementAndGet();
        
        reactive.acquireLock(renewalLockKey, lockValue, initialTimeout)
            .flatMap(acquired -> {
                if (acquired) {
                    lockSuccesses.incrementAndGet();
                    logger.info("✅ Lock acquired for renewal test: {} = {}", renewalLockKey, lockValue);
                    
                    // Simulate long-running work with lock renewal
                    return Flux.interval(Duration.ofSeconds(2))
                        .take(5) // Renew 5 times over 10 seconds
                        .flatMap(tick -> {
                            logger.info("🔄 Renewing lock (attempt {})...", tick + 1);
                            // Renewal is essentially re-acquiring the lock with the same value
                            return reactive.acquireLock(renewalLockKey, lockValue, Duration.ofSeconds(6))
                                .doOnNext(renewed -> {
                                    if (renewed) {
                                        logger.info("✅ Lock renewed successfully (tick {})", tick + 1);
                                    } else {
                                        logger.warn("⚠️ Lock renewal failed (tick {})", tick + 1);
                                    }
                                });
                        })
                        .then(reactive.releaseLock(renewalLockKey, lockValue));
                } else {
                    lockFailures.incrementAndGet();
                    logger.warn("❌ Failed to acquire lock for renewal test");
                    return Mono.just(false);
                }
            })
            .doOnNext(released -> {
                if (released) {
                    logger.info("🔓 Renewal test lock released successfully");
                } else {
                    logger.warn("⚠️ Failed to release renewal test lock");
                }
            })
            .doOnSuccess(v -> renewalLatch.countDown())
            .doOnError(error -> {
                logger.error("Error in lock renewal test", error);
                renewalLatch.countDown();
            })
            .subscribe();
        
        await(renewalLatch);
    }
    
    private void demonstrateLockSafetyAndRecovery(MultiDatacenterRedisClient client) {
        logger.info("=== Lock Safety and Recovery ===");
        
        SyncOperations sync = client.sync();
        
        String safetyLockKey = "lock:safety:resource";
        String lockValue = "safety_owner_" + System.currentTimeMillis();
        Duration shortTimeout = Duration.ofSeconds(3);
        
        // Acquire lock with short timeout
        lockAttempts.incrementAndGet();
        boolean acquired = sync.acquireLock(safetyLockKey, lockValue, shortTimeout);
        
        if (acquired) {
            lockSuccesses.incrementAndGet();
            logger.info("✅ Safety test lock acquired: {} = {}", safetyLockKey, lockValue);
            
            // Simulate work that takes longer than the lock timeout
            try {
                logger.info("🔒 Simulating long work (5 seconds) that exceeds lock timeout (3 seconds)...");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Check if lock still exists
            boolean stillExists = sync.isLocked(safetyLockKey);
            logger.info("Lock status after timeout: {}", stillExists ? "STILL_EXISTS" : "EXPIRED");
            
            // Try to release (should fail gracefully if lock expired)
            boolean released = sync.releaseLock(safetyLockKey, lockValue);
            if (released) {
                logger.info("🔓 Lock released (unexpectedly still valid)");
            } else {
                logger.info("⏰ Lock release failed - lock had already expired (expected behavior)");
            }
        } else {
            lockFailures.incrementAndGet();
            logger.warn("❌ Failed to acquire safety test lock");
        }
        
        // Demonstrate recovery - another process can now acquire the lock
        String recoveryValue = "recovery_owner_" + System.currentTimeMillis();
        lockAttempts.incrementAndGet();
        boolean recovered = sync.acquireLock(safetyLockKey, recoveryValue, Duration.ofSeconds(5));
        
        if (recovered) {
            lockSuccesses.incrementAndGet();
            logger.info("✅ Recovery successful - new owner acquired lock: {}", recoveryValue);
            sync.releaseLock(safetyLockKey, recoveryValue);
            logger.info("🔓 Recovery lock released");
        } else {
            lockFailures.incrementAndGet();
            logger.warn("❌ Recovery failed - could not acquire lock");
        }
    }
    
    private void demonstrateHighConcurrencyLocking(MultiDatacenterRedisClient client) {
        logger.info("=== High Concurrency Locking ===");
        
        int numThreads = 20;
        int numResources = 5;
        CountDownLatch concurrencyLatch = new CountDownLatch(numThreads);
        
        // Create multiple threads competing for multiple resources
        for (int thread = 1; thread <= numThreads; thread++) {
            final int threadId = thread;
            
            scheduler.submit(() -> {
                try {
                    // Each thread tries to acquire locks for multiple resources
                    for (int resource = 1; resource <= numResources; resource++) {
                        String lockKey = "lock:concurrent:resource:" + resource;
                        String lockValue = "thread_" + threadId + "_resource_" + resource + "_" + System.currentTimeMillis();
                        
                        lockAttempts.incrementAndGet();
                        boolean acquired = client.sync().acquireLock(lockKey, lockValue, Duration.ofSeconds(2));
                        
                        if (acquired) {
                            lockSuccesses.incrementAndGet();
                            logger.debug("Thread {} acquired lock for resource {}", threadId, resource);
                            
                            // Brief work
                            Thread.sleep(100);
                            
                            client.sync().releaseLock(lockKey, lockValue);
                        } else {
                            lockFailures.incrementAndGet();
                            logger.debug("Thread {} failed to acquire lock for resource {}", threadId, resource);
                        }
                        
                        // Small delay between resource attempts
                        Thread.sleep(50);
                    }
                } catch (Exception e) {
                    logger.error("Thread {} encountered error in high concurrency test", threadId, e);
                } finally {
                    concurrencyLatch.countDown();
                }
            });
        }
        
        await(concurrencyLatch);
        
        // Check that all locks are released
        int stillLocked = 0;
        for (int resource = 1; resource <= numResources; resource++) {
            String lockKey = "lock:concurrent:resource:" + resource;
            if (client.sync().isLocked(lockKey)) {
                stillLocked++;
            }
        }
        
        logger.info("High concurrency test completed - {} resources still locked (should be 0)", stillLocked);
    }
    
    private void reportLockingStatistics() {
        logger.info("=== Locking Statistics Report ===");
        logger.info("Total lock attempts: {}", lockAttempts.get());
        logger.info("Successful acquisitions: {}", lockSuccesses.get());
        logger.info("Failed acquisitions: {}", lockFailures.get());
        
        if (lockAttempts.get() > 0) {
            double successRate = (double) lockSuccesses.get() / lockAttempts.get() * 100;
            logger.info("Success rate: {:.1f}%", successRate);
        }
    }
    
    private void await(CountDownLatch latch) {
        try {
            latch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for operation completion");
        }
    }
    
    public void shutdown() {
        logger.info("Shutting down distributed lock example");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
