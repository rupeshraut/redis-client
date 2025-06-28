package com.redis.multidc.routing;

import com.redis.multidc.config.DatacenterConfiguration;
import com.redis.multidc.config.DatacenterEndpoint;
import com.redis.multidc.config.RoutingStrategy;
import com.redis.multidc.model.DatacenterInfo;
import com.redis.multidc.model.DatacenterPreference;
import com.redis.multidc.observability.MetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Routes requests to appropriate datacenters based on configuration,
 * health status, latency, and routing strategy.
 */
public class DatacenterRouter {
    
    private static final Logger logger = LoggerFactory.getLogger(DatacenterRouter.class);
    
    private final DatacenterConfiguration configuration;
    private final MetricsCollector metricsCollector;
    private final ConcurrentHashMap<String, DatacenterInfo> datacenterInfoMap;
    private volatile long lastRoutingRefresh = 0;
    
    public DatacenterRouter(DatacenterConfiguration configuration, MetricsCollector metricsCollector) {
        this.configuration = configuration;
        this.metricsCollector = metricsCollector;
        this.datacenterInfoMap = new ConcurrentHashMap<>();
        
        // Initialize datacenter info
        initializeDatacenterInfo();
    }
    
    private void initializeDatacenterInfo() {
        for (DatacenterEndpoint endpoint : configuration.getDatacenters()) {
            DatacenterInfo info = DatacenterInfo.builder()
                .id(endpoint.getId())
                .region(endpoint.getRegion())
                .host(endpoint.getHost())
                .port(endpoint.getPort())
                .local(endpoint.getId().equals(configuration.getLocalDatacenterId()))
                .healthy(true) // Will be updated by health monitor
                .build();
            
            datacenterInfoMap.put(endpoint.getId(), info);
        }
    }
    
    /**
     * Select the best datacenter for a read operation.
     */
    public Optional<String> selectDatacenterForRead(DatacenterPreference preference) {
        return selectDatacenter(preference, true);
    }
    
    /**
     * Select the best datacenter for a write operation.
     */
    public Optional<String> selectDatacenterForWrite(DatacenterPreference preference) {
        return selectDatacenter(preference, false);
    }
    
    private Optional<String> selectDatacenter(DatacenterPreference preference, boolean isRead) {
        List<DatacenterInfo> availableDatacenters = getAvailableDatacenters();
        
        if (availableDatacenters.isEmpty()) {
            logger.warn("No available datacenters found");
            return Optional.empty();
        }
        
        return switch (preference) {
            case LOCAL_ONLY -> selectLocalOnly(availableDatacenters);
            case LOCAL_PREFERRED -> selectLocalPreferred(availableDatacenters);
            case ANY_AVAILABLE -> selectAnyAvailable(availableDatacenters, isRead);
            case LOWEST_LATENCY -> selectLowestLatency(availableDatacenters);
            case REMOTE_ONLY -> selectRemoteOnly(availableDatacenters);
            case SPECIFIC_DATACENTER -> selectSpecificDatacenter(availableDatacenters);
        };
    }
    
    private Optional<String> selectLocalOnly(List<DatacenterInfo> availableDatacenters) {
        return availableDatacenters.stream()
            .filter(DatacenterInfo::isLocal)
            .map(DatacenterInfo::getId)
            .findFirst();
    }
    
    private Optional<String> selectLocalPreferred(List<DatacenterInfo> availableDatacenters) {
        // Try local first
        Optional<String> local = selectLocalOnly(availableDatacenters);
        if (local.isPresent()) {
            return local;
        }
        
        // Fallback to any available
        return selectAnyAvailable(availableDatacenters, true);
    }
    
    private Optional<String> selectAnyAvailable(List<DatacenterInfo> availableDatacenters, boolean isRead) {
        RoutingStrategy strategy = configuration.getRoutingStrategy();
        
        return switch (strategy) {
            case LOCAL_ONLY -> selectLocalOnly(availableDatacenters);
            case LATENCY_BASED -> selectLowestLatency(availableDatacenters);
            case PRIORITY_BASED -> selectByPriority(availableDatacenters);
            case WEIGHTED_ROUND_ROBIN -> selectByWeight(availableDatacenters);
            case LOCAL_WRITE_ANY_READ -> isRead ? selectAnyHealthy(availableDatacenters) : selectLocalOnly(availableDatacenters);
            case CUSTOM -> selectByCustomLogic(availableDatacenters);
        };
    }
    
    private Optional<String> selectLowestLatency(List<DatacenterInfo> availableDatacenters) {
        return availableDatacenters.stream()
            .min((a, b) -> Long.compare(a.getLatencyMs(), b.getLatencyMs()))
            .map(DatacenterInfo::getId);
    }
    
    private Optional<String> selectRemoteOnly(List<DatacenterInfo> availableDatacenters) {
        return availableDatacenters.stream()
            .filter(dc -> !dc.isLocal())
            .min((a, b) -> Long.compare(a.getLatencyMs(), b.getLatencyMs()))
            .map(DatacenterInfo::getId);
    }
    
    private Optional<String> selectSpecificDatacenter(List<DatacenterInfo> availableDatacenters) {
        // This would need additional context about which specific datacenter is requested
        // For now, fallback to local preferred
        return selectLocalPreferred(availableDatacenters);
    }
    
    private Optional<String> selectByPriority(List<DatacenterInfo> availableDatacenters) {
        // Priority is stored in the endpoint configuration
        return availableDatacenters.stream()
            .max((a, b) -> {
                DatacenterEndpoint endpointA = getEndpointById(a.getId());
                DatacenterEndpoint endpointB = getEndpointById(b.getId());
                return Integer.compare(endpointA.getPriority(), endpointB.getPriority());
            })
            .map(DatacenterInfo::getId);
    }
    
    private Optional<String> selectByWeight(List<DatacenterInfo> availableDatacenters) {
        // Weighted round-robin based on datacenter weights
        double totalWeight = availableDatacenters.stream()
            .mapToDouble(dc -> getEndpointById(dc.getId()).getWeight())
            .sum();
        
        if (totalWeight <= 0) {
            return selectAnyHealthy(availableDatacenters);
        }
        
        double random = ThreadLocalRandom.current().nextDouble(totalWeight);
        double currentWeight = 0;
        
        for (DatacenterInfo dc : availableDatacenters) {
            currentWeight += getEndpointById(dc.getId()).getWeight();
            if (random <= currentWeight) {
                return Optional.of(dc.getId());
            }
        }
        
        return selectAnyHealthy(availableDatacenters);
    }
    
    private Optional<String> selectByCustomLogic(List<DatacenterInfo> availableDatacenters) {
        // Custom routing logic would be pluggable here
        // For now, fallback to latency-based
        return selectLowestLatency(availableDatacenters);
    }
    
    private Optional<String> selectAnyHealthy(List<DatacenterInfo> availableDatacenters) {
        if (availableDatacenters.isEmpty()) {
            return Optional.empty();
        }
        
        // Random selection from available datacenters
        int index = ThreadLocalRandom.current().nextInt(availableDatacenters.size());
        return Optional.of(availableDatacenters.get(index).getId());
    }
    
    private List<DatacenterInfo> getAvailableDatacenters() {
        return datacenterInfoMap.values().stream()
            .filter(DatacenterInfo::isAvailable)
            .collect(Collectors.toList());
    }
    
    private DatacenterEndpoint getEndpointById(String datacenterId) {
        return configuration.getDatacenters().stream()
            .filter(endpoint -> endpoint.getId().equals(datacenterId))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown datacenter: " + datacenterId));
    }
    
    public List<DatacenterInfo> getAllDatacenterInfo() {
        return List.copyOf(datacenterInfoMap.values());
    }
    
    public DatacenterInfo getLocalDatacenter() {
        return datacenterInfoMap.values().stream()
            .filter(DatacenterInfo::isLocal)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Local datacenter not found"));
    }
    
    public void updateDatacenterInfo(DatacenterInfo info) {
        datacenterInfoMap.put(info.getId(), info);
        metricsCollector.recordDatacenterUpdate(info);
    }
    
    public void refreshRoutingInfo() {
        lastRoutingRefresh = System.currentTimeMillis();
        logger.debug("Routing information refreshed at {}", Instant.now());
    }
    
    public Optional<DatacenterInfo> getDatacenterInfo(String datacenterId) {
        return Optional.ofNullable(datacenterInfoMap.get(datacenterId));
    }
}
