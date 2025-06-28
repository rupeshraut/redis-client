# Copilot Instructions for Redis Multi-Datacenter Client Library

<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

## Project Overview
This is a modern Java Gradle-based Redis client library using Lettuce that supports synchronous, asynchronous, and reactive programming models for multi-datacenter deployments.

## Core Architecture Principles
- **Multi-Datacenter Awareness**: All components should be designed with datacenter locality and routing in mind
- **Programming Model Support**: Provide sync, async, and reactive APIs consistently across all features
- **Enterprise-Grade**: Focus on resilience, observability, and scalability
- **Lettuce Integration**: Build upon Lettuce Redis client as the foundation

## Key Components
1. **DatacenterRouter**: Handles intelligent routing based on proximity and availability
2. **MultiDatacenterClient**: Main client interface with sync/async/reactive variants
3. **DataLocalityManager**: Manages read/write operations targeting local or remote datacenters
4. **TombstoneKeyManager**: Handles soft deletion, cache invalidation, and distributed locks
5. **ObservabilityProvider**: Metrics, monitoring, and health checks
6. **ReplicationManager**: Handles CRDB scenarios and conflict resolution

## Code Style Guidelines
- Use Java 17+ features appropriately
- Follow reactive programming patterns for reactive APIs
- Implement proper exception handling and circuit breaker patterns
- Include comprehensive logging and metrics
- Use builder patterns for complex configuration objects
- Implement proper resource management with try-with-resources

## Testing Strategy
- Unit tests for all core components
- Integration tests with embedded Redis
- Performance tests for latency and throughput
- Chaos engineering tests for resilience validation

## Dependencies
- Lettuce (Redis client)
- Spring Boot (for auto-configuration)
- Micrometer (metrics)
- SLF4J (logging)
- Project Reactor (reactive streams)
- Jackson (JSON serialization)
