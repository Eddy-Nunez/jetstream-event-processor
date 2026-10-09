## Purpose

Cluster topology and operations: manifests that deploy the ingest and per-type workers to local k3s with NATS JetStream as shared durable storage, per-worker scaling and fail isolation, plus an optional LocalStack story scoped to exactly one demonstration worker.

## ADDED Requirements

### Requirement: Ingest runs single-replica
The ingest deployment SHALL run exactly one replica, because the ordered WS cursor cannot be cleanly sharded across pods; scaling ingest horizontally is out of scope.

#### Scenario: One ingest pod owns the cursor
- **WHEN** the ingest deployment is applied
- **THEN** it runs one replica, and the cursor file/state lives with that pod, so resume correctness does not depend on cross-pod coordination

### Requirement: Per-type worker isolation
Each worker type SHALL be its own Deployment with its own HPA (scaling on its own JetStream queue depth), resource limits, and DLQ subject, so a stuck or saturated worker of one type cannot block another type's progress.

#### Scenario: One type fails, others keep flowing
- **WHEN** the worker for one type is killed or saturated
- **THEN** the other worker deployments continue consuming and processing their own subjects, and the failed type's messages accumulate to its DLQ, not to shared storage that blocks others

### Requirement: Durable broker and optional state stores
The NATS cluster SHALL run as a StatefulSet with a PVC so buffered events survive restarts; LocalStack (posts→SQS one-type sink) SHALL be explicitly optional and documented, with the core routing demo requiring only NATS.

#### Scenario: NATS survives pod restart
- **WHEN** the NATS pod restarts
- **THEN** buffered events on `events.*` are still available from the PVC and workers resume without data loss

#### Scenario: Optional stores are additive
- **WHEN** the cluster is deployed without LocalStack
- **THEN** the routing demo still works end-to-end; only the follows-burst state store and the SQS sink features are unavailable

### Requirement: AWS-parity story is documented, not outsourced
The routing decision SHALL remain in Go; any AWS integration (SQS/EventBridge) SHALL be documented in DESIGN.md as the production path and demonstrated optionally for ONE type (posts notifications), never used as the router.

#### Scenario: Documented AWS path without runtime coupling
- **WHEN** the deployment lacks AWS credentials
- **THEN** the system runs fully on local k3s, and DESIGN.md still describes how the posts-notification sink maps to SQS/EventBridge in production
