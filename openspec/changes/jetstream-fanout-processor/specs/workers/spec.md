## Purpose

Independent per-type consumers (posts, likes, reposts, follows, deletes) that pull only from their own subject, apply real per-type downstream logic, and shut down gracefully without losing in-flight work.

## ADDED Requirements

### Requirement: One generic consumer binary, type by configuration
A single worker binary SHALL be deployable per type via configuration (env/flag), consuming only the subject for its configured type and the matching DLQ, so each worker deployment is an independent unit with its own HPA and limits.

#### Scenario: Worker consumes only its own subject
- **WHEN** a worker is configured as type `likes`
- **THEN** it pulls only from `events.like` (and its `events.like.dlq`), never from `events.post` or `events.follow`

### Requirement: Bounded pull concurrency with ack/nak
The worker SHALL pull with a bounded concurrency limit and SHALL ack a message only after its per-type processing commits, naking (with deferral) on transient failure so redelivery backs off.

#### Scenario: Transient failure defers redelivery
- **WHEN** per-type processing fails with a transient error inside the concurrency window
- **THEN** the message is nacked with a delay, not acked, and is redelivered later

### Requirement: Real per-type downstream logic
Each type SHALL implement its own real processing logic, not a no-op stub:
- posts: keyword/language matcher producing an on-hit notification (log line, optional SQS out)
- likes/reposts: in-memory rolling counters with a threshold alert
- follows: sliding-window burst detection per target account (bounded small state)
- deletes: bounded tombstone set with TTL (in-memory)

Orders within a type are commutative for posts/likes/reposts/deletes; follows burst detection is window-based, not strict-order.

#### Scenario: Posts matcher fires on keyword hit
- **WHEN** a post record contains a configured keyword
- **THEN** the worker emits an on-hit notification (log line / optional sink) for that post

#### Scenario: Like counter crosses threshold
- **WHEN** rolling like counts cross the configured threshold inside the window
- **THEN** the worker emits a threshold alert, then resets the rolling count

#### Scenario: Follow burst detected for a target
- **WHEN** follows for one target account exceed the burst rate in the sliding window
- **THEN** the worker emits a burst alert for that target

#### Scenario: Delete records a tombstone
- **WHEN** a delete commit is processed
- **THEN** the worker records the key (`did|collection|rkey`) in the tombstone set with TTL, without ever attempting content matching

### Requirement: Graceful shutdown
A worker SHALL, on termination signal, stop pulling new messages, finish and ack in-flight messages within a configurable grace period, then exit; the ingest process SHALL additionally flush and persist its cursor on shutdown.

#### Scenario: In-flight messages acked before exit
- **WHEN** a worker receives a termination signal while processing messages
- **THEN** it stops new pulls, acks in-flight work inside the grace period, and exits cleanly with no redelivery storm
