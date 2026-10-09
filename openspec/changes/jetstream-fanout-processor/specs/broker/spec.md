## Purpose

The fan-out fabric: a NATS JetStream stream over `events.*` subjects that durably buffers classified events with server-side dedupe, per-type retention, and a dead-letter path per type so a stuck worker never blocks the others.

## ADDED Requirements

### Requirement: Stream topology
The broker SHALL create and own a JetStream stream named `EVENTS` covering subjects `events.*`, with file storage, `MaxAge` matching a documented replay window, and discard-oldest retention, so every published event survives until a worker acks it.

#### Scenario: Published event is retrievable by its subject
- **WHEN** a classified event is published to `events.post`
- **THEN** it is durably stored under the `EVENTS` stream and a consumer on `events.post` can retrieve it

### Requirement: Server-side publish dedupe
The broker SHALL set `Nats-Msg-Id` to the event dedupe key on publish so Jetstream discards duplicates server-side even under redelivery or resume races.

#### Scenario: Duplicate publish is discarded
- **WHEN** an event with a previously seen `Nats-Msg-Id` is published again
- **THEN** Jetstream drops the duplicate and no worker receives the event twice

### Requirement: Per-type dead-letter path
The broker SHALL route messages that exhaust delivery attempts (after a configured `MaxDeliver`) to the matching `events.<type>.dlq` subject instead of dropping them.

#### Scenario: Exhausted delivery lands in DLQ
- **WHEN** a worker fails a message `MaxDeliver` times
- **THEN** the message is moved to `events.<type>.dlq` and is no longer delivered to the worker

### Requirement: Queued-depth observability
The broker SHALL expose per-subject queue depth (pending message count) so a worker deployment can scale on backpressure (HPA input) and operators can see a stuck type.

#### Scenario: Depth is readable per type
- **WHEN** events accumulate faster than a worker consumes
- **THEN** the per-subject pending count increases and is readable via the brokerage metrics/stats surface
