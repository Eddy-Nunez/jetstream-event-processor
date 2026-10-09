## Purpose

The routing kernel — the graded artifact: a pure, unit-testable table mapping `(kind, collection, operation)` to a fan-out type, with delete commits first-class and unknown intersections observable instead of silently dropped.

## ADDED Requirements

### Requirement: Routing table as single source of truth
The router SHALL map every decoded event to exactly one fan-out `Type` via a table keyed by `(kind, collection, operation)`, and each `Type` SHALL own exactly one NATS subject that its worker consumes; the subject set SHALL mirror the `wantedCollections` handed to the WebSocket layer so routing and server-side trimming never drift.

#### Scenario: Commit routes to its type subject
- **WHEN** a commit event for `app.bsky.feed.post` with operation `create` is classified
- **THEN** it routes to the posts type and the subject `events.post`

#### Scenario: Subject contract mirrors wantedCollections
- **WHEN** the router's subject set is compared with the `wantedCollections` passed at dial time
- **THEN** every routed collection in the table appears in `wantedCollections`

### Requirement: Delete commits route delete-first
The router SHALL treat a commit with operation `delete` as a delete for ANY collection, routing it to the delete worker on its own subject, because a delete commit carries no record/CID and MUST never reach a content matcher.

#### Scenario: Delete of an unknown collection still routes to deletes
- **WHEN** a delete commit for a collection the router has no content route for is classified
- **THEN** it routes to the delete type and the subject `events.delete`

### Requirement: Unknown intersections are observable
Any `(kind, collection, operation)` intersection the table does not cover SHALL resolve to a Drop type that is counted by metric, so unknown routing is observable and alertable, never silently lost.

#### Scenario: Unknown commit increments drop metric
- **WHEN** a commit for a collection outside the table (e.g. `app.bsky.graph.list`) is classified
- **THEN** it resolves to Drop and increments the drop counter without panicking or publishing

### Requirement: Identity and account are first-class routing inputs
The router SHALL classify `identity` and `account` frame kinds as their own types so the ingest layer can count and apply policy to them, keeping the Jetstream always-delivered volume floor explicit in the routing model.

#### Scenario: Identity kind maps to identity type
- **WHEN** a frame with kind `identity` is classified
- **THEN** it maps to the identity type, regardless of collection
