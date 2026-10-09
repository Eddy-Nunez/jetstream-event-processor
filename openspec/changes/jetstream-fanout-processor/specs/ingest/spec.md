## Purpose

Ingests the Bluesky Jetstream firehose over a single ordered WebSocket: decode each frame once, classify-and-drop before any broker write, and resume without loss or duplication after disconnects.

## ADDED Requirements

### Requirement: Single ordered WebSocket ingestion
The ingest process SHALL maintain exactly one WebSocket connection to a configurable Jetstream endpoint, reading events in stream order, and SHALL apply the configured `wantedCollections` filter at the connection level while continuing to receive (and later classify) the identity/account frames that Jetstream always delivers.

#### Scenario: Events decoded in stream order
- **WHEN** the ingest process reads a WebSocket frame carrying multiple newline-delimited JSON objects
- **THEN** every top-level JSON object in the frame is decoded and emitted in the order it appeared in the frame

#### Scenario: wantedCollections applied at connection level
- **WHEN** the ingest process opens the connection with `wantedCollections` set to the demo collections
- **THEN** commit events outside those collections are trimmed by the server, while identity and account frames still arrive and reach the classifier

### Requirement: Cursor resume with inclusive-cursor dedupe
The ingest process SHALL persist a cursor (last processed `seq`/`time_us`) and SHALL resume from it on reconnect; because the Jetstream cursor is inclusive, the first event received after resume SHALL be deduped against the persisted cursor so no event is processed twice.

#### Scenario: Clean reconnect resumes without loss
- **WHEN** the connection drops and the ingest process reconnects with the persisted cursor
- **THEN** it resumes from the cursor position, skips the inclusive re-delivered first event, and continues without gaps

#### Scenario: Dedupe key identifies one event
- **WHEN** an event is decoded, its dedupe key is derived from its commit identity (`did|collection|operation|rkey|rev`, or `seq` for identity/account frames)
- **THEN** the key is stable across redelivery, so the same logical event never publishes twice even when the server re-sends it

### Requirement: Identity and account frames never silently lost
The ingest process SHALL classify every identity/account frame it receives and SHALL apply a configured drop policy (drop-at-publish by default), with a drop counter incremented per frame so the steady identity/account volume floor is observable, not silently discarded.

#### Scenario: Identity frame counted and dropped
- **WHEN** an identity frame is received
- **THEN** it is classified as identity, the drop counter for identity is incremented, and no broker message is published for it unless the policy says otherwise

### Requirement: Fixture-fidelity framing
The live ingest decode path SHALL reuse the same frame-splitting logic as the bounded capture tool so `testdata/sample.jsonl` (the committed fixture) and the live pipeline see byte-identical framing, keeping the fixture valid as a resume and integration-test input.

#### Scenario: Fixture and live pipeline share framing
- **WHEN** the integration test feeds a line from `testdata/sample.jsonl` through the framing logic
- **THEN** the decoded events match what the live pipeline would decode from the identical raw bytes

### Requirement: Decode-once with malformed-frame tolerance
The ingest process SHALL decode each raw frame exactly once and SHALL skip a malformed JSON object without killing the stream or corrupting the cursor, counting it as an error.

#### Scenario: Malformed object does not stop the stream
- **WHEN** a frame contains one malformed JSON object followed by valid objects
- **THEN** the malformed object is counted as an error, the valid objects are processed normally, and the connection stays up
