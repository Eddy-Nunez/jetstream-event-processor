package ingest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
)

// splitRaw returns the raw bytes of every top-level JSON object in a WS
// payload, handling both "whole payload is one object" and newline-delimited
// batched frames. It is the verbatim counterpart of splitEvents: the bounded
// capture writes these bytes straight to the fixture, so the fixture is exactly
// what Jetstream sent — no field loss, no re-marshal — and the live pipeline
// decodes the *same* framing logic.
func splitRaw(data []byte) []json.RawMessage {
	t := bytes.TrimSpace(data)
	if len(t) == 0 {
		return nil
	}
	// Fast path: whole payload is one object.
	var probe map[string]json.RawMessage
	if json.Unmarshal(t, &probe) == nil {
		return []json.RawMessage{t}
	}
	// Slow path: newline-delimited batch.
	var out []json.RawMessage
	sc := bufio.NewScanner(bytes.NewReader(t))
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(line, &m) == nil {
			out = append(out, line)
		}
	}
	return out
}

// NextRaw returns the raw bytes of every top-level JSON object carried by the
// next WS text frame (one object, or several newline-delimited ones). It is
// what the bounded capture writes, verbatim, to the fixture.
func (c *Conn) NextRaw(ctx context.Context) ([]json.RawMessage, error) {
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return nil, err
		}
		raw := splitRaw(data)
		if len(raw) == 0 {
			continue
		}
		return raw, nil
	}
}
