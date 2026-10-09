// Package ingest owns the Jetstream WebSocket client: connect, decode-once,
// and (in Phase 2) publish. The bounded capture in cmd/capture reuses the
// exact same decode path so the fixture and the live pipeline can't drift.
package ingest

import "encoding/json"

// Event is the lightweight Jetstream envelope we decode once. Fields are the
// union of what commit/identity/account frames carry; we keep only what
// routing needs plus the fields the analysis step (throughput.md) depends on.
type Event struct {
	DID    string  `json:"did"`
	TimeUS int64   `json:"time_us"`
	Kind   string  `json:"kind"` // commit | identity | account
	Commit *Commit `json:"commit,omitempty"`
	// identity/account frames carry a seq (monotonic per-{did,resolver}) and
	// zero or more identity fields; we don't need them for routing.
	Seq int64 `json:"seq,omitempty"`
}

// Commit is the nested commit payload. Delete commits carry NO Record/CID —
// only Operation + Collection + RKey — so Record stays nil for them.
type Commit struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"` // create | update | delete
	Collection string          `json:"collection"`
	RKey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record,omitempty"`
	CID        string          `json:"cid,omitempty"`
}

func (c *Commit) IsDelete() bool { return c != nil && c.Operation == "delete" }

// DedupeKey returns the key that identifies one event, used for cursor resume
// (Jetstream re-delivers the inclusive cursor event) and server-side MsgId
// dedupe. For commit events the natural identity is
//
//	did | collection | operation | rkey | rev
//
// i.e. the full commit identity; seq is only guaranteed monotonic for
// identity/account, and even then per-{did, resolver}, so we don't rely on it
// as a global dedupe key.
func (e *Event) DedupeKey() string {
	if e.Commit == nil {
		// identity/account: seq is the documented monotonic cursor there.
		return "idn:" + e.DID + ":" + i64s(e.Seq)
	}
	c := e.Commit
	return c.Collection + "|" + c.RKey + "|" + c.Operation + "|" + e.DID
}

func i64s(v int64) string {
	// small, dependency-free int->dec
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
