// Package ingest owns the Jetstream WebSocket client: connect, decode-once,
// and (in Phase 2) publish. The bounded capture in cmd/capture reuses the
// exact same decode path so the fixture and the live pipeline can't drift.
package ingest

import "encoding/json"

// Event is the lightweight Jetstream envelope we decode once. Fields are the
// union of what commit/identity/account frames carry; we keep only what
// routing needs plus the fields the analysis step (throughput.md) depends on.
//
// Fixture-verified framing (throughput.md §4): there is NO top-level `seq` on
// any frame; identity/account carry seq NESTED inside their own object, and
// commits carry no seq at all. time_us is present on every frame and is the
// global resume cursor.
type Event struct {
	DID     string   `json:"did"`
	TimeUS  int64    `json:"time_us"`
	Kind    string   `json:"kind"` // commit | identity | account
	Commit  *Commit  `json:"commit,omitempty"`
	Identity *IDFrame `json:"identity,omitempty"`
	Account  *AcctFrame `json:"account,omitempty"`
}

// IDFrame is the nested identity object. seq is the monotonic per-{did,
// resolver} cursor used to dedupe identity frames.
type IDFrame struct {
	DID  string `json:"did"`
	Seq  int64  `json:"seq"`
	Time string `json:"time"`
}

// AcctFrame is the nested account object; Active/Status describe the account.
type AcctFrame struct {
	Active bool   `json:"active"`
	DID    string `json:"did"`
	Seq    int64  `json:"seq"`
	Status string `json:"status,omitempty"`
	Time   string `json:"time"`
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
// dedupe. For commit events the identity is
//
//	did | collection | rkey | operation | rev
//
// (fixture-verified: did|collection|rkey|operation was already unique in the
// 42,186-event sample; rev covers the update-bumps on a stable rkey). The
// identity/account nested seq is the documented monotonic cursor there.
func (e *Event) DedupeKey() string {
	if c := e.Commit; c != nil {
		return e.DID + "|" + c.Collection + "|" + c.RKey + "|" + c.Operation + "|" + c.Rev
	}
	if idf := e.Identity; idf != nil {
		return "idn:" + idf.DID + ":" + i64s(idf.Seq)
	}
	if acf := e.Account; acf != nil {
		return "acct:" + acf.DID + ":" + i64s(acf.Seq)
	}
	return "unknown:" + e.Kind + ":" + e.DID
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
