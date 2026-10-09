package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	neturl "net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// Jetstream2 global endpoint. Preferred over the regional jetstream.* hosts:
// it is the shared public firehose and its cursor (time_us) is already on the
// global clock, which keeps the resume story identical across regions.
const DefaultURL = "wss://jetstream2.us-east.bsky.network/subscribe"

// Conn is a single, ordered Jetstream WS connection. It deliberately exposes
// only "read the next event" — decoding happens exactly once here, and every
// downstream layer consumes the already-type-shaped Event.
type Conn struct {
	ws   *websocket.Conn
	url  string
	next timeUsClamp // monotonic-time clamp for Jetstream clock skew
}

// timeUsClamp guards against one real failure mode: Jetstream timestamps are
// produced on the server's NTP clock; a single backwards jump (or our own NTP
// step) would otherwise make the cursor roll back and re-deliver. We clamp so
// cursor-only moves forward. Offline analysis does NOT use this (fixture is
// as-captured); it protects the live resume path.
type timeUsClamp struct {
	prev int64
	set  bool
}

// Dial opens the WS connection. wantedCollections trims *commit* events to
// those collections; passing nil/empty keeps the full firehose (collector,
// identity/account/sync still always arrive regardless — we classify-and-drop
// those in Go). The `did` header echoes our identity for server-side logging.
func Dial(ctx context.Context, url string, wantedCollections []string) (*Conn, error) {
	u := url
	if u == "" {
		u = DefaultURL
	}
	sub := strings.TrimSuffix(u, "/")
	if strings.Contains(sub, "/subscribe") {
		// keep path as-is
	} else {
		sub += "/subscribe"
	}
	if len(wantedCollections) > 0 {
		var sb strings.Builder
		for i, c := range wantedCollections {
			if i > 0 {
				sb.WriteByte('&')
			}
			sb.WriteString("wantedCollections=")
			sb.WriteString(neturl.QueryEscape(c))
		}
		sep := "?"
		if strings.Contains(sub, "?") {
			sep = "&"
		}
		sub += sep + sb.String()
	}

	hdr := http.Header{}
	hdr.Set("User-Agent", "eventworker/0.1 (senior-swe-takehome)")
	hdr.Set("Origin", "https://eventworker.local")

	d := websocket.Dialer{
		HandshakeTimeout: 20 * time.Second,
		// We do NOT negotiate permessage-deflate: at the trimmed
		// wantedCollections volume the payload is small and skipping
		// compression keeps the decode path dependency-free. This is a
		// documented Phase-2 option for the full-firehose case.
	}
	ws, resp, err := d.DialContext(ctx, sub, hdr)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial %s: %w (resp %d)", sub, err, resp.StatusCode)
		}
		return nil, fmt.Errorf("dial %s: %w", sub, err)
	}
	return &Conn{ws: ws, url: sub}, nil
}

// NextEvents decodes every JSON object in the next WS text frame (one frame
// may carry one object or several newline-delimited ones). It shares splitRaw
// with the capture path — the fixture and the live pipeline therefore see
// byte-identical framing. Returning the whole frame keeps batched frames loss-
// free; the caller owns ordering (the frame is emitted in stream order). The
// monotonic time clamp applies per event, because the cursor is per event.
func (c *Conn) NextEvents(ctx context.Context) ([]*Event, error) {
	for {
		raw, err := c.NextRaw(ctx)
		if err != nil {
			return nil, err
		}
		var out []*Event
		for _, line := range raw {
			var ev Event
			if err := json.Unmarshal(line, &ev); err != nil {
				continue // one malformed object must not kill the stream
			}
			if ev.TimeUS > 0 {
				ev.TimeUS = c.next.Clamp(ev.TimeUS)
			}
			out = append(out, &ev)
		}
		if len(out) > 0 {
			return out, nil
		}
	}
}

// Close closes the underlying WS connection and any read loop pinned to it.
func (c *Conn) Close() error {
	if c.ws == nil {
		return nil
	}
	return c.ws.Close()
}

// Clamp returns v if it is >= the previous value seen, else the previous value
// (monotone). Mirrors the live-resume cursor safety; never used on fixtures.
func (t *timeUsClamp) Clamp(v int64) int64 {
	if !t.set {
		t.set = true
		t.prev = v
		return v
	}
	if v > t.prev {
		t.prev = v
	}
	return t.prev
}
