package peer

import (
	"testing"
	"time"
)

func TestRosterInstallAccept(t *testing.T) {
	r := NewRoster()
	s := &Session{nodeID: "b", localID: "a", dialer: true, addr: "127.0.0.1:1"}
	s.healthy.Store(true)
	res, old := r.Install(s)
	if res != installAccepted || old != nil {
		t.Fatalf("res=%v old=%v", res, old)
	}
	if r.Get("b") != s {
		t.Fatal("expected session")
	}
}

func TestRosterRejectHealthyRedundant(t *testing.T) {
	r := NewRoster()
	// a < b so preferred dialer is a. Existing is dialer (a dialed b).
	cur := &Session{nodeID: "b", localID: "a", dialer: true, addr: "1"}
	cur.healthy.Store(true)
	r.Install(cur)

	// New inbound (b dialed a) — preferred dialer is a, so keep existing.
	neu := &Session{nodeID: "b", localID: "a", dialer: false, addr: "2"}
	neu.healthy.Store(true)
	res, old := r.Install(neu)
	if res != installRejected || old != nil {
		t.Fatalf("res=%v old=%v", res, old)
	}
	if r.Get("b") != cur {
		t.Fatal("should keep existing")
	}
}

func TestRosterReplaceUnhealthy(t *testing.T) {
	r := NewRoster()
	cur := &Session{nodeID: "b", localID: "a", dialer: true, addr: "1"}
	cur.healthy.Store(false)
	r.Install(cur)

	neu := &Session{nodeID: "b", localID: "a", dialer: true, addr: "2"}
	neu.healthy.Store(true)
	res, old := r.Install(neu)
	if res != installReplaced || old != cur {
		t.Fatalf("res=%v old=%v", res, old)
	}
	if r.Get("b") != neu {
		t.Fatal("should have new")
	}
}

func TestRosterRacePrefersSmallerDialer(t *testing.T) {
	r := NewRoster()
	// local=a, remote=b, a < b → preferred dialer a.
	// Existing is inbound (b dialed) — not preferred.
	cur := &Session{nodeID: "b", localID: "a", dialer: false, addr: "1"}
	cur.healthy.Store(true)
	r.Install(cur)

	// New outbound (a dialed) — preferred.
	neu := &Session{nodeID: "b", localID: "a", dialer: true, addr: "2"}
	neu.healthy.Store(true)
	res, old := r.Install(neu)
	if res != installReplaced || old != cur {
		t.Fatalf("res=%v old=%p want replaced", res, old)
	}
	if r.Get("b") != neu {
		t.Fatal("preferred dialer should win")
	}
}

func TestRosterSnapshot(t *testing.T) {
	r := NewRoster()
	s := &Session{nodeID: "p", localID: "me", addr: "x:1", since: time.Now()}
	s.healthy.Store(true)
	r.Install(s)
	snap := r.Snapshot()
	if len(snap) != 1 || snap[0].NodeID != "p" || snap[0].Addr != "x:1" {
		t.Fatalf("%+v", snap)
	}
}

func TestPreferNewSession(t *testing.T) {
	// a < b: preferred dialer is a
	outbound := &Session{nodeID: "b", localID: "a", dialer: true}
	inbound := &Session{nodeID: "b", localID: "a", dialer: false}
	if !preferNewSession(outbound, inbound) {
		t.Fatal("outbound from smaller id should prefer new")
	}
	if preferNewSession(inbound, outbound) {
		t.Fatal("inbound should not replace preferred outbound")
	}
}
