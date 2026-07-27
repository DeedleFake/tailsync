package peer

import "testing"

func TestDialBackAddrUsesAdvertisedPort(t *testing.T) {
	got := dialBackAddr("127.0.0.1:54321", 19001, 5960)
	if got != "127.0.0.1:19001" {
		t.Fatalf("got %q", got)
	}
	// Missing advertised port falls back to local default (same-port mesh).
	got = dialBackAddr("100.64.0.2:9999", 0, 5960)
	if got != "100.64.0.2:5960" {
		t.Fatalf("got %q", got)
	}
	if dialBackAddr("not-a-hostport", 1, 2) != "" {
		t.Fatal("expected empty for bad remote")
	}
}
