package mobile

import (
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
)

func TestIsSecretKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		key  string
		want bool
	}{
		{"authkey", true},
		{"AuthKey", true},
		{"auth_key", true},
		{"ts_authkey", true},
		{"password", true},
		{"secret", true},
		{"token", true},
		{"group.authkey", true},
		{"nested.group.token", true},
		{"hostname", false},
		{"dir", false},
		{"msg", false},
		{"authorization", false}, // leaf name is authorization, not token
	}
	for _, tc := range cases {
		if got := isSecretKey(tc.key); got != tc.want {
			t.Errorf("isSecretKey(%q)=%v want %v", tc.key, got, tc.want)
		}
	}
}

func TestAppendAttrRedactsSecrets(t *testing.T) {
	t.Parallel()
	dst := map[string]any{}
	appendAttr(dst, "", slog.String("authkey", "tskey-secret"))
	appendAttr(dst, "", slog.String("hostname", "node-a"))
	appendAttr(dst, "cfg", slog.String("token", "abc"))
	appendAttr(dst, "", slog.Group("nested", slog.String("password", "x"), slog.Int("port", 1)))

	if dst["authkey"] != "[redacted]" {
		t.Fatalf("authkey: %v", dst["authkey"])
	}
	if dst["hostname"] != "node-a" {
		t.Fatalf("hostname: %v", dst["hostname"])
	}
	if dst["cfg.token"] != "[redacted]" {
		t.Fatalf("cfg.token: %v", dst["cfg.token"])
	}
	if dst["nested.password"] != "[redacted]" {
		t.Fatalf("nested.password: %v", dst["nested.password"])
	}
	if dst["nested.port"] != int64(1) && dst["nested.port"] != 1 {
		// slog may store as int or int64 depending on Value.Any()
		t.Fatalf("nested.port: %v (%T)", dst["nested.port"], dst["nested.port"])
	}
}

type captureListener struct {
	mu     sync.Mutex
	events []string
}

func (c *captureListener) OnEvent(eventJSON string) {
	c.mu.Lock()
	c.events = append(c.events, eventJSON)
	c.mu.Unlock()
}

func (c *captureListener) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.events))
	copy(out, c.events)
	return out
}

func TestEventHandlerRedactsLogAttrs(t *testing.T) {
	n := &Node{}
	lis := &captureListener{}
	n.SetListener(lis)
	h := newEventHandler(n)
	log := slog.New(h)

	log.Info("bring up",
		"authkey", "tskey-should-redact",
		"auth_key", "also-secret",
		"hostname", "phone",
	)

	evs := lis.snapshot()
	if len(evs) != 1 {
		t.Fatalf("events: %v", evs)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(evs[0]), &m); err != nil {
		t.Fatal(err)
	}
	if m["type"] != "log" {
		t.Fatalf("type: %v", m["type"])
	}
	attrs, ok := m["attrs"].(map[string]any)
	if !ok {
		t.Fatalf("attrs: %T %v", m["attrs"], m["attrs"])
	}
	if attrs["authkey"] != "[redacted]" {
		t.Fatalf("authkey attr: %v", attrs["authkey"])
	}
	if attrs["auth_key"] != "[redacted]" {
		t.Fatalf("auth_key attr: %v", attrs["auth_key"])
	}
	if attrs["hostname"] != "phone" {
		t.Fatalf("hostname: %v", attrs["hostname"])
	}
	// Free-text messages are not scrubbed (documented).
	if m["msg"] != "bring up" {
		t.Fatalf("msg: %v", m["msg"])
	}
}
