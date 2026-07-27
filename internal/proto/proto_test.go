package proto_test

import (
	"bytes"
	"testing"
	"time"

	"deedles.dev/tailsync/internal/index"
	"deedles.dev/tailsync/internal/proto"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	msg := proto.NewFileData("a/b.txt", index.Entry{
		Path:      "a/b.txt",
		Size:      5,
		Hash:      "abc",
		ModTime:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Mode:      0o644,
		UpdatedAt: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}, []byte("hello"))

	var buf bytes.Buffer
	if err := proto.Encode(&buf, msg); err != nil {
		t.Fatal(err)
	}
	got, err := proto.Decode(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.Type != proto.TypeFileData {
		t.Fatalf("type %q", got.Header.Type)
	}
	if got.Header.Path != "a/b.txt" || string(got.Payload) != "hello" {
		t.Fatalf("got %+v payload %q", got.Header, got.Payload)
	}
	if got.Header.PayloadLen != 5 {
		t.Fatalf("payload_len %d", got.Header.PayloadLen)
	}
}

func TestManifestMessage(t *testing.T) {
	entries := []index.ManifestEntry{
		{Path: "x", Hash: "1", UpdatedAt: time.Now()},
		{Path: "y", Deleted: true, UpdatedAt: time.Now()},
	}
	msg := proto.NewManifest(entries)
	var buf bytes.Buffer
	if err := proto.Encode(&buf, msg); err != nil {
		t.Fatal(err)
	}
	got, err := proto.Decode(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Header.Entries) != 2 {
		t.Fatalf("entries %d", len(got.Header.Entries))
	}
}

func TestHello(t *testing.T) {
	var buf bytes.Buffer
	if err := proto.Encode(&buf, proto.NewHello("node-a", 5960)); err != nil {
		t.Fatal(err)
	}
	got, err := proto.Decode(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.Type != proto.TypeHello || got.Header.NodeID != "node-a" || got.Header.Version != proto.Version {
		t.Fatalf("%+v", got.Header)
	}
	if got.Header.Port != 5960 {
		t.Fatalf("port %d", got.Header.Port)
	}
}

func TestNotifyRoundTrip(t *testing.T) {
	hints := []index.ManifestEntry{
		{Path: "a.txt", Hash: "abc", UpdatedAt: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
	}
	var buf bytes.Buffer
	if err := proto.Encode(&buf, proto.NewNotify("node-a", 5960, hints)); err != nil {
		t.Fatal(err)
	}
	got, err := proto.Decode(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.Type != proto.TypeNotify || got.Header.NodeID != "node-a" {
		t.Fatalf("%+v", got.Header)
	}
	if len(got.Header.Entries) != 1 || got.Header.Entries[0].Path != "a.txt" {
		t.Fatalf("entries %+v", got.Header.Entries)
	}
}

func TestDecodeRejectsOversizedTotal(t *testing.T) {
	// Craft a header claiming a payload that would exceed MaxMessageSize with header.
	// Small JSON header with huge PayloadLen.
	h := []byte(`{"type":"file_data","payload_len":999999999}`)
	var frame []byte
	frame = append(frame, 0, 0, 0, 0)
	// little-endian header len
	frame[0] = byte(len(h))
	frame[1] = byte(len(h) >> 8)
	frame = append(frame, h...)
	// no payload body — Decode should fail on total size before reading payload
	_, err := proto.Decode(bytes.NewReader(frame))
	if err == nil {
		t.Fatal("expected oversized message error")
	}
}
