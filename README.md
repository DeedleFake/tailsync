# tailsync

[![Go Reference](https://pkg.go.dev/badge/deedles.dev/tailsync.svg)](https://pkg.go.dev/deedles.dev/tailsync)

Synchronize a directory between your Tailscale machines.

> **Warning:** tailsync is **alpha** software. The protocol, on-disk index format, flags, and APIs may change without compatibility guarantees. Do not rely on it as the sole copy of important data; keep independent backups.

tailsync is a small daemon that runs on each machine. Instances discover each other on your tailnet (or via an explicit peer list), exchange file manifests, and pull missing or updated files. Transfers use rsync-style block signatures so only changed regions are sent when a local basis exists. A persistent local index records known file state so deletions made while the daemon was stopped are still detected and propagated.

## Install

```bash
go install deedles.dev/tailsync/cmd/tailsync@latest
```

Or build from a checkout:

```bash
go build -o tailsync ./cmd/tailsync
```

## Usage

On each machine (with [Tailscale](https://tailscale.com/) already running and logged in):

```bash
tailsync -dir /path/to/shared
```

By default, tailsync uses the system **`tailscaled`** (LocalAPI). It does not register a separate machine in the Tailscale admin console; it is just a process on the existing node. It listens on TCP port `5960` on the host’s Tailscale IP(s) and periodically:

1. Scans the sync directory and reconciles against the on-disk index (adds, modifies, and offline deletions).
2. Connects to online tailnet peers (or addresses from `-peers`) and merges remote manifests using last-writer-wins on `updated_at`.

Keep host clocks roughly in sync (NTP). Conflict resolution uses wall-clock `updated_at`; equal-timestamp ties use a stable total order (deletion preference, then content hash, mode, then mtime).

For regular files, permission bits (`mode`) and modification time (`mtime`) are synchronized, including touch-only changes. Content hash and size are authoritative for file contents. Access time (atime), ownership, extended attributes, and ACLs are not synchronized.

### Network modes

| Mode | Flag | Behavior |
|------|------|----------|
| **host** (default) | *(none)* | Use the system Tailscale daemon. Listen on the host’s Tailscale IP(s) (IPv4 and IPv6 when bindable; unavailable address families are skipped). Dial peers by Tailscale IP (MagicDNS only if no IP is known). No auth key. Requires `tailscaled` running and logged in. |
| **tsnet** | `-tsnet` | Embed a [tsnet](https://pkg.go.dev/tailscale.com/tsnet) node that registers as a **separate** machine on the tailnet. Useful in containers without host Tailscale. Supports `-hostname` and `-authkey`. |
| **plain** | `-plain` | Localhost TCP only, for testing. Requires `TAILSYNC_TESTING=1`. |

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-dir` | (required) | Directory to synchronize |
| `-state` | `<dir>/.tailsync` | Index directory (also holds tsnet state when `-tsnet`) |
| `-hostname` | `tailsync-<os-hostname>` (tsnet only) | tsnet hostname; in host mode, identity comes from LocalAPI |
| `-service` | (empty) | Only dial peers whose hostname or DNS name contains this substring; empty means all online tailnet peers (see [Peer discovery](#peer-discovery)) |
| `-port` | `5960` | TCP port for peer connections |
| `-authkey` | `$TS_AUTHKEY` | Tailscale auth key for **`-tsnet`** only (optional if tsnet state already exists) |
| `-peers` | (discover) | Comma-separated `host:port` peers (skips discovery) |
| `-scan-interval` | `30s` | Local directory rescan period |
| `-sync-interval` | `45s` | Peer sync period |
| `-block-size` | `4096` | Delta block size |
| `-tsnet` | `false` | Use embedded tsnet instead of host `tailscaled` |
| `-plain` | `false` | Plain TCP on `127.0.0.1` (requires `TAILSYNC_TESTING=1`) |
| `-v` | `false` | Debug logging |

`-plain` and `-tsnet` are mutually exclusive.

### Peer discovery

With host or tsnet mode and no `-peers` list, tailsync dials online tailnet peers on `-port` each sync interval, using Tailscale IPs from status (falling back to MagicDNS when needed). By default that includes **every** online peer—phones, TVs, unrelated servers—which is fine on a small personal tailnet. On larger tailnets, prefer:

- `-service <substring>` to only dial hosts whose Tailscale hostname or DNS name contains that string (for example `-service tailsync` with tsnet names like `tailsync-*`), or
- `-peers host:port,...` to pin exact addresses and skip discovery.

```bash
# two machines (each uses its host Tailscale identity)
tailsync -dir ~/shared   # machine a
tailsync -dir ~/shared   # machine b

# pin peers explicitly
tailsync -dir ~/shared -peers other-host:5960,100.x.y.z:5960
```

### Embedded tsnet

When there is no system Tailscale daemon (for example some containers):

```bash
tailsync -tsnet -dir ~/shared -hostname tailsync-a
# optional: -authkey $TS_AUTHKEY
```

This registers a separate node named with `-hostname` (default `tailsync-<os-hostname>`).

### Local testing without Tailscale

```bash
# terminal 1
TAILSYNC_TESTING=1 tailsync -plain -dir /tmp/sync-a -state /tmp/state-a -port 5960 -peers 127.0.0.1:5961 -hostname a

# terminal 2
TAILSYNC_TESTING=1 tailsync -plain -dir /tmp/sync-b -state /tmp/state-b -port 5961 -peers 127.0.0.1:5960 -hostname b
```

## How it works

- **Index** — JSON under `-state` with size, mtime, mode, content SHA-256, and deletion tombstones (GC’d after 30 days by default). After a tombstone is dropped, a lagging peer that never saw the delete can re-introduce the file; keep the TTL longer than the maximum expected peer offline window.
- **Scan** — Walks regular files only; live index entries missing on disk become tombstones (offline deletion). Empty directories and symlinks are not synced.
- **Hash fast path** — Reuses the stored SHA-256 when size and mtime still match the index. Silent content rewrites that preserve mtime are not detected until another field changes.
- **Delta** — Adler-style rolling weak checksums and MD5 strong match per block; full-file SHA-256 is authoritative after apply. Whole-file buffers are used for transfers (default max 64 MiB per file).
- **Concurrency** — Local reconcile and peer apply share one mutex, including during network transfers (correctness over throughput; a slow peer can delay scans).
- **Protocol** — Length-prefixed JSON headers with optional binary payloads over a single TCP session.
- **Conflicts** — Last-writer-wins on `updated_at`; equal clocks use a stable total order (deletion, hash, mode, mtime) so peers converge.
- **Metadata** — Mode and mtime are synchronized end-to-end; peers adopt metadata when the same content hash wins LWW.
- **Networking** — Host mode binds only to Tailscale addresses (not `0.0.0.0`), discovers peers via LocalAPI status, and dials with the host network stack (routed by `tailscaled`).

State directories under the sync tree named `.tailsync` or `.tailsync-*` are ignored by the scanner.

## Android / gomobile

Android apps can embed the same sync engine via [gomobile](https://pkg.go.dev/golang.org/x/mobile/cmd/gomobile) using the package `deedles.dev/tailsync/mobile`. The CLI (`cmd/tailsync`) remains the usual way to run on desktops and servers.

On mobile, the default network mode is **tsnet** (an embedded Tailscale node). Host LocalAPI mode expects a system `tailscaled` and is not a typical Android setup. The app owns the lifecycle (`Start` / `Stop`), usually from a foreground service, and must pass **absolute, writable** paths (for example app-private storage).

### Bind (AAR)

Requires gomobile and an Android NDK/SDK.

```bash
go install golang.org/x/mobile/cmd/gomobile@latest
gomobile init

# from a checkout of this module
gomobile bind -target=android -o tailsync.aar deedles.dev/tailsync/mobile
```

### API overview

| Go | Role |
|----|------|
| `Config` | Settings: `Dir`, `StateDir`, `Hostname`, `AuthKey`, `Port`, `Peers`, `ServiceName`, `ScanIntervalMs`, `SyncIntervalMs`, `BlockSize`, `NetMode` |
| `NewNode(cfg)` | Validates config and returns a stopped `Node` |
| `Node.Start()` / `Stop()` / `IsRunning()` | Lifecycle. `Start` blocks until listening succeeds or fails; call it off the main thread |
| `Node.SetListener(EventListener)` | Optional JSON event callbacks (logs and status); handlers must return quickly |
| `Node.StatusJSON()` | Snapshot for UI (never includes `AuthKey`; zero config fields are shown as effective defaults; includes `phase`) |
| `Version()` | Module or build version string |

`NetMode` values: `"tsnet"` (default), `"host"`, `"plain"` (localhost TCP for tests only).

`IsRunning()` is true while starting, serving, or stopping (resources may still be held after a timed-out `Stop`). `StatusJSON`’s `running` field is true only while serving after a successful `Start`. `phase` is one of `idle`, `starting`, `running`, or `stopping`.

### Kotlin example

```kotlin
// After adding tailsync.aar to the Android app module.
val cfg = Config().apply {
    dir = context.filesDir.resolve("sync").absolutePath
    stateDir = context.filesDir.resolve("tailsync-state").absolutePath
    hostname = "tailsync-phone"
    authKey = BuildConfig.TS_AUTHKEY // prefer a reusable auth key
    // netMode defaults to "tsnet"
}
val node = Mobile.newNode(cfg)
val mainHandler = Handler(Looper.getMainLooper())
node.setListener(EventListener { eventJSON ->
    // Called from a Go background thread — keep this fast (no network/disk/UI).
    mainHandler.post {
        // parse JSON: type = log | status | error
        Log.i("tailsync", eventJSON)
    }
})

// From a foreground service — never call start() on the main thread
// (tsnet bring-up can block long enough to ANR).
serviceScope.launch(Dispatchers.IO) {
    try {
        node.start() // blocks until listening or failure
    } catch (e: Exception) {
        Log.e("tailsync", "start failed", e)
    }
}

serviceScope.launch(Dispatchers.IO) {
    node.stop() // no-op if already stopped; call when the service is destroyed
}
```

**Notes**

- First registration needs a Tailscale **auth key** (or existing tsnet state under `StateDir`).
- Paths must be absolute and writable by the app process.
- Call `Stop` when the service is destroyed so the embedded node and goroutines exit.
- Run `start()` / `stop()` off the main thread; keep `OnEvent` non-blocking (post to the main thread only for UI).
- Do not log or ship auth keys. Mobile events redact secret-like **attribute keys**; free-text log messages are not scrubbed.
- Zero `Port`, interval, or `BlockSize` mean daemon defaults; `StatusJSON` reports effective values (for example port `5960`).

## Development

```bash
go mod tidy
go test -vet=all ./...
go fmt ./...
go tool modernize ./...
go tool staticcheck ./...
```

`modernize` and `staticcheck` are module tools (see the `tool` block in `go.mod`). CI runs the same checks (`.github/workflows/ci.yml`), including a `go mod tidy` drift check. Android SDK is not required for `go test`; plain-mode tests exercise the mobile API on localhost.
