# tailsync

[![Go Reference](https://pkg.go.dev/badge/deedles.dev/tailsync.svg)](https://pkg.go.dev/deedles.dev/tailsync)

Synchronize a directory between your Tailscale machines.

> **Warning:** tailsync is **alpha** software. The protocol, on-disk index format, flags, and APIs may change without compatibility guarantees. Do not rely on it as the sole copy of important data; keep independent backups.

tailsync is a small daemon that runs on each machine. Instances discover each other on your tailnet, wake peers with best-effort **notifies**, and **pull** missing or updated files (pull is authoritative; notify is optional wake-up). Transfers use rsync-style block signatures so only changed regions are sent when a local basis exists. A persistent local index records known file state so deletions made while the daemon was stopped are still detected and propagated.

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

By default, tailsync uses the system **`tailscaled`** (LocalAPI). It does not register a separate machine in the Tailscale admin console; it is just a process on the existing node. It listens with **QUIC** (UDP) on port `5960` on the host’s Tailscale IP(s) and:

1. Watches the sync directory for filesystem events (debounced, default 1 s), with a periodic full rescan as a safety net, and reconciles against the on-disk index (adds, modifies, and offline deletions).
2. **Discovers** online tailnet peers in the background and keeps **persistent QUIC connections** (one per peer, Hello once per connection).
3. When local index content changes, fans out concurrent **best-effort notifies** as cheap streams on already-connected sessions. Dead or not-yet-connected peers cannot stall the writer.
4. On notify (or on the pull interval / bootstrap / peer-up), each node **pulls** manifests and content from connected peers (one-off streams per op). Notify never commits state; the pull-time manifest (LWW) is truth.
5. Merges remote manifests using last-writer-wins on `updated_at`.

Keep host clocks roughly in sync (NTP). Conflict resolution uses wall-clock `updated_at`; equal-timestamp ties use a stable total order (deletion preference, then content hash, mode, then mtime).

For regular files, permission bits (`mode`) and modification time (`mtime`) are synchronized, including touch-only changes. Content hash and size are authoritative for file contents. Access time (atime), ownership, extended attributes, and ACLs are not synchronized.

### Network modes

| Mode | Flag | Behavior |
|------|------|----------|
| **host** (default) | *(none)* | Use the system Tailscale daemon. Listen on the host’s Tailscale IP(s) (IPv4 and IPv6 when bindable; unavailable address families are skipped). Dial peers by Tailscale IP (MagicDNS only if no IP is known). No auth key. Requires `tailscaled` running and logged in. |
| **tsnet** | `-tsnet` | Embed a [tsnet](https://pkg.go.dev/tailscale.com/tsnet) node that registers as a **separate** machine on the tailnet. Useful in containers without host Tailscale. Supports `-hostname` and `-authkey`. |
| **plain** | `-plain` | Localhost QUIC only, for testing. Requires `TAILSYNC_TESTING=1`. |

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-dir` | (required) | Directory to synchronize |
| `-state` | `<dir>/.tailsync` | Index directory (also holds tsnet state when `-tsnet`) |
| `-hostname` | `tailsync-<os-hostname>` (tsnet only) | tsnet hostname; in host mode, identity comes from LocalAPI |
| `-port` | `5960` | UDP port for QUIC peer connections |
| `-authkey` | `$TS_AUTHKEY` | Tailscale auth key for **`-tsnet`** only (optional if tsnet state already exists) |
| `-peers` | (discover) | Comma-separated `host:port` peers (**test/override only**; skips status discovery) |
| `-scan-interval` | `30s` | Safety-net full rescan period (FS watch handles most local edits) |
| `-sync-interval` | `45s` | Backup peer **pull** period (local changes notify; peers pull on notify or this interval) |
| `-watch-debounce` | `1s` | Debounce wait after FS events before reconcile (`0` = default) |
| `-no-watch` | `false` | Disable filesystem watching; rely on `-scan-interval` only |
| `-block-size` | `4096` | Delta block size |
| `-dial-timeout` | `30s` (`daemon.DefaultDialTimeout`) | Max wait for each outbound discovery dial+handshake (`0` = daemon default); caps waits on nodes not listening |
| `-tsnet` | `false` | Use embedded tsnet instead of host `tailscaled` |
| `-plain` | `false` | Plain QUIC on `127.0.0.1` (requires `TAILSYNC_TESTING=1`) |
| `-v` | `false` | Debug logging |

`-plain` and `-tsnet` are mutually exclusive.

### Peer discovery and connections

Discovery is a **background service** that builds a roster of persistent peer sessions:

| Source | Role |
|--------|------|
| **Status Online (same user)** | Candidates from Tailscale status owned by the current user (IPs preferred; MagicDNS fallback) |
| **`-peers`** | Test/override pin only; when set, status discovery is skipped |
| **Inbound** | Accept connections from the current user's machines only (WhoIs + Hello; plain mode is local tests) |

Once connected, each peer has **at most one** QUIC connection. Hello is **connection-scoped** (not per stream). Application ops (notify, manifest, file, delta, ping) open short-lived streams on that connection. Redundant connections are rejected with `already_connected`; simultaneous dial races pick a deterministic winner by node ID. Unhealthy sessions (failed heartbeats) are replaced and re-discovered with exponential backoff (**never permanently banned**). Discovery dials use an in-flight concurrency semaphore (default 32) that is released **before** backoff sleep so other peers can still dial.

Notify and pull use the **connected roster** only (no one-shot dial-per-op). Pull content streams are capped globally (default 8). Offline (status) peers are not dialed.

With empty `-peers`, discovery dials **online nodes owned by the current Tailscale user** (same `UserID` as Self)—not every node on the tailnet. Tagged machines of that user are included. Shared-in devices, other users' machines, sharee-only netmap entries, and Mullvad exit nodes (`tag:mullvad-exit-node` and/or `*.mullvad.ts.net`) are skipped. Soft dial failures and exponential backoff are expected for nodes that do not run tailsync; they do not block writers. Inbound Hello is bound to Tailscale WhoIs and the same ownership check (plain/local test mode skips WhoIs). Use **`-peers host:port,...`** only for local tests / explicit overrides.

```bash
# two machines (each uses its host Tailscale identity; zero-config mesh)
tailsync -dir ~/shared   # machine a
tailsync -dir ~/shared   # machine b
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
- **FS watch + debounce** — Local edits are detected via recursive filesystem events (debounced, default 1 s), then reconciled. Paths under `.tailsync` / `.tailsync-*` are ignored. If watching fails to start (unsupported platform/permissions), tailsync logs a warning and falls back to timer-only scanning.
- **Scan** — Walks regular files only; live index entries missing on disk become tombstones (offline deletion). Empty directories and symlinks are not synced. `-scan-interval` remains a full safety-net rescan when watch is active.
- **Notify + pull** — Local peer-visible index changes fan out concurrent best-effort **notifies** on connected sessions (soft hints: path/hash/`updated_at`; not final bytes). Receivers schedule a **pull**; the pull-time manifest and LWW apply are authoritative. After a successful pull, optional **infect-and-die** notifies only newly acquired content ids. Content-identity dedupe avoids notify storms (already-have → ignore; no re-notify loop).
- **Catch-up** — `-sync-interval` pull, peer-up, and inbound serve recover missed notifies and offline peers. Late joiners are discovered, connected, and pull. Writers are not blocked waiting on dead peers.
- **Hash fast path** — Reuses the stored SHA-256 when size and mtime still match the index. Silent content rewrites that preserve mtime are not detected until another field changes.
- **Delta** — Adler-style rolling weak checksums and MD5 strong match per block; full-file SHA-256 is authoritative after apply. Whole-file buffers are used for transfers (default max 64 MiB per file).
- **Concurrency** — Discovery dials (~32 in-flight) are separate from pull content streams (~8 global). Notify fan-out is high-parallelism with no batch barrier on the main loop. Local reconcile and peer apply commits share one mutex; network transfer for a content apply runs unlocked (re-LWW on commit).
- **Protocol** — Length-prefixed JSON headers with optional binary payloads over **QUIC**. Hello is once per connection (connection-scoped session model); each op uses its own stream (`notify`, `manifest_req`, `file_req`, `delta_req`, `ping`, …). Wire version is **`proto.Version` (currently 2)** — pull-oriented with notify + `already_connected`. Hello accepts only that exact version (any other value, including 0, is rejected). QUIC uses ephemeral self-signed TLS (clients skip verify); peer trust is the tailnet mesh, not a public CA.
- **Conflicts** — Last-writer-wins on `updated_at`; equal clocks use a stable total order (deletion, hash, mode, mtime) so peers converge.
- **Metadata** — Mode and mtime are synchronized end-to-end; peers adopt metadata when the same content hash wins LWW.
- **Networking** — Host mode binds only to Tailscale addresses (not `0.0.0.0`). Shared UDP `PacketConn`s (via quic-go `Transport`) serve both listen and dial so sessions do not open ephemeral sockets per op. Family-matched dials for dual-stack. tsnet resolves names via LocalAPI status (not system DNS alone).
- **Sync-tree confinement** — File I/O under `-dir` (scan, serve, apply, deletes) uses Go’s [`os.Root`](https://pkg.go.dev/os#Root) so path traversal and symlink escapes cannot reach outside the sync directory; index/state paths under `-state` are separate trusted local storage. Peer paths under `.tailsync` / `.tailsync-*` are rejected so the default state dir cannot be written via sync. On multi-party tailnets, prefer an explicit `-state` path outside `-dir`.

State directories under the sync tree named `.tailsync` or `.tailsync-*` are ignored by the scanner and cannot be applied from peers.

## Embedding (library API)

The public Go package [`deedles.dev/tailsync/daemon`](https://pkg.go.dev/deedles.dev/tailsync/daemon) is the primary library API for embedding tailsync (still **alpha**; may change without compatibility guarantees, same as the rest of the project). The CLI (`cmd/tailsync`) is a thin wrapper around the same package.

### Daemon API overview

| Go | Role |
|----|------|
| `daemon.Config` | Settings: `Dir`, `StateDir`, `Hostname`, `AuthKey`, `Port`, `Peers`, intervals, `DisableWatch`, `BlockSize`, `DialTimeout`, `DiscoveryConcurrency`, `PullStreamConcurrency`, `HeartbeatInterval`, `TombstoneTTL`, `NetMode`, `Logger`, `OnReady`, `OnAuthURL`, … |
| `daemon.New(cfg)` | Validates config and returns a stopped `*Daemon` (does not start networking) |
| `(*Daemon).Run(ctx)` | Brings up networking and runs until `ctx` is canceled or a fatal error |
| `(*Daemon).InjectNetworkChange()` | After host connectivity updates in **tsnet** mode, inject a netmon event (no-op if not running / not yet installed) |
| `daemon.DefaultPort`, `DefaultScanInterval`, `DefaultSyncInterval`, `DefaultWatchDebounce`, `DefaultBlockSize`, `DefaultDialTimeout`, `DefaultDiscoveryConcurrency`, `DefaultPullStreamConcurrency`, `DefaultHeartbeatInterval`, `DefaultTombstoneTTL`, … | Effective defaults when zero values are left in `Config` |
| `daemon.NetModeHost` / `NetModeTSNet` / `NetModePlain` | Network attachment modes (same semantics as CLI flags) |

Minimal embed:

```go
cfg := daemon.Config{
	Dir:     "/path/to/shared",
	NetMode: daemon.NetModeTSNet, // or NetModeHost / NetModePlain
	// AuthKey, Hostname, StateDir, OnReady, OnAuthURL, Logger, …
}
d, err := daemon.New(cfg)
if err != nil {
	return err
}
return d.Run(ctx)
```

Zero `Port`, interval, `BlockSize`, or `DialTimeout` mean the `Default*` constants above (for example port `5960`, block size `4096`).

### Android / gomobile

This module no longer ships a `mobile` package. Android apps should:

1. Depend on **`deedles.dev/tailsync/daemon`** for the sync engine.
2. Own a **gomobile-bindable wrapper** in the app repository (config mapping, Start/Stop lifecycle, JSON event callbacks, Android netmon interface injection). That wrapper imports `daemon` and is what `gomobile bind` targets.

Requires gomobile and an Android NDK/SDK:

```bash
go install golang.org/x/mobile/cmd/gomobile@latest
gomobile init

# Bind the app-owned package (example path — lives in the Android app repo):
gomobile bind -target=android -o tailsync.aar ./mobile
```

On Android, prefer **`NetModeTSNet`** (embedded Tailscale node). Host LocalAPI expects a system `tailscaled` and is not a typical phone setup. The app owns lifecycle (usually a foreground service) and must pass **absolute, writable** paths (for example app-private storage).

**Authentication (tsnet)**

| Situation | What happens |
|-----------|----------------|
| `AuthKey` set and valid | Silent enroll |
| Existing tsnet state under `StateDir` (prior successful login) | Silent reconnect |
| Empty `AuthKey`, no enrolled state | Interactive browser login; use `Config.OnAuthURL` to surface the login URL while `Run` / bring-up is still waiting |

Keep a stable `StateDir` across launches so the node does not re-prompt after the first successful login.

**Network interfaces on Android (required for tsnet)**

On **Android API 30+**, Go’s `net.Interfaces()` fails with permission errors. The app-owned bind layer should register a host interface snapshot for tsnet/netmon (via Tailscale’s `netmon` getters) **before** bring-up, and call `(*Daemon).InjectNetworkChange()` after connectivity updates once the node is running. During the long `tsnet.Up` window inject may no-op until NetMon is installed; the daemon then fires a catch-up inject.

The `INTERNET` permission is still required for sockets; it does **not** fix `net.Interfaces` alone. Typical sources: `ConnectivityManager` / `LinkProperties` for interface name, index, flags, MTU, address CIDRs, default route interface, and gateway.

Desktop/CLI is unchanged: if nothing overrides interfaces, netmon keeps using `net.Interfaces()`.

**Notes for app wrappers**

- Paths must be absolute and writable by the process.
- Cancel `Run`’s context (or stop the wrapper’s generation) when the service is destroyed so the embedded node and goroutines exit.
- Call long-running start paths off the Android main thread; keep auth/log callbacks non-blocking (post to the main looper only for UI).
- Do not log or ship auth keys.

## Development

```bash
go mod tidy
go test -vet=all ./...
go fmt ./...
go tool modernize ./...
go tool staticcheck ./...
```

`modernize` and `staticcheck` are module tools (see the `tool` block in `go.mod`). CI runs the same checks (`.github/workflows/ci.yml`), including a `go mod tidy` drift check.
