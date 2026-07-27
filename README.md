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
2. When local index content changes, fans out concurrent **best-effort notifies** to candidates (in-memory hot set + status Online peers). Dead peers cannot stall the writer.
3. On notify (or on the pull interval / bootstrap), each node **pulls** manifests and content from candidates. Notify never commits state; the pull-time manifest (LWW) is truth.
4. Merges remote manifests using last-writer-wins on `updated_at`.

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
| `-service` | (empty) | Only dial peers whose hostname or DNS name contains this substring; **empty discovery may dial all online peers** (see [Peer discovery](#peer-discovery)) |
| `-port` | `5960` | UDP port for QUIC peer connections |
| `-authkey` | `$TS_AUTHKEY` | Tailscale auth key for **`-tsnet`** only (optional if tsnet state already exists) |
| `-peers` | (discover) | Comma-separated `host:port` peers (**test/override only**; skips status discovery). Prefer hot set + `-service` in production |
| `-scan-interval` | `30s` | Safety-net full rescan period (FS watch handles most local edits) |
| `-sync-interval` | `45s` | Backup peer **pull** period (local changes notify; peers pull on notify or this interval) |
| `-watch-debounce` | `1s` | Debounce wait after FS events before reconcile (`0` = default) |
| `-no-watch` | `false` | Disable filesystem watching; rely on `-scan-interval` only |
| `-block-size` | `4096` | Delta block size |
| `-dial-timeout` | `5s` (`daemon.DefaultDialTimeout`) | Max wait for each outbound peer dial (`0` = daemon default); caps waits on nodes not listening |
| `-tsnet` | `false` | Use embedded tsnet instead of host `tailscaled` |
| `-plain` | `false` | Plain QUIC on `127.0.0.1` (requires `TAILSYNC_TESTING=1`) |
| `-v` | `false` | Debug logging |

`-plain` and `-tsnet` are mutually exclusive.

### Peer discovery

Membership is **memory-only** (not persisted):

| Source | Role |
|--------|------|
| **Hot set** | After a successful Hello (notify or pull), remember `nodeID → addr` for fast re-dial |
| **Status Online** | Bootstrap from Tailscale status (IPs preferred; MagicDNS fallback) |
| **`-peers`** | Test/override pin only; when set, status discovery is skipped |

Candidates for notify and pull are the hot set union status Online peers (plus `-peers` when set). Soft-fail (timeout/refused) applies **per dial address** to hot-set and status-discovered candidates with exponential backoff, but **never permanently bans**; after backoff expires, status Online and successful Hello reintroduce the peer. Explicit `-peers` pins always re-dial each batch (test/override). Offline (status) peers are skipped when possible.

With empty `-peers` and empty `-service`, discovery may include **every** online tailnet node—phones, TVs, unrelated servers—not only machines running tailsync. Soft dial failures are expected and do not block writers (notifies are fire-and-forget; pulls are capped). **Mullvad VPN exit nodes** (`tag:mullvad-exit-node`) are always excluded; they appear Online but never run tailsync. Prefer:

- **`-service <substring>`** to only dial hosts whose Tailscale hostname or DNS name contains that string (for example `-service tailsync` with tsnet names like `tailsync-*`), or
- **`-peers host:port,...`** only for local tests / explicit overrides (not the recommended production path).

```bash
# two machines (each uses its host Tailscale identity; zero-config mesh)
tailsync -dir ~/shared   # machine a
tailsync -dir ~/shared   # machine b

# filter discovery on a large tailnet
tailsync -dir ~/shared -service tailsync
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
- **Notify + pull** — Local peer-visible index changes fan out concurrent best-effort **notifies** (soft hints: path/hash/`updated_at`; not final bytes). Receivers schedule a **pull**; the pull-time manifest and LWW apply are authoritative. After a successful pull, optional **infect-and-die** notifies only newly acquired content ids. Content-identity dedupe avoids notify storms (already-have → ignore; no re-notify loop).
- **Catch-up** — `-sync-interval` pull (and inbound serve) recovers missed notifies and offline peers. Late joiners dial out and pull. Writers are not blocked waiting on dead peers.
- **Hash fast path** — Reuses the stored SHA-256 when size and mtime still match the index. Silent content rewrites that preserve mtime are not detected until another field changes.
- **Delta** — Adler-style rolling weak checksums and MD5 strong match per block; full-file SHA-256 is authoritative after apply. Whole-file buffers are used for transfers (default max 64 MiB per file).
- **Concurrency** — Notify fan-out is high-parallelism with no batch barrier on the main loop. Pulls use a separate memory-aware concurrency cap. Local reconcile and peer apply commits share one mutex; network transfer for a content apply runs unlocked (re-LWW on commit).
- **Protocol** — Length-prefixed JSON headers with optional binary payloads over a single **QUIC** stream per session (`hello`, `notify`, manifest/file/delta, `sync_done`). Wire version **1** is pull-oriented (no reverse-pull) and includes notify. Hello rejects other non-zero versions. QUIC uses ephemeral self-signed TLS (clients skip verify); peer trust is the tailnet mesh, not a public CA.
- **Conflicts** — Last-writer-wins on `updated_at`; equal clocks use a stable total order (deletion, hash, mode, mtime) so peers converge.
- **Metadata** — Mode and mtime are synchronized end-to-end; peers adopt metadata when the same content hash wins LWW.
- **Networking** — Host mode binds only to Tailscale addresses (not `0.0.0.0`), bootstraps peers via LocalAPI status + hot set, and dials with the host network stack (routed by `tailscaled`).
- **Sync-tree confinement** — File I/O under `-dir` (scan, serve, apply, deletes) uses Go’s [`os.Root`](https://pkg.go.dev/os#Root) so path traversal and symlink escapes cannot reach outside the sync directory; index/state paths under `-state` are separate trusted local storage. Peer paths under `.tailsync` / `.tailsync-*` are rejected so the default state dir cannot be written via sync. On multi-party tailnets, prefer an explicit `-state` path outside `-dir`.

State directories under the sync tree named `.tailsync` or `.tailsync-*` are ignored by the scanner and cannot be applied from peers.

## Embedding (library API)

The public Go package [`deedles.dev/tailsync/daemon`](https://pkg.go.dev/deedles.dev/tailsync/daemon) is the primary library API for embedding tailsync (still **alpha**; may change without compatibility guarantees, same as the rest of the project). The CLI (`cmd/tailsync`) is a thin wrapper around the same package.

### Daemon API overview

| Go | Role |
|----|------|
| `daemon.Config` | Settings: `Dir`, `StateDir`, `Hostname`, `AuthKey`, `Port`, `Peers`, `ServiceName`, intervals, `DisableWatch`, `BlockSize`, `DialTimeout`, `TombstoneTTL`, `NetMode`, `Logger`, `OnReady`, `OnAuthURL`, … |
| `daemon.New(cfg)` | Validates config and returns a stopped `*Daemon` (does not start networking) |
| `(*Daemon).Run(ctx)` | Brings up networking and runs until `ctx` is canceled or a fatal error |
| `(*Daemon).InjectNetworkChange()` | After host connectivity updates in **tsnet** mode, inject a netmon event (no-op if not running / not yet installed) |
| `daemon.DefaultPort`, `DefaultScanInterval`, `DefaultSyncInterval`, `DefaultWatchDebounce`, `DefaultBlockSize`, `DefaultDialTimeout`, `DefaultTombstoneTTL`, … | Effective defaults when zero values are left in `Config` |
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
