# tailsync

Synchronize a directory between your Tailscale machines.

tailsync runs a small daemon on each machine. Instances discover each other on your tailnet (or via an explicit peer list), exchange file manifests, and pull missing or updated files. Transfers use rsync-style block signatures so only changed regions are sent when a local basis exists. A persistent local index records known file state so deletions made while the daemon was stopped are still detected and propagated.

## Install

```bash
go install deedles.dev/tailsync/cmd/tailsync@latest
```

Or build from a checkout:

```bash
go build -o tailsync ./cmd/tailsync
```

## Usage

On each machine (with [Tailscale](https://tailscale.com/) already running):

```bash
tailsync -dir /path/to/shared
```

**By default**, tailsync uses the **host `tailscaled`** (LocalAPI). It does **not** register a separate machine in the admin console; it is just a process on the existing node. It binds TCP port `5960` on the host’s Tailscale IP(s) and periodically:

1. Scans the sync directory and reconciles against the on-disk index (adds / modifies / offline deletions)
2. Connects to online tailnet peers (or `-peers`) and merges remote manifests (last-writer-wins by `updated_at`)

Keep host clocks roughly in sync (NTP). LWW uses wall-clock `updated_at`; equal-timestamp ties use a stable total order (deletion preference, then content hash, mode, then mtime).

**Metadata synced** for regular files: permission bits (`mode`) and modification time (`mtime`), including touch-only changes. Content hash and size are authoritative for file body. Access time (atime), ownership, xattrs, and ACLs are not synchronized.

### Network modes

| Mode | Flag | Behavior |
|------|------|----------|
| **host** (default) | *(none)* | Use system Tailscale daemon. Listen on host Tailscale IP(s) (IPv4 and IPv6 when bindable; failed address families are skipped). Dial peers by Tailscale IP (MagicDNS only if no IP is known). No auth key. Requires `tailscaled` running and logged in. |
| **tsnet** | `-tsnet` | Embedded [tsnet](https://pkg.go.dev/tailscale.com/tsnet) node: registers a **separate** machine on the tailnet. Useful in containers without host Tailscale. Supports `-hostname` / `-authkey`. |
| **plain** | `-plain` | Localhost TCP only (testing). Requires `TAILSYNC_TESTING=1`. |

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-dir` | (required) | Directory to synchronize |
| `-state` | `<dir>/.tailsync` | Index directory (also holds tsnet state when `-tsnet`) |
| `-hostname` | `tailsync-<os-hostname>` (tsnet only) | tsnet hostname on the tailnet; in host mode identity comes from LocalAPI Self |
| `-service` | (empty) | Optional discovery filter: only dial peers whose hostname/DNS **contains** this substring; empty means **all** online tailnet peers (see note below) |
| `-port` | `5960` | TCP port for peer connections |
| `-authkey` | `$TS_AUTHKEY` | Tailscale auth key for **`-tsnet`** only (optional if tsnet state already exists) |
| `-peers` | (discover) | Comma-separated `host:port` peers (skips discovery) |
| `-scan-interval` | `30s` | Local directory rescan period |
| `-sync-interval` | `45s` | Peer sync period |
| `-block-size` | `4096` | Delta block size |
| `-tsnet` | `false` | Use embedded tsnet node instead of host tailscaled |
| `-plain` | `false` | Plain TCP on `127.0.0.1` (requires `TAILSYNC_TESTING=1`) |
| `-v` | `false` | Debug logging |

`-plain` and `-tsnet` are mutually exclusive.

### Example (two machines on a tailnet)

```bash
# machine a (uses that host's Tailscale identity)
tailsync -dir ~/shared

# machine b
tailsync -dir ~/shared
```

Peers are dialed on `-port` using Tailscale IPs from LocalAPI status (falls back to MagicDNS names when a peer has no IP in status). By default **every online tailnet peer** is dialed each sync interval—including phones, TVs, and unrelated servers. That is fine on small personal tailnets; on larger ones prefer:

- `-service <substring>` to only dial hosts whose Tailscale hostname/DNS contains that string (e.g. `-service tailsync` with `-tsnet` names `tailsync-*`), or
- `-peers host:port,...` to pin exact addresses and skip discovery entirely.

```bash
tailsync -dir ~/shared -peers other-host:5960,100.x.y.z:5960
```

### Embedded tsnet (optional)

When the host has no Tailscale daemon (e.g. some containers):

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

- **Index** — JSON under `-state` with size, mtime, mode, content SHA-256, and deletion tombstones (GC’d after 30 days by default). **Tombstone GC risk:** after a tombstone is dropped, a lagging peer that never saw the delete can re-introduce the file; keep TTL longer than the maximum expected peer offline window
- **Scan** — Walks regular files only; index entries missing on disk become tombstones (offline deletion). Empty directories and symlinks are not synced (v1)
- **Hash fast path** — Reuses the stored SHA-256 when size and mtime still match the index (common tradeoff; silent content rewrites that preserve mtime are not detected until another field changes)
- **Delta** — Adler-style rolling weak checksums + MD5 strong match per block; full-file SHA-256 is authoritative after apply. Whole-file buffers are used for transfers (default max 64 MiB per file)
- **Concurrency** — Local reconcile and peer apply share one mutex, including during network transfers (v1 correctness over throughput; a slow peer can delay scans)
- **Protocol** — Length-prefixed JSON headers with optional binary payloads over a single TCP session
- **Conflicts** — Last-writer-wins on `updated_at`; equal clocks use a stable total order (deletion, hash, mode, mtime) so peers converge
- **Metadata** — Mode and mtime are synchronized end-to-end (scan detects mode-only and touch-only changes; peers adopt metadata when same content hash wins LWW)
- **Networking** — Default host mode binds only to Tailscale addresses (not `0.0.0.0`), discovers peers via LocalAPI status, and dials with the host network stack (routed by tailscaled)

State under the sync tree named `.tailsync` / `.tailsync-*` is ignored by the scanner.

## Development

```bash
go test ./...
go vet ./...
go fmt ./...
```
