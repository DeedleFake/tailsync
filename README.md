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

On each machine:

```bash
tailsync -dir /path/to/shared
```

The daemon joins your tailnet with [tsnet](https://pkg.go.dev/tailscale.com/tsnet) (hostname `tailsync-<os-hostname>` by default), listens on TCP port `5960`, and periodically:

1. Scans the sync directory and reconciles against the on-disk index (adds / modifies / offline deletions)
2. Connects to online tailnet peers (or `-peers`) and merges remote manifests (last-writer-wins by `updated_at`)

Keep host clocks roughly in sync (NTP). LWW uses wall-clock `updated_at`; equal-timestamp ties use a stable total order (deletion preference, then content hash).

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-dir` | (required) | Directory to synchronize |
| `-state` | `<dir>/.tailsync` | Index + tsnet state directory |
| `-hostname` | `tailsync-<os-hostname>` | tsnet hostname on the tailnet |
| `-service` | (empty) | Optional discovery filter: only dial peers whose hostname/DNS **contains** this substring; empty means all online peers |
| `-port` | `5960` | TCP port for peer connections |
| `-authkey` | `$TS_AUTHKEY` | Tailscale auth key (optional if state already exists) |
| `-peers` | (discover) | Comma-separated `host:port` peers (skips discovery) |
| `-scan-interval` | `30s` | Local directory rescan period |
| `-sync-interval` | `45s` | Peer sync period |
| `-block-size` | `4096` | Delta block size |
| `-plain` | `false` | Plain TCP on `127.0.0.1` (requires `TAILSYNC_TESTING=1`) |
| `-v` | `false` | Debug logging |

### Example (two machines on a tailnet)

```bash
# machine a
tailsync -dir ~/shared -hostname tailsync-a

# machine b
tailsync -dir ~/shared -hostname tailsync-b
```

Peers are dialed via MagicDNS / Tailscale IPs on `-port`. By default every online tailnet peer is considered; set `-service tailsync` to only dial names containing that substring (useful if default hostnames are `tailsync-*`). Custom `-hostname` values do not need a service filter unless you want one. To pin addresses:

```bash
tailsync -dir ~/shared -peers tailsync-a:5960,100.x.y.z:5960
```

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
- **Conflicts** — Last-writer-wins on `updated_at`; equal clocks use a stable hash/deletion order so peers converge

State under the sync tree named `.tailsync` / `.tailsync-*` is ignored by the scanner.

## Development

```bash
go test ./...
go vet ./...
go fmt ./...
```
