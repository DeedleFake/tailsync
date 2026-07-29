# tailsync

[![Go Reference](https://pkg.go.dev/badge/deedles.dev/tailsync.svg)](https://pkg.go.dev/deedles.dev/tailsync)

Synchronize a directory between your Tailscale machines.

> **Warning:** tailsync is **alpha** software. The protocol, on-disk index format, flags, and APIs may change without compatibility guarantees. Do not rely on it as the sole copy of important data; keep independent backups.

tailsync is a small daemon that runs on each machine. Instances find each other on your tailnet, notify peers when local files change, and **pull** missing or updated content. Transfers use rsync-style block signatures so only changed regions are sent when possible. A local index under the state directory remembers known file state so deletions made while the daemon was stopped are still detected and propagated.

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

By default, tailsync uses the system **`tailscaled`**. It does not register a separate machine; it runs as a process on the existing Tailscale node. It listens with **QUIC** (UDP) on port `5960` on the host’s Tailscale IP(s).

Typical flow:

1. Watches the sync directory for changes (debounced, default 1 s), with a periodic full rescan as a safety net.
2. Discovers eligible online peers and keeps a persistent connection to each.
3. When local content changes, sends best-effort **notifies** to connected peers.
4. Peers **pull** manifests and file content; the pull (last-writer-wins) is authoritative.
5. Merges remote updates using wall-clock `updated_at` (keep host clocks roughly in sync via NTP).

For regular files, permission bits and modification time are synchronized. Content hash and size define file contents. Access time, ownership, extended attributes, and ACLs are **not** synchronized. Empty directories and symlinks are not synced.

### Network modes

| Mode | Flag | Behavior |
|------|------|----------|
| **host** (default) | *(none)* | Use system Tailscale. Listen on the host’s Tailscale IP(s). No auth key. Requires `tailscaled` running and logged in. |
| **tsnet** | `-tsnet` | Embed a [tsnet](https://pkg.go.dev/tailscale.com/tsnet) node that registers as a **separate** machine. Useful in containers without host Tailscale. Supports `-hostname` and `-authkey`. |
| **plain** | `-plain` | Localhost only, for testing. Requires `TAILSYNC_TESTING=1`. |

`-plain` and `-tsnet` are mutually exclusive.

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-dir` | (required) | Directory to synchronize |
| `-state` | `<dir>/.tailsync` | Index directory (also holds tsnet state when `-tsnet`) |
| `-hostname` | `tailsync-<os-hostname>` (tsnet only) | tsnet hostname; in host mode, identity comes from Tailscale |
| `-port` | `5960` | UDP port for peer connections |
| `-authkey` | `$TS_AUTHKEY` | Tailscale auth key for **`-tsnet`** only (optional if tsnet state already exists) |
| `-peers` | (discover) | Comma-separated `host:port` dial list; skips status discovery only. On host/tsnet, mesh trust still applies at Hello. |
| `-mesh-tag` | (none) | When **this** machine is tagged: required ACL tag peers must share (for example `tag:tailsync`). Listen fails if this machine is tagged and the flag is missing or not on Self, or if this machine is untagged and the flag is set. |
| `-scan-interval` | `30s` | Full rescan period (FS watch handles most local edits) |
| `-sync-interval` | `45s` | Backup peer pull period if a notify was missed |
| `-watch-debounce` | `1s` | Wait after filesystem events before reconciling |
| `-no-watch` | `false` | Disable filesystem watching; rely on `-scan-interval` only |
| `-block-size` | `4096` | Delta block size |
| `-dial-timeout` | `30s` | Max wait for each outbound discovery dial |
| `-tsnet` | `false` | Use embedded tsnet instead of host `tailscaled` |
| `-plain` | `false` | Localhost testing mode (`TAILSYNC_TESTING=1` required) |
| `-v` | `false` | Debug logging |

### Who connects to whom

On **host** and **tsnet**, mesh membership requires **mesh trust** for *this* machine. Trust applies both when selecting discovery dial targets and when accepting a Hello (WhoIs). A pin list does not relax that check.

- **Untagged machine** (typical laptop or desktop): other machines owned by the **same Tailscale user**. Tagged devices usually do **not** match (they are not user-owned in Tailscale’s model). Do **not** set `-mesh-tag` (listen fails if it is set).
- **Tagged machine** (typical server): other machines that carry the same **`-mesh-tag`** (for example `tag:tailsync`). Extra ACL tags on either side do not affect membership. If this machine is tagged, `-mesh-tag` is **required** and must appear on Self, or the daemon refuses to start.

Always denied: devices shared into the tailnet as share-only entries, and [Mullvad](https://tailscale.com/kb/1258/mullvad-exit-nodes) exit nodes.

**`-peers`** only sets which `host:port` addresses to dial and skips status-based discovery (useful for tests and fixed dial lists). On host/tsnet it does **not** bypass mesh trust: untrusted pins still fail Hello and will not join the mesh. To run without Tailscale identity checks, use **`-plain`** (localhost testing only; requires `TAILSYNC_TESTING=1`).

**Laptop ↔ tagged server** is not supported under this policy. Run tailsync on a consistent class of machines (all your untagged devices under one user, or tagged servers that share a dedicated mesh tag such as `tag:tailsync`).

```bash
# two untagged machines (same Tailscale user)
tailsync -dir ~/shared   # machine a
tailsync -dir ~/shared   # machine b

# two servers with tag:tailsync (each machine must be tagged and pass -mesh-tag)
tailsync -dir /data/shared -mesh-tag tag:tailsync
```

Soft dial failures and retries are normal for online nodes that are not running tailsync; they do not block local writes.

### Embedded tsnet

When there is no system Tailscale daemon (for example some containers):

```bash
tailsync -tsnet -dir ~/shared -hostname tailsync-a
# optional: -authkey $TS_AUTHKEY
```

This registers a separate node named with `-hostname` (default `tailsync-<os-hostname>`). Keep a stable `-state` directory so the node does not need to re-authenticate every launch.

### Local testing without Tailscale

Use `-plain` so Hello does not require Tailscale identity. Pin the other process with `-peers` (status discovery does not apply in this mode):

```bash
# terminal 1
TAILSYNC_TESTING=1 tailsync -plain -dir /tmp/sync-a -state /tmp/state-a -port 5960 -peers 127.0.0.1:5961 -hostname a

# terminal 2
TAILSYNC_TESTING=1 tailsync -plain -dir /tmp/sync-b -state /tmp/state-b -port 5961 -peers 127.0.0.1:5960 -hostname b
```

## Behavior notes

- **State** — Index JSON under `-state` (size, mtime, mode, content hash, deletion markers). Deletion markers expire after 30 days by default; if a peer stays offline longer than that, it might reintroduce a deleted file.
- **Ignored paths** — `.tailsync` and `.tailsync-*` under the sync tree are never scanned or accepted from peers. Prefer `-state` outside `-dir` on multi-party setups.
- **Conflicts** — Last-writer-wins on `updated_at`; equal timestamps use a stable tie-break so peers converge.
- **Limits** — Whole-file buffers for transfers (default max 64 MiB per file). Only regular files are synced.
- **Hash fast path** — Reuses a stored content hash when size and mtime still match. Silent rewrites that keep the same mtime may not be detected until something else changes.
- **Networking** — Host mode listens only on Tailscale addresses (not the public Internet). Trust is the tailnet mesh plus the rules above, not a public CA.

## Embedding (library API)

The package [`deedles.dev/tailsync/daemon`](https://pkg.go.dev/deedles.dev/tailsync/daemon) is the library API (still **alpha**). The `tailsync` CLI is a thin wrapper around it.

| Go | Role |
|----|------|
| `daemon.Config` | Settings (`Dir`, `StateDir`, `NetMode`, `MeshTag`, intervals, `OnReady`, `OnAuthURL`, …) |
| `daemon.New(cfg)` | Validate config; returns a stopped `*Daemon` |
| `(*Daemon).Run(ctx)` | Run until `ctx` is canceled or a fatal error |
| `(*Daemon).InjectNetworkChange()` | After connectivity changes in **tsnet** mode (e.g. mobile networks) |
| `daemon.NetModeHost` / `NetModeTSNet` / `NetModePlain` | Same modes as the CLI |

```go
cfg := daemon.Config{
	Dir:     "/path/to/shared",
	NetMode: daemon.NetModeHost, // or NetModeTSNet / NetModePlain
}
d, err := daemon.New(cfg)
if err != nil {
	return err
}
return d.Run(ctx)
```

Zero values for port, intervals, and block size use the package defaults (for example port `5960`).

### Mobile and other embedders

Use **`daemon`** from your app (or a small bindable wrapper you own). On Android, prefer **`NetModeTSNet`**: there is usually no system `tailscaled`. Pass absolute, writable paths (for example app-private storage). For interactive login, set `Config.OnAuthURL` so the UI can open the browser URL while bring-up is waiting. Keep `StateDir` stable across launches.

On Android API 30+, supply network interface information to the process (via your platform layer and `InjectNetworkChange` after connectivity updates). See the [`daemon` package docs](https://pkg.go.dev/deedles.dev/tailsync/daemon) for details.

## Development

```bash
go mod tidy
go test -vet=all ./...
go fmt ./...
go tool modernize ./...
go tool staticcheck ./...
```

`modernize` and `staticcheck` are declared as module tools in `go.mod`.
