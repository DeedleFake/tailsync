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

By default, tailsync uses the system **`tailscaled`** (LocalAPI). It does not register a separate machine in the Tailscale admin console; it is just a process on the existing node. It listens on TCP port `5960` on the host’s Tailscale IP(s) and:

1. Watches the sync directory for filesystem events (debounced), with a periodic full rescan as a safety net, and reconciles against the on-disk index (adds, modifies, and offline deletions).
2. When local index content changes, opens an immediate **bidirectional** peer session (both sides pull on one connection, coalesced); a periodic sync interval remains as catch-up for offline peers.
3. Merges remote manifests using last-writer-wins on `updated_at`.

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
| `-service` | (empty) | Only dial peers whose hostname or DNS name contains this substring; **empty discovery dials all online peers** (see [Peer discovery](#peer-discovery)) |
| `-port` | `5960` | TCP port for peer connections |
| `-authkey` | `$TS_AUTHKEY` | Tailscale auth key for **`-tsnet`** only (optional if tsnet state already exists) |
| `-peers` | (discover) | Comma-separated `host:port` peers (skips discovery). Prefer this or `-service` when the tailnet has devices not running tailsync |
| `-scan-interval` | `30s` | Safety-net full rescan period (FS watch handles most local edits) |
| `-sync-interval` | `45s` | Backup peer sync period (local changes also open a bidirectional session) |
| `-watch-debounce` | `300ms` | Debounce wait after FS events before reconcile (`0` = default) |
| `-no-watch` | `false` | Disable filesystem watching; rely on `-scan-interval` only |
| `-block-size` | `4096` | Delta block size |
| `-dial-timeout` | `5s` (`daemon.DefaultDialTimeout`) | Max wait for each outbound peer dial (`0` = daemon default); caps waits on nodes not listening |
| `-tsnet` | `false` | Use embedded tsnet instead of host `tailscaled` |
| `-plain` | `false` | Plain TCP on `127.0.0.1` (requires `TAILSYNC_TESTING=1`) |
| `-v` | `false` | Debug logging |

`-plain` and `-tsnet` are mutually exclusive.

### Peer discovery

With host or tsnet mode and no `-peers` list, tailsync dials online tailnet peers on `-port` each sync interval, using Tailscale IPs from status (falling back to MagicDNS when needed). By default that includes **every** online peer—phones, TVs, unrelated servers—not only machines running tailsync. That is fine on a small personal tailnet, but nodes that are online and not listening for tailsync still consume a dial attempt each batch (capped by `-dial-timeout`, default 5s) and can delay outbound sync until those dials finish. Inbound connections still work in that case (e.g. phone→PC succeeds while PC→phone waits), so prefer:

- `-peers host:port,...` to pin exact addresses and skip discovery (recommended when only a few machines sync), or
- `-service <substring>` to only dial hosts whose Tailscale hostname or DNS name contains that string (for example `-service tailsync` with tsnet names like `tailsync-*`).

```bash
# two machines (each uses its host Tailscale identity)
tailsync -dir ~/shared   # machine a
tailsync -dir ~/shared   # machine b

# pin peers explicitly (avoids dialing the rest of the tailnet)
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
- **FS watch + debounce** — Local edits are detected via recursive filesystem events (debounced, default 300 ms), then reconciled. Paths under `.tailsync` / `.tailsync-*` are ignored. If watching fails to start (unsupported platform/permissions), tailsync logs a warning and falls back to timer-only scanning.
- **Scan** — Walks regular files only; live index entries missing on disk become tombstones (offline deletion). Empty directories and symlinks are not synced. `-scan-interval` remains a full safety-net rescan when watch is active.
- **Sync-on-change** — When reconcile applies peer-visible local index changes, a coalesced bidirectional peer session is opened: the dialer pulls the peer’s manifest, then the peer reverse-pulls the dialer’s, so a local write is delivered without waiting for the peer’s `-sync-interval`. That interval remains backup/catch-up for offline peers. End-to-end lag is typically on the order of watch debounce plus one dial/RTT (not a full sync interval).
- **Hash fast path** — Reuses the stored SHA-256 when size and mtime still match the index. Silent content rewrites that preserve mtime are not detected until another field changes.
- **Delta** — Adler-style rolling weak checksums and MD5 strong match per block; full-file SHA-256 is authoritative after apply. Whole-file buffers are used for transfers (default max 64 MiB per file).
- **Concurrency** — Local reconcile and peer apply share one mutex, including during network transfers (correctness over throughput; a slow peer can delay scans).
- **Protocol** — Length-prefixed JSON headers with optional binary payloads over a single TCP session.
- **Conflicts** — Last-writer-wins on `updated_at`; equal clocks use a stable total order (deletion, hash, mode, mtime) so peers converge.
- **Metadata** — Mode and mtime are synchronized end-to-end; peers adopt metadata when the same content hash wins LWW.
- **Networking** — Host mode binds only to Tailscale addresses (not `0.0.0.0`), discovers peers via LocalAPI status, and dials with the host network stack (routed by `tailscaled`).
- **Sync-tree confinement** — File I/O under `-dir` (scan, serve, apply, deletes) uses Go’s [`os.Root`](https://pkg.go.dev/os#Root) so path traversal and symlink escapes cannot reach outside the sync directory; index/state paths under `-state` are separate trusted local storage. Peer paths under `.tailsync` / `.tailsync-*` are rejected so the default state dir cannot be written via sync. On multi-party tailnets, prefer an explicit `-state` path outside `-dir`.

State directories under the sync tree named `.tailsync` or `.tailsync-*` are ignored by the scanner and cannot be applied from peers.

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
| `Config` | Settings: `Dir`, `StateDir`, `Hostname`, `AuthKey`, `Port`, `Peers`, `ServiceName`, `ScanIntervalMs`, `SyncIntervalMs`, `WatchDebounceMs`, `DisableWatch`, `BlockSize`, `DialTimeoutMs`, `NetMode` |
| `NewNode(cfg)` | Validates config and returns a stopped `Node` |
| `Node.Start()` / `Stop()` / `IsRunning()` | Lifecycle. `Start` blocks until listening succeeds or fails; call it off the main thread |
| `Node.SetListener(EventListener)` | Optional JSON event callbacks (logs, status, auth); handlers must return quickly |
| `Node.StatusJSON()` | Snapshot for UI (never includes `AuthKey`; zero config fields are shown as effective defaults; includes `phase`; may include `needs_login` / `auth_url`) |
| `Version()` | Module or build version string |
| `SetNetworkInterfacesJSON` / `SetNetworkInterfaces` | Supply host interfaces for tsnet (required on Android API 30+ before `Start`) |
| `SetDefaultRouteInterface` / `SetDefaultGateway` | Default route/gateway from `ConnectivityManager` / `LinkProperties` |
| `NotifyNetworkChange` / `Node.NotifyNetworkChange` | After interface/route updates while running, inject a netmon event (no-op if not running) |

`NetMode` values: `"tsnet"` (default), `"host"`, `"plain"` (localhost TCP for tests only).

`IsRunning()` is true while starting, serving, or stopping (resources may still be held after a timed-out `Stop`). `StatusJSON`’s `running` field is true only while serving after a successful `Start`. `phase` is one of `idle`, `starting`, `running`, or `stopping`.

### Network interfaces on Android (required for tsnet)

On **Android API 30+**, Go’s `net.Interfaces()` fails with permission errors. tsnet/netmon will not start correctly unless the app supplies interfaces **before** `node.start()` (and updates them when connectivity changes).

The `INTERNET` permission is still required for sockets; it does **not** fix `net.Interfaces` alone.

**Required Kotlin flow**

1. Use `ConnectivityManager` / `LinkProperties` to build the interface list (name, index, flags, MTU, address CIDRs) and the default route interface name + gateway IP.
2. Call `SetNetworkInterfacesJSON` (or the `NetworkInterfaceList` builder + `SetNetworkInterfaces`), `SetDefaultRouteInterface`, and `SetDefaultGateway` **before** `node.start()`.
3. On network callbacks (`NetworkCallback`, default-network changes, etc.), update the same APIs again and call `NotifyNetworkChange()` (or `node.notifyNetworkChange()`). Snapshot updates are visible to the getter immediately; `NotifyNetworkChange` wakes netmon after `tsnet.Up` has installed the monitor (during the long Up/auth window it is a no-op, then the daemon fires a catch-up inject).

Interface JSON example:

```json
[
  {"name":"wlan0","index":21,"flags":51,"mtu":1500,"addrs":["192.168.1.2/24","fe80::1/64"]},
  {"name":"lo","index":1,"flags":5,"mtu":65536,"addrs":["127.0.0.1/8"]}
]
```

`flags` are Go `net.Flags` bits (`1=Up`, `2=Broadcast`, `4=Loopback`, `8=PointToPoint`, `16=Multicast`, `32=Running`). Mirror OS flags when possible; live uplinks should include `Up|Running` (example `51` = `Up|Broadcast|Multicast|Running`). Prefer including at least loopback and the active uplink; an empty list is accepted but can break tsnet bring-up.

`SetDefaultGateway` rejects non-empty strings that are not valid IP addresses (empty string still clears).

**Multi-node:** package-level `NotifyNetworkChange` targets only the most recently started `Node`. If you run more than one node in-process, call `node.notifyNetworkChange()` on each.

Desktop/CLI is unchanged: if the app never sets interfaces, netmon keeps using `net.Interfaces()`.

### Kotlin example

```kotlin
// After adding tailsync.aar to the Android app module.
val cfg = Config().apply {
    dir = context.filesDir.resolve("sync").absolutePath
    // Persist StateDir across runs so browser login is only needed once.
    stateDir = context.filesDir.resolve("tailsync-state").absolutePath
    hostname = "tailsync-phone"
    // Optional: pre-provisioned auth key. Leave empty for browser login on first run.
    // authKey = BuildConfig.TS_AUTHKEY
    // netMode defaults to "tsnet"
}
val node = Mobile.newNode(cfg)
val mainHandler = Handler(Looper.getMainLooper())
node.setListener(EventListener { eventJSON ->
    // Called from a Go background thread — keep this fast (no network/disk/UI).
    mainHandler.post {
        // parse JSON: type = log | status | error | auth
        val obj = JSONObject(eventJSON)
        when (obj.getString("type")) {
            "auth" -> {
                // Interactive login needed — open Custom Tab / browser while start() waits.
                val url = obj.getString("url")
                val intent = CustomTabsIntent.Builder().build()
                intent.launchUrl(context, Uri.parse(url))
            }
            else -> Log.i("tailsync", eventJSON)
        }
    }
})

// Supply interfaces from ConnectivityManager BEFORE start (API 30+).
// Build JSON from LinkProperties / NetworkInterface; flags = Go net.Flags bits
// (include FlagRunning=32 on live uplinks when the OS reports it).
fun publishNetworkToGo(ifacesJson: String, defaultIf: String, gateway: String) {
    Mobile.setNetworkInterfacesJSON(ifacesJson)
    Mobile.setDefaultRouteInterface(defaultIf) // "" if network lost
    Mobile.setDefaultGateway(gateway)         // "" if none / lost; invalid IP throws
}

// Call once before start, and again from NetworkCallback when paths change.
publishNetworkToGo(currentIfacesJson(), currentDefaultIf(), currentGateway())

// From a foreground service — never call start() on the main thread
// (tsnet bring-up / browser login can block long enough to ANR).
serviceScope.launch(Dispatchers.IO) {
    try {
        node.start() // blocks until listening or failure; auth events fire while waiting
    } catch (e: Exception) {
        Log.e("tailsync", "start failed", e)
    }
}

// On ConnectivityManager network callbacks (after updating interfaces/route):
publishNetworkToGo(currentIfacesJson(), currentDefaultIf(), currentGateway())
// Prefer node.notifyNetworkChange() if multiple Nodes may exist in-process.
Mobile.notifyNetworkChange() // package-level: most recently started node only

serviceScope.launch(Dispatchers.IO) {
    node.stop() // no-op if already stopped; call when the service is destroyed
}
```

**Authentication (tsnet)**

| Situation | What happens |
|-----------|----------------|
| `AuthKey` set and valid | Silent enroll; no `"auth"` event |
| Existing tsnet state under `StateDir` (prior successful login) | Silent reconnect; no `"auth"` event |
| Empty `AuthKey`, no enrolled state | tsnet starts interactive login; emits `{"type":"auth","url":"..."}` while `Start` blocks so the app can open a browser / Custom Tab |

After the user completes login in the browser, `Start` finishes when the node is `Running`. Keep the same `StateDir` on later launches so the node does not re-prompt.

`StatusJSON` may include `needs_login` and `auth_url` while interactive login is in progress (never includes `AuthKey`).

**Notes**

- Paths must be absolute and writable by the app process.
- Call `Stop` when the service is destroyed so the embedded node and goroutines exit.
- Run `start()` / `stop()` off the main thread; keep `OnEvent` non-blocking (post to the main thread only for UI).
- Do not log or ship auth keys. Mobile events redact secret-like **attribute keys**; free-text log messages are not scrubbed.
- Zero `Port`, interval, or `BlockSize` mean daemon defaults; `StatusJSON` reports effective values (for example port `5960`).
- For tsnet on Android API 30+, set network interfaces and default route **before** `start()`; update + `notifyNetworkChange` on connectivity changes (see above).

## Development

```bash
go mod tidy
go test -vet=all ./...
go fmt ./...
go tool modernize ./...
go tool staticcheck ./...
```

`modernize` and `staticcheck` are module tools (see the `tool` block in `go.mod`). CI runs the same checks (`.github/workflows/ci.yml`), including a `go mod tidy` drift check. Android SDK is not required for `go test`; plain-mode tests exercise the mobile API on localhost.
