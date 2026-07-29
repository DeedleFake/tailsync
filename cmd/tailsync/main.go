// Command tailsync synchronizes a directory across Tailscale machines.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"deedles.dev/tailsync/daemon"
)

func main() {
	var (
		dir           = flag.String("dir", "", "directory to synchronize (required)")
		stateDir      = flag.String("state", "", "state directory for index (and tsnet state when -tsnet); default: <dir>/.tailsync")
		hostname      = flag.String("hostname", "", "tsnet hostname when -tsnet (default: tailsync-<os-hostname>); ignored for protocol identity in host mode")
		port          = flag.Int("port", daemon.DefaultPort, "UDP port for QUIC peer sessions on the tailnet (or localhost with -plain)")
		authKey       = flag.String("authkey", "", "Tailscale auth key for -tsnet (or set TS_AUTHKEY); unused in host mode")
		scanEvery     = flag.Duration("scan-interval", daemon.DefaultScanInterval, "safety-net full rescan period (FS watch handles most local edits)")
		syncEvery     = flag.Duration("sync-interval", daemon.DefaultSyncInterval, "backup peer pull period (local changes fan out best-effort notifies; peers pull independently)")
		watchDebounce = flag.Duration("watch-debounce", 0, "debounce wait after FS events before reconcile (0 = default)")
		noWatch       = flag.Bool("no-watch", false, "disable filesystem watching; rely on -scan-interval only")
		blockSize     = flag.Int("block-size", 0, "rsync-style block size for delta transfers (0 = daemon default)")
		dialTimeout   = flag.Duration("dial-timeout", 0, fmt.Sprintf("timeout for outbound peer dials (0 = default %s); caps waits on nodes not running tailsync", daemon.DefaultDialTimeout))
		peers         = flag.String("peers", "", "comma-separated peer addresses host:port (test/override only); when empty, discover via mesh trust (untagged Self: same UserID; tagged Self: -tag-match; excludes sharees, Mullvad)")
		tagMatch      = flag.String("tag-match", "intersect", "when Self is tagged, how peer tags must relate: intersect (default), equal, or contains (peer has all of Self's tags); ignored when Self is untagged")
		useTSNet      = flag.Bool("tsnet", false, "use embedded tsnet node (registers a separate tailnet machine) instead of host tailscaled")
		plain         = flag.Bool("plain", false, "use plain QUIC on 127.0.0.1 (requires TAILSYNC_TESTING=1)")
		verbose       = flag.Bool("v", false, "verbose debug logging")
	)
	flag.Parse()

	if *dir == "" {
		fmt.Fprintln(os.Stderr, "error: -dir is required")
		flag.Usage()
		os.Exit(2)
	}

	if *plain && *useTSNet {
		fmt.Fprintln(os.Stderr, "error: -plain and -tsnet are mutually exclusive")
		os.Exit(2)
	}
	if *plain && os.Getenv("TAILSYNC_TESTING") != "1" {
		fmt.Fprintln(os.Stderr, "error: -plain requires TAILSYNC_TESTING=1 (testing only)")
		os.Exit(2)
	}
	tagMode, err := daemon.ParseTagMatchMode(*tagMatch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	mode := daemon.NetModeHost
	switch {
	case *plain:
		mode = daemon.NetModePlain
	case *useTSNet:
		mode = daemon.NetModeTSNet
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	auth := *authKey
	if auth == "" {
		auth = os.Getenv("TS_AUTHKEY")
	}

	var peerList []string
	if *peers != "" {
		for p := range strings.SplitSeq(*peers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peerList = append(peerList, p)
			}
		}
	}

	cfg := daemon.Config{
		Dir:           *dir,
		StateDir:      *stateDir,
		Hostname:      *hostname,
		Port:          *port,
		AuthKey:       auth,
		ScanInterval:  *scanEvery,
		SyncInterval:  *syncEvery,
		WatchDebounce: *watchDebounce,
		DisableWatch:  *noWatch,
		BlockSize:     *blockSize,
		DialTimeout:   *dialTimeout,
		Logger:        log,
		NetMode:       mode,
		Peers:         peerList,
		TagMatch:      tagMode,
	}

	d, err := daemon.New(cfg)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := d.Run(ctx); err != nil {
		log.Error("run", "err", err)
		os.Exit(1)
	}
}
