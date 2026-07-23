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

	"deedles.dev/tailsync/internal/daemon"
)

func main() {
	var (
		dir           = flag.String("dir", "", "directory to synchronize (required)")
		stateDir      = flag.String("state", "", "state directory for index (and tsnet state when -tsnet); default: <dir>/.tailsync")
		hostname      = flag.String("hostname", "", "tsnet hostname when -tsnet (default: tailsync-<os-hostname>); ignored for protocol identity in host mode")
		service       = flag.String("service", "", "discovery filter: only dial hosts whose name contains this substring; empty dials all online peers (use with -peers on large tailnets; non-tailsync nodes waste dial time)")
		port          = flag.Int("port", daemon.DefaultPort, "TCP port on the tailnet (or localhost with -plain)")
		authKey       = flag.String("authkey", "", "Tailscale auth key for -tsnet (or set TS_AUTHKEY); unused in host mode")
		scanEvery     = flag.Duration("scan-interval", daemon.DefaultScanInterval, "safety-net full rescan period (FS watch handles most local edits)")
		syncEvery     = flag.Duration("sync-interval", daemon.DefaultSyncInterval, "backup peer sync period (local changes also open a bidirectional sync session)")
		watchDebounce = flag.Duration("watch-debounce", 0, "debounce wait after FS events before reconcile (0 = default)")
		noWatch       = flag.Bool("no-watch", false, "disable filesystem watching; rely on -scan-interval only")
		blockSize     = flag.Int("block-size", 0, "rsync-style block size for delta transfers (0 = daemon default)")
		dialTimeout   = flag.Duration("dial-timeout", 0, fmt.Sprintf("timeout for outbound peer dials (0 = default %s); caps waits on nodes not running tailsync", daemon.DefaultDialTimeout))
		peers         = flag.String("peers", "", "comma-separated peer addresses host:port; when empty, discovery dials all online tailnet peers (prefer explicit list or -service if some nodes lack tailsync)")
		useTSNet      = flag.Bool("tsnet", false, "use embedded tsnet node (registers a separate tailnet machine) instead of host tailscaled")
		plain         = flag.Bool("plain", false, "use plain TCP on 127.0.0.1 (requires TAILSYNC_TESTING=1)")
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
		ServiceName:   *service,
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
