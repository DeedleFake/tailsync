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
	"time"

	"deedles.dev/tailsync/internal/daemon"
)

func main() {
	var (
		dir       = flag.String("dir", "", "directory to synchronize (required)")
		stateDir  = flag.String("state", "", "state directory for index (and tsnet state when -tsnet); default: <dir>/.tailsync")
		hostname  = flag.String("hostname", "", "tsnet hostname when -tsnet (default: tailsync-<os-hostname>); ignored for protocol identity in host mode")
		service   = flag.String("service", "", "optional discovery filter: only dial hosts whose name contains this substring (empty = all online peers)")
		port      = flag.Int("port", 5960, "TCP port on the tailnet (or localhost with -plain)")
		authKey   = flag.String("authkey", "", "Tailscale auth key for -tsnet (or set TS_AUTHKEY); unused in host mode")
		scanEvery = flag.Duration("scan-interval", 30*time.Second, "how often to rescan the local directory")
		syncEvery = flag.Duration("sync-interval", 45*time.Second, "how often to sync with peers")
		blockSize = flag.Int("block-size", 4096, "rsync-style block size for delta transfers")
		peers     = flag.String("peers", "", "comma-separated peer addresses host:port (optional; default: discover via Tailscale)")
		useTSNet  = flag.Bool("tsnet", false, "use embedded tsnet node (registers a separate tailnet machine) instead of host tailscaled")
		plain     = flag.Bool("plain", false, "use plain TCP on 127.0.0.1 (requires TAILSYNC_TESTING=1)")
		verbose   = flag.Bool("v", false, "verbose debug logging")
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
		for _, p := range strings.Split(*peers, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				peerList = append(peerList, p)
			}
		}
	}

	cfg := daemon.Config{
		Dir:          *dir,
		StateDir:     *stateDir,
		Hostname:     *hostname,
		ServiceName:  *service,
		Port:         *port,
		AuthKey:      auth,
		ScanInterval: *scanEvery,
		SyncInterval: *syncEvery,
		BlockSize:    *blockSize,
		Logger:       log,
		NetMode:      mode,
		Peers:        peerList,
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
