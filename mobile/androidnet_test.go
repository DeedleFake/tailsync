package mobile

import (
	"net"
	"sync"
	"testing"
	"time"

	"tailscale.com/net/netmon"
)

// flagsWlanLive is Up|Broadcast|Multicast|Running (51), a typical live uplink.
const flagsWlanLive = int(net.FlagUp | net.FlagBroadcast | net.FlagMulticast | net.FlagRunning)

func TestSetNetworkInterfacesJSON(t *testing.T) {
	t.Cleanup(resetAndroidNetForTest)
	resetAndroidNetForTest()

	const jsonStr = `[
		{"name":"wlan0","index":21,"flags":51,"mtu":1500,"addrs":["192.168.1.2/24","fe80::1/64"]},
		{"name":"lo","index":1,"flags":5,"mtu":65536,"addrs":["127.0.0.1/8"]}
	]`
	if err := SetNetworkInterfacesJSON(jsonStr); err != nil {
		t.Fatalf("SetNetworkInterfacesJSON: %v", err)
	}

	list, err := netmon.GetInterfaceList()
	if err != nil {
		t.Fatalf("GetInterfaceList: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("got %d interfaces, want 2", len(list))
	}

	wlan := list[0]
	if wlan.Name != "wlan0" || wlan.Index != 21 || wlan.MTU != 1500 {
		t.Fatalf("wlan0 fields: name=%q index=%d mtu=%d", wlan.Name, wlan.Index, wlan.MTU)
	}
	if wlan.Flags != net.Flags(flagsWlanLive) {
		t.Fatalf("wlan0 flags: got %v want %d", wlan.Flags, flagsWlanLive)
	}
	if wlan.AltAddrs == nil {
		t.Fatal("wlan0 AltAddrs must be non-nil")
	}
	if len(wlan.AltAddrs) != 2 {
		t.Fatalf("wlan0 addrs: got %d want 2", len(wlan.AltAddrs))
	}
	ipn0, ok := wlan.AltAddrs[0].(*net.IPNet)
	if !ok {
		t.Fatalf("addr0 type %T", wlan.AltAddrs[0])
	}
	if got := ipn0.IP.String(); got != "192.168.1.2" {
		t.Fatalf("addr0 IP: got %s want 192.168.1.2", got)
	}
	// Addrs() must use AltAddrs, not stdlib.
	addrs, err := wlan.Addrs()
	if err != nil {
		t.Fatalf("Addrs: %v", err)
	}
	if len(addrs) != 2 {
		t.Fatalf("Addrs len %d", len(addrs))
	}

	lo := list[1]
	if lo.Name != "lo" || !lo.IsLoopback() {
		t.Fatalf("lo: name=%q loopback=%v flags=%v", lo.Name, lo.IsLoopback(), lo.Flags)
	}
}

func TestSetNetworkInterfacesBuilder(t *testing.T) {
	t.Cleanup(resetAndroidNetForTest)
	resetAndroidNetForTest()

	list := NewNetworkInterfaceList()
	list.Add("rmnet0", 3, int(net.FlagUp|net.FlagMulticast|net.FlagRunning), 1400, "10.1.2.3/32,fe80::abcd/64")
	if list.Len() != 1 {
		t.Fatalf("Len=%d", list.Len())
	}
	if err := SetNetworkInterfaces(list); err != nil {
		t.Fatalf("SetNetworkInterfaces: %v", err)
	}

	got, err := netmon.GetInterfaceList()
	if err != nil {
		t.Fatalf("GetInterfaceList: %v", err)
	}
	if len(got) != 1 || got[0].Name != "rmnet0" {
		t.Fatalf("got %+v", got)
	}
	if len(got[0].AltAddrs) != 2 {
		t.Fatalf("addrs: %v", got[0].AltAddrs)
	}
}

func TestSetNetworkInterfacesJSONErrors(t *testing.T) {
	t.Cleanup(resetAndroidNetForTest)
	resetAndroidNetForTest()

	if err := SetNetworkInterfacesJSON(`not-json`); err == nil {
		t.Fatal("expected parse error")
	}
	if err := SetNetworkInterfacesJSON(`[{"name":"","index":1}]`); err == nil {
		t.Fatal("expected empty name error")
	}
	if err := SetNetworkInterfacesJSON(`[{"name":"x","addrs":["not-a-cidr"]}]`); err == nil {
		t.Fatal("expected cidr error")
	}
}

func TestSetNetworkInterfacesEmptyList(t *testing.T) {
	t.Cleanup(resetAndroidNetForTest)
	resetAndroidNetForTest()

	if err := SetNetworkInterfacesJSON(`[]`); err != nil {
		t.Fatalf("empty array: %v", err)
	}
	list, err := netmon.GetInterfaceList()
	if err != nil {
		t.Fatalf("GetInterfaceList: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("want empty snapshot, got %d", len(list))
	}
	// Empty AltAddrs path via builder with no addrs still non-nil.
	bl := NewNetworkInterfaceList()
	bl.Add("empty", 1, int(net.FlagUp), 1500, "")
	if err := SetNetworkInterfaces(bl); err != nil {
		t.Fatal(err)
	}
	list, err = netmon.GetInterfaceList()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].AltAddrs == nil {
		t.Fatalf("AltAddrs should be non-nil empty, got %#v", list)
	}
	addrs, err := list[0].Addrs()
	if err != nil || len(addrs) != 0 {
		t.Fatalf("Addrs: %v %v", addrs, err)
	}
}

func TestInterfaceGetterFallbackWhenUnset(t *testing.T) {
	t.Cleanup(resetAndroidNetForTest)
	resetAndroidNetForTest()

	// No Android snapshot: should return real OS interfaces (at least loopback
	// on normal test hosts).
	list, err := netmon.GetInterfaceList()
	if err != nil {
		t.Fatalf("fallback GetInterfaceList: %v", err)
	}
	if len(list) == 0 {
		t.Fatal("expected at least one OS interface from fallback")
	}
	// Stdlib path: AltAddrs should be nil so Addrs uses the real interface.
	if list[0].AltAddrs != nil {
		t.Fatalf("fallback should not set AltAddrs, got %v", list[0].AltAddrs)
	}

	// After set, snapshot takes over even if empty.
	if err := SetNetworkInterfacesJSON(`[]`); err != nil {
		t.Fatal(err)
	}
	list, err = netmon.GetInterfaceList()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("after set empty, want 0 got %d", len(list))
	}
}

func TestGetterReturnsCopy(t *testing.T) {
	t.Cleanup(resetAndroidNetForTest)
	resetAndroidNetForTest()

	if err := SetNetworkInterfacesJSON(`[{"name":"a","index":1,"flags":1,"mtu":1000,"addrs":["10.0.0.1/24"]}]`); err != nil {
		t.Fatal(err)
	}
	a, err := netmon.GetInterfaceList()
	if err != nil {
		t.Fatal(err)
	}
	a[0].Name = "mutated"
	if len(a[0].AltAddrs) != 1 {
		t.Fatalf("addrs: %v", a[0].AltAddrs)
	}
	ipn, ok := a[0].AltAddrs[0].(*net.IPNet)
	if !ok {
		t.Fatalf("type %T", a[0].AltAddrs[0])
	}
	// Mutate IP bytes and mask on the returned copy.
	if len(ipn.IP) > 0 {
		ipn.IP[0] ^= 0xff
	}
	if len(ipn.Mask) > 0 {
		ipn.Mask[0] ^= 0xff
	}
	a[0].AltAddrs = a[0].AltAddrs[:0]

	b, err := netmon.GetInterfaceList()
	if err != nil {
		t.Fatal(err)
	}
	if b[0].Name != "a" {
		t.Fatalf("mutation leaked: name=%q", b[0].Name)
	}
	if len(b[0].AltAddrs) != 1 {
		t.Fatalf("mutation leaked addrs: %v", b[0].AltAddrs)
	}
	bipn := b[0].AltAddrs[0].(*net.IPNet)
	if got := bipn.IP.String(); got != "10.0.0.1" {
		t.Fatalf("IP mutation leaked: got %s", got)
	}
	ones, bits := bipn.Mask.Size()
	if ones != 24 || bits != 32 {
		t.Fatalf("mask mutation leaked: %d/%d", ones, bits)
	}
}

func TestSetDefaultRouteAndGateway(t *testing.T) {
	t.Cleanup(resetAndroidNetForTest)
	resetAndroidNetForTest()

	SetDefaultRouteInterface("wlan0")
	if err := SetDefaultGateway("192.168.1.1"); err != nil {
		t.Fatal(err)
	}
	androidNet.mu.Lock()
	if androidNet.routeIf != "wlan0" || androidNet.gateway != "192.168.1.1" {
		androidNet.mu.Unlock()
		t.Fatalf("route=%q gw=%q", androidNet.routeIf, androidNet.gateway)
	}
	androidNet.mu.Unlock()

	// Empty clears (network lost) is allowed.
	SetDefaultRouteInterface("")
	if err := SetDefaultGateway(""); err != nil {
		t.Fatal(err)
	}
	androidNet.mu.Lock()
	if androidNet.routeIf != "" || androidNet.gateway != "" {
		androidNet.mu.Unlock()
		t.Fatalf("expected cleared, got route=%q gw=%q", androidNet.routeIf, androidNet.gateway)
	}
	androidNet.mu.Unlock()

	// Invalid gateway rejected; state unchanged.
	if err := SetDefaultGateway("192.168.1.1"); err != nil {
		t.Fatal(err)
	}
	if err := SetDefaultGateway("not-an-ip"); err == nil {
		t.Fatal("expected error for invalid gateway")
	}
	androidNet.mu.Lock()
	if androidNet.gateway != "192.168.1.1" {
		androidNet.mu.Unlock()
		t.Fatalf("gateway should be unchanged after bad set, got %q", androidNet.gateway)
	}
	androidNet.mu.Unlock()
}

func TestNotifyNetworkChangeNoopWhenIdle(t *testing.T) {
	// Must not panic when nothing is registered.
	NotifyNetworkChange()
	var n *Node
	n.NotifyNetworkChange()
	n = &Node{}
	n.NotifyNetworkChange()
}

func TestInterfaceGetterConcurrent(t *testing.T) {
	t.Cleanup(resetAndroidNetForTest)
	resetAndroidNetForTest()

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n * 2)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			_ = SetNetworkInterfacesJSON(`[{"name":"wlan0","index":1,"flags":1,"mtu":1500,"addrs":["10.0.0.1/24"]}]`)
		}(i)
		go func() {
			defer wg.Done()
			_, _ = netmon.GetInterfaceList()
		}()
	}
	wg.Wait()

	list, err := netmon.GetInterfaceList()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "wlan0" {
		t.Fatalf("final list: %+v", list)
	}
}

func TestParseAddrCIDRKeepsHostIP(t *testing.T) {
	addrs, err := parseAddrCIDRs([]string{"192.168.1.50/24"})
	if err != nil {
		t.Fatal(err)
	}
	ipn := addrs[0].(*net.IPNet)
	if ipn.IP.String() != "192.168.1.50" {
		t.Fatalf("want host IP 192.168.1.50, got %s (network would be .0)", ipn.IP)
	}
	ones, bits := ipn.Mask.Size()
	if ones != 24 || bits != 32 {
		t.Fatalf("mask %d/%d", ones, bits)
	}
}

func TestInjectRegisterAndClearOnStartStop(t *testing.T) {
	// Plain mode: package inject is registered for the node even though NetMon
	// is never installed (daemon InjectNetworkChange no-ops).
	dir := t.TempDir()
	state := t.TempDir()
	port := mustFreePort(t)

	n, err := NewNode(&Config{
		Dir:            dir,
		StateDir:       state,
		Hostname:       "inject-lifecycle",
		Port:           port,
		NetMode:        "plain",
		ScanIntervalMs: 200,
		SyncIntervalMs: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if packageInjectRegistered() {
		t.Fatal("inject should be nil before Start")
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	if !packageInjectRegistered() {
		t.Fatal("package inject should be set after Start")
	}
	if n.d == nil {
		t.Fatal("node.d should be set while running")
	}
	// Safe no-op in plain mode (no NetMon).
	NotifyNetworkChange()
	n.NotifyNetworkChange()

	if err := n.Stop(); err != nil {
		t.Fatal(err)
	}
	if packageInjectRegistered() {
		t.Fatal("package inject should be cleared after Stop")
	}
	if n.d != nil {
		t.Fatal("node.d should be nil after Stop")
	}
}

func TestNotifyNetworkChangeConcurrentWithStop(t *testing.T) {
	dir := t.TempDir()
	state := t.TempDir()
	port := mustFreePort(t)

	n, err := NewNode(&Config{
		Dir:            dir,
		StateDir:       state,
		Hostname:       "inject-race",
		Port:           port,
		NetMode:        "plain",
		ScanIntervalMs: 200,
		SyncIntervalMs: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	const workers = 8
	stop := make(chan struct{})
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					// Final wave after stop begins.
					for range 20 {
						NotifyNetworkChange()
						n.NotifyNetworkChange()
					}
					return
				default:
					NotifyNetworkChange()
					n.NotifyNetworkChange()
				}
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	if err := n.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	close(stop)
	wg.Wait()

	if packageInjectRegistered() {
		t.Fatal("inject should be nil after Stop")
	}
}

// mustFreePort is shared with mobile_test via duplicate — defined here for
// package mobile tests. Uses ephemeral bind.
func mustFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}
