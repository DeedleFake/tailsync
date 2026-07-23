package mobile

import (
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"

	"tailscale.com/net/netmon"
)

// androidNet holds the process-global interface snapshot supplied by the Android
// host (ConnectivityManager / LinkProperties). When set, netmon uses this
// instead of Go's net.Interfaces(), which fails with permission errors on
// Android API 30+.
//
// Updates are visible immediately to the registered getter; call
// NotifyNetworkChange after updates once a tsnet node is running so the
// monitor re-evaluates routes without waiting for its next poll.
type androidNetState struct {
	mu      sync.Mutex
	set     bool // true once the host has supplied a snapshot (even if empty)
	ifaces  []netmon.Interface
	routeIf string
	gateway string
	// inject is the active tsnet NetMon InjectEvent (or nil). Copied under mu;
	// callers invoke the func outside the lock so Stop cannot nil it mid-call
	// after the copy (method value holds the Monitor pointer).
	inject    func()
	injectGen uint64 // bumps on each setInject; clearInject only matches gen
}

var androidNet androidNetState

func init() {
	// Register early so any later tsnet.Up uses our getter. When the host has
	// never set a snapshot, fall back to net.Interfaces so desktop/CLI is unchanged.
	netmon.RegisterInterfaceGetter(androidNet.getInterfaces)
}

// SetNetworkInterfacesJSON replaces the process-global interface list from a
// JSON array. Call this before Node.Start when using tsnet on Android, and
// again from ConnectivityManager callbacks when networks change.
//
// Each element:
//
//	{"name":"wlan0","index":1,"flags":51,"mtu":1500,"addrs":["192.168.1.2/24","fe80::1/64"]}
//
// flags are Go net.Flags bits (1=Up, 2=Broadcast, 4=Loopback, 8=PointToPoint,
// 16=Multicast, 32=Running). Mirror OS flags when possible; live uplinks should
// include Up|Running (and usually Broadcast|Multicast, e.g. 51). addrs are CIDR
// strings; they become AltAddrs so netmon never calls the broken stdlib
// Interface.Addrs on Android.
//
// An empty array is accepted and stored (getter returns no interfaces). Prefer
// at least loopback or a real uplink before starting tsnet; an empty list can
// cause tsnet bring-up to fail or mis-detect connectivity.
//
// INTERNET permission is still required for sockets; it does not make
// net.Interfaces work on modern Android.
func SetNetworkInterfacesJSON(jsonStr string) error {
	var raw []ifaceJSON
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return fmt.Errorf("parse network interfaces json: %w", err)
	}
	ifaces, err := ifacesFromJSON(raw)
	if err != nil {
		return err
	}
	androidNet.setInterfaces(ifaces)
	return nil
}

// SetNetworkInterfaces replaces the process-global interface list from a
// gomobile-friendly builder. See SetNetworkInterfacesJSON for semantics.
// list may be nil (treated as empty); prefer non-empty before tsnet Start.
func SetNetworkInterfaces(list *NetworkInterfaceList) error {
	var items []NetworkInterface
	if list != nil {
		items = list.snapshot()
	}
	ifaces, err := ifacesFromBuilder(items)
	if err != nil {
		return err
	}
	androidNet.setInterfaces(ifaces)
	return nil
}

// SetDefaultRouteInterface records the name of the interface that owns the
// default route (for example "wlan0" or "rmnet0"). Empty string means the
// default route was lost. On Android this wraps netmon.UpdateLastKnownDefaultRouteInterface;
// on other GOOS it only updates local state (for tests).
//
// Call before Start and whenever ConnectivityManager reports a path change.
func SetDefaultRouteInterface(name string) {
	androidNet.mu.Lock()
	androidNet.routeIf = name
	androidNet.mu.Unlock()
	setDefaultRouteInterfacePlatform(name)
}

// SetDefaultGateway records the default gateway IP from LinkProperties.
// Empty string clears the cached gateway (for example cellular with no private
// gateway, or network lost). Non-empty values must parse as an IP address
// (netip.ParseAddr); invalid strings return an error and leave state unchanged.
// On Android this wraps netmon.UpdateLastKnownDefaultGateway.
func SetDefaultGateway(ip string) error {
	ip = strings.TrimSpace(ip)
	if ip != "" {
		if _, err := netip.ParseAddr(ip); err != nil {
			return fmt.Errorf("invalid default gateway %q: %w", ip, err)
		}
	}
	androidNet.mu.Lock()
	androidNet.gateway = ip
	androidNet.mu.Unlock()
	setDefaultGatewayPlatform(ip)
	return nil
}

// NotifyNetworkChange tells a running tsnet netmon that connectivity changed
// after SetNetworkInterfaces* / SetDefaultRoute* updates. No-op if no mobile
// node has registered an inject callback (not started, already stopped) or if
// tsnet NetMon is not yet installed (during the long Up window, inject is a
// no-op until Up completes; the daemon then fires a catch-up InjectEvent so
// the latest snapshot is applied). Interface snapshot updates are still
// visible to the registered getter without this call.
//
// Package-level NotifyNetworkChange targets only the most recently started
// Node. Multi-node hosts must call Node.NotifyNetworkChange on each node
// (or run a single node).
//
// Safe concurrent with Stop: copies the inject func under a lock, then invokes
// it; a concurrent shutdown may no-op a later call but will not nil the
// Monitor under an in-flight InjectEvent.
func NotifyNetworkChange() {
	androidNet.mu.Lock()
	inject := androidNet.inject
	androidNet.mu.Unlock()
	if inject != nil {
		inject()
	}
}

// NetworkInterface is one host interface for SetNetworkInterfaces.
// AddrCIDRs is a comma-separated list of CIDR strings
// (for example "192.168.1.2/24,fe80::1/64"). Empty means no addresses
// (AltAddrs is still non-nil so netmon does not call stdlib Addrs).
type NetworkInterface struct {
	Name      string
	Index     int
	Flags     int // net.Flags bits
	MTU       int
	AddrCIDRs string
}

// NetworkInterfaceList is a gomobile-friendly builder for SetNetworkInterfaces.
type NetworkInterfaceList struct {
	mu    sync.Mutex
	items []NetworkInterface
}

// NewNetworkInterfaceList returns an empty list for Add + SetNetworkInterfaces.
func NewNetworkInterfaceList() *NetworkInterfaceList {
	return &NetworkInterfaceList{}
}

// Add appends one interface. addrCIDRs is comma-separated CIDRs (may be empty).
func (l *NetworkInterfaceList) Add(name string, index, flags, mtu int, addrCIDRs string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.items = append(l.items, NetworkInterface{
		Name:      name,
		Index:     index,
		Flags:     flags,
		MTU:       mtu,
		AddrCIDRs: addrCIDRs,
	})
	l.mu.Unlock()
}

// Len returns the number of interfaces in the list.
func (l *NetworkInterfaceList) Len() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}

func (l *NetworkInterfaceList) snapshot() []NetworkInterface {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]NetworkInterface(nil), l.items...)
}

type ifaceJSON struct {
	Name  string   `json:"name"`
	Index int      `json:"index"`
	Flags int      `json:"flags"`
	MTU   int      `json:"mtu"`
	Addrs []string `json:"addrs"`
}

func ifacesFromJSON(raw []ifaceJSON) ([]netmon.Interface, error) {
	out := make([]netmon.Interface, 0, len(raw))
	for i, r := range raw {
		name := strings.TrimSpace(r.Name)
		if name == "" {
			return nil, fmt.Errorf("interface %d: name is required", i)
		}
		addrs, err := parseAddrCIDRs(r.Addrs)
		if err != nil {
			return nil, fmt.Errorf("interface %q: %w", name, err)
		}
		out = append(out, netmon.Interface{
			Interface: &net.Interface{
				Index: r.Index,
				MTU:   r.MTU,
				Name:  name,
				Flags: net.Flags(r.Flags),
			},
			// Non-nil even when empty so Addrs() never hits stdlib.
			AltAddrs: addrs,
		})
	}
	return out, nil
}

func ifacesFromBuilder(items []NetworkInterface) ([]netmon.Interface, error) {
	out := make([]netmon.Interface, 0, len(items))
	for i, it := range items {
		name := strings.TrimSpace(it.Name)
		if name == "" {
			return nil, fmt.Errorf("interface %d: name is required", i)
		}
		var cidrs []string
		for part := range strings.SplitSeq(it.AddrCIDRs, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				cidrs = append(cidrs, part)
			}
		}
		addrs, err := parseAddrCIDRs(cidrs)
		if err != nil {
			return nil, fmt.Errorf("interface %q: %w", name, err)
		}
		out = append(out, netmon.Interface{
			Interface: &net.Interface{
				Index: it.Index,
				MTU:   it.MTU,
				Name:  name,
				Flags: net.Flags(it.Flags),
			},
			AltAddrs: addrs,
		})
	}
	return out, nil
}

func parseAddrCIDRs(cidrs []string) ([]net.Addr, error) {
	// Always non-nil so netmon.Interface.Addrs uses AltAddrs.
	addrs := make([]net.Addr, 0, len(cidrs))
	for _, s := range cidrs {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		ip, ipnet, err := net.ParseCIDR(s)
		if err != nil {
			return nil, fmt.Errorf("parse addr %q: %w", s, err)
		}
		// Keep the host IP (not the network address) with the mask, matching
		// what net.Interface.Addrs typically returns.
		addrs = append(addrs, &net.IPNet{IP: ip, Mask: ipnet.Mask})
	}
	return addrs, nil
}

func (s *androidNetState) setInterfaces(ifaces []netmon.Interface) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set = true
	s.ifaces = ifaces
}

func (s *androidNetState) getInterfaces() ([]netmon.Interface, error) {
	s.mu.Lock()
	if !s.set {
		s.mu.Unlock()
		// Do not hold the mutex across net.Interfaces (syscall / netlink).
		return stdNetInterfaces()
	}
	out := copyNetmonInterfaces(s.ifaces)
	s.mu.Unlock()
	return out, nil
}

// setInject installs f as the package-level NotifyNetworkChange target and
// returns a generation token for clearInject.
func (s *androidNetState) setInject(f func()) uint64 {
	s.mu.Lock()
	s.injectGen++
	gen := s.injectGen
	s.inject = f
	s.mu.Unlock()
	return gen
}

// clearInject removes the inject callback only if gen is still current.
func (s *androidNetState) clearInject(gen uint64) {
	s.mu.Lock()
	if s.injectGen == gen {
		s.inject = nil
	}
	s.mu.Unlock()
}

func stdNetInterfaces() ([]netmon.Interface, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	ret := make([]netmon.Interface, len(ifs))
	for i := range ifs {
		ret[i].Interface = &ifs[i]
	}
	return ret, nil
}

func copyNetmonInterfaces(in []netmon.Interface) []netmon.Interface {
	if in == nil {
		return []netmon.Interface{}
	}
	out := make([]netmon.Interface, len(in))
	for i, iface := range in {
		var ni *net.Interface
		if iface.Interface != nil {
			cp := *iface.Interface
			ni = &cp
		}
		var alt []net.Addr
		if iface.AltAddrs != nil {
			alt = make([]net.Addr, len(iface.AltAddrs))
			for j, a := range iface.AltAddrs {
				alt[j] = cloneNetAddr(a)
			}
		}
		out[i] = netmon.Interface{
			Interface: ni,
			AltAddrs:  alt,
			Desc:      iface.Desc,
		}
	}
	return out
}

// cloneNetAddr deep-copies address types we produce (*net.IPNet) so callers
// cannot mutate the process-global snapshot via shared IP/Mask slices.
func cloneNetAddr(a net.Addr) net.Addr {
	switch v := a.(type) {
	case *net.IPNet:
		if v == nil {
			return (*net.IPNet)(nil)
		}
		return &net.IPNet{
			IP:   append(net.IP(nil), v.IP...),
			Mask: append(net.IPMask(nil), v.Mask...),
		}
	case *net.IPAddr:
		if v == nil {
			return (*net.IPAddr)(nil)
		}
		return &net.IPAddr{
			IP:   append(net.IP(nil), v.IP...),
			Zone: v.Zone,
		}
	default:
		// We only store *net.IPNet from parseAddrCIDRs; other types are rare.
		return a
	}
}

// packageInjectRegistered reports whether package-level NotifyNetworkChange
// has a non-nil inject callback. Test-only.
func packageInjectRegistered() bool {
	androidNet.mu.Lock()
	defer androidNet.mu.Unlock()
	return androidNet.inject != nil
}

// resetAndroidNetForTest clears the snapshot so the getter falls back to
// net.Interfaces. Test-only.
func resetAndroidNetForTest() {
	androidNet.mu.Lock()
	androidNet.set = false
	androidNet.ifaces = nil
	androidNet.routeIf = ""
	androidNet.gateway = ""
	// Leave inject alone; lifecycle tests manage their own node.
	androidNet.mu.Unlock()
	setDefaultRouteInterfacePlatform("")
	setDefaultGatewayPlatform("")
}
