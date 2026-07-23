//go:build android

package mobile

import "tailscale.com/net/netmon"

func setDefaultRouteInterfacePlatform(name string) {
	netmon.UpdateLastKnownDefaultRouteInterface(name)
}

func setDefaultGatewayPlatform(ip string) {
	netmon.UpdateLastKnownDefaultGateway(ip)
}
