//go:build !android

package mobile

// On non-Android builds, Tailscale does not export UpdateLastKnownDefault*
// (those symbols live in netmon's android-only file). Stubs keep the mobile
// API compiling for desktop tests and CLI; values are still recorded in
// androidNet for verification.

func setDefaultRouteInterfacePlatform(name string) {
	// no-op outside Android
	_ = name
}

func setDefaultGatewayPlatform(ip string) {
	// no-op outside Android
	_ = ip
}
