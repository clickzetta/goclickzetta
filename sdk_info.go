package goclickzetta

import (
	"net"
	"net/url"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

// sdkInfoHint carries the driver identity to the server on every job, in the
// same name:version[:commit] shape the Java and Python SDKs use.
const sdkInfoHint = "cz.sql.sdk.info"

const (
	sdkName       = "goclickzetta"
	sdkModulePath = "github.com/clickzetta/goclickzetta"
	// unknownVersion is reported when the build records no version for the
	// driver: a replace to a local directory, or the driver's own go test.
	// Such code may differ from every release, so naming one would mislead.
	unknownVersion = "unknown"
)

var (
	sdkInfoOnce  sync.Once
	sdkInfoValue string

	// clientIPCache maps a service host:port to the local address used to
	// reach it, so the lookup runs once per endpoint rather than per job.
	clientIPCache sync.Map
)

// pseudoVersionCommit matches the commit hash at the end of a Go module
// pseudo-version such as v0.0.0-20261112093000-abcdef123456.
var pseudoVersionCommit = regexp.MustCompile(`-(?:0\.)?\d{14}-([0-9a-f]{12})$`)

// sdkInfo returns goclickzetta:<version>[:<commit>] for the driver build
// linked into the running binary.
func sdkInfo() string {
	sdkInfoOnce.Do(func() {
		info, _ := debug.ReadBuildInfo()
		sdkInfoValue = formatSdkInfo(sdkVersionFromBuildInfo(info))
	})
	return sdkInfoValue
}

// formatSdkInfo leaves the commit out when there is none. A release tag already
// names its commit, so a tagged build reports just goclickzetta:<version>.
func formatSdkInfo(version, commit string) string {
	if commit == "" {
		return sdkName + ":" + version
	}
	return sdkName + ":" + version + ":" + commit
}

// sdkVersionFromBuildInfo finds this module in the build info. When the driver
// is a dependency, the module version is what the application pinned and a
// pseudo-version also names the commit. When the driver is the main module (its
// own tests, a local checkout) or replaced by a local directory, the version is
// "(devel)" and reported as unknown; the commit then comes from the VCS stamp,
// which only go build of a main package records. The commit is "" when neither
// source has it, a tagged release included.
func sdkVersionFromBuildInfo(info *debug.BuildInfo) (version, commit string) {
	version = unknownVersion
	if info == nil {
		return version, commit
	}

	var mod *debug.Module
	if info.Main.Path == sdkModulePath {
		mod = &info.Main
	} else {
		for _, dep := range info.Deps {
			if dep.Path == sdkModulePath {
				mod = dep
				break
			}
		}
	}
	if mod == nil {
		return version, commit
	}
	if mod.Replace != nil {
		mod = mod.Replace
	}

	if mod.Version != "" && mod.Version != "(devel)" {
		version = strings.TrimPrefix(mod.Version, "v")
		if m := pseudoVersionCommit.FindStringSubmatch(mod.Version); m != nil {
			commit = m[1]
		}
	}

	if mod == &info.Main && commit == "" {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				commit = s.Value
				if len(commit) > 12 {
					commit = commit[:12]
				}
				break
			}
		}
	}
	return version, commit
}

// clientIP returns the local IP address the driver uses to reach service, for
// the job's user_agent. Dialing UDP sends no packet; it only asks the kernel to
// pick the route, so the result is the interface that really talks to the
// server even on hosts with several. It returns "" when no address is found.
func clientIP(service string) string {
	target := serviceDialTarget(service)
	if target != "" {
		if v, ok := clientIPCache.Load(target); ok {
			return v.(string)
		}
	}

	ip := ""
	if target != "" {
		ip = localIPToward(target)
	}
	if ip == "" {
		ip = firstNonLoopbackIP()
	}
	if target != "" && ip != "" {
		clientIPCache.Store(target, ip)
	}
	return ip
}

// serviceDialTarget turns the configured service (normally scheme://host[:port])
// into host:port.
func serviceDialTarget(service string) string {
	service = strings.TrimSpace(service)
	if service == "" {
		return ""
	}
	if !strings.Contains(service, "://") {
		service = "https://" + service
	}
	u, err := url.Parse(service)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	port := u.Port()
	if port == "" {
		port = "443"
		if u.Scheme == "http" {
			port = "80"
		}
	}
	return net.JoinHostPort(u.Hostname(), port)
}

func localIPToward(target string) string {
	d := net.Dialer{Timeout: 2 * time.Second}
	c, err := d.Dial("udp", target)
	if err != nil {
		return ""
	}
	defer c.Close()
	if addr, ok := c.LocalAddr().(*net.UDPAddr); ok && addr.IP != nil && !addr.IP.IsUnspecified() {
		return addr.IP.String()
	}
	return ""
}

func firstNonLoopbackIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	fallback := ""
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() || ipNet.IP.IsLinkLocalUnicast() {
			continue
		}
		if ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
		if fallback == "" {
			fallback = ipNet.IP.String()
		}
	}
	return fallback
}
