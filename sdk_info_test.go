package goclickzetta

import (
	"context"
	"net"
	"runtime/debug"
	"strings"
	"testing"
)

func TestSdkVersionFromBuildInfo(t *testing.T) {
	tests := []struct {
		name        string
		info        *debug.BuildInfo
		wantVersion string
		wantCommit  string
	}{
		{
			name:        "no build info",
			info:        nil,
			wantVersion: unknownVersion,
			wantCommit:  "",
		},
		{
			name: "tagged dependency",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app"},
				Deps: []*debug.Module{{Path: sdkModulePath, Version: "v0.0.15"}},
			},
			wantVersion: "0.0.15",
			wantCommit:  "",
		},
		{
			name: "pseudo-version dependency",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app"},
				Deps: []*debug.Module{{Path: sdkModulePath, Version: "v0.0.16-0.20261112093000-abcdef123456"}},
			},
			wantVersion: "0.0.16-0.20261112093000-abcdef123456",
			wantCommit:  "abcdef123456",
		},
		{
			name: "replaced dependency",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app"},
				Deps: []*debug.Module{{
					Path:    sdkModulePath,
					Version: "v0.0.15",
					Replace: &debug.Module{Path: "example.com/fork", Version: "v0.0.0-20261001000000-0123456789ab"},
				}},
			},
			wantVersion: "0.0.0-20261001000000-0123456789ab",
			wantCommit:  "0123456789ab",
		},
		{
			name: "main module with vcs stamp",
			info: &debug.BuildInfo{
				Main:     debug.Module{Path: sdkModulePath, Version: "(devel)"},
				Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "95b0a24c0ffee0123456789abcdef0123456789a"}},
			},
			wantVersion: unknownVersion,
			wantCommit:  "95b0a24c0ffe",
		},
		{
			name: "replaced by local directory",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app"},
				Deps: []*debug.Module{{
					Path:    sdkModulePath,
					Version: "v0.0.15",
					Replace: &debug.Module{Path: "../goclickzetta", Version: "(devel)"},
				}},
			},
			wantVersion: unknownVersion,
			wantCommit:  "",
		},
		{
			name: "driver not in build",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app"},
			},
			wantVersion: unknownVersion,
			wantCommit:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, commit := sdkVersionFromBuildInfo(tt.info)
			if version != tt.wantVersion || commit != tt.wantCommit {
				t.Errorf("got (%q, %q), want (%q, %q)", version, commit, tt.wantVersion, tt.wantCommit)
			}
		})
	}
}

func TestFormatSdkInfo(t *testing.T) {
	if got := formatSdkInfo("0.0.15", ""); got != "goclickzetta:0.0.15" {
		t.Errorf("tagged: got %q, want goclickzetta:0.0.15", got)
	}
	if got := formatSdkInfo("0.0.16-0.20261112093000-abcdef123456", "abcdef123456"); got != "goclickzetta:0.0.16-0.20261112093000-abcdef123456:abcdef123456" {
		t.Errorf("pseudo-version: got %q", got)
	}
}

func TestSdkInfoShape(t *testing.T) {
	parts := strings.Split(sdkInfo(), ":")
	if len(parts) < 2 || len(parts) > 3 || parts[0] != "goclickzetta" || parts[1] == "" || (len(parts) == 3 && parts[2] == "") {
		t.Fatalf("sdkInfo() = %q, want goclickzetta:<version>[:<commit>]", sdkInfo())
	}
}

func TestServiceDialTarget(t *testing.T) {
	tests := map[string]string{
		"":                                    "",
		"https://cn-shanghai.api.example.com": "cn-shanghai.api.example.com:443",
		"http://localhost":                    "localhost:80",
		"http://127.0.0.1:8080":               "127.0.0.1:8080",
		"api.example.com":                     "api.example.com:443",
		"https://[::1]:9443":                  "[::1]:9443",
	}
	for in, want := range tests {
		if got := serviceDialTarget(in); got != want {
			t.Errorf("serviceDialTarget(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestClientIPTowardLoopback(t *testing.T) {
	if got := clientIP("http://127.0.0.1:8080"); got != "127.0.0.1" {
		t.Errorf("clientIP toward loopback = %q, want 127.0.0.1", got)
	}
}

func TestClientIPIsAnAddressOrEmpty(t *testing.T) {
	// No service: falls back to an interface address, which may legitimately
	// be absent in a sandbox, but must parse when present.
	if got := clientIP(""); got != "" && net.ParseIP(got) == nil {
		t.Errorf("clientIP(\"\") = %q, not an IP", got)
	}
}

func TestExecInternal_SendsSdkInfoAndUserAgent(t *testing.T) {
	mock := &capturingMockClient{
		postResponses: []mockResponse{{body: []byte(`{"status":{"state":"SUCCEED"}}`)}},
	}
	conn := newTestConnWithConfig(mock, &Config{
		Service:        "http://127.0.0.1:8080",
		Workspace:      "ws",
		VirtualCluster: "vc",
		Schema:         "public",
	})

	if _, err := conn.execInternal(context.Background(), "select 1\n;", nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := jsonStr(mock.lastBody, "jobDesc", "sqlJob", "sqlConfig", "hint", sdkInfoHint); got != sdkInfo() {
		t.Errorf("hint %s = %q, want %q", sdkInfoHint, got, sdkInfo())
	}
	if got := jsonStr(mock.lastBody, "jobDesc", "userAgent"); got != "127.0.0.1" {
		t.Errorf("jobDesc.userAgent = %q, want 127.0.0.1", got)
	}
}
