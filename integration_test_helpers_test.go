package goclickzetta

import (
	"os"
	"strings"
	"testing"
)

func integrationDSN(t *testing.T) string {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("CLICKZETTA_DSN"))
	if dsn == "" {
		t.Skip("CLICKZETTA_DSN is not set")
	}
	return dsn
}

func integrationConfig(t *testing.T) Config {
	t.Helper()
	cfg, err := ParseDSN(integrationDSN(t))
	if err != nil {
		t.Fatalf("parse CLICKZETTA_DSN: %v", err)
	}
	return *cfg
}
