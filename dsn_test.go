package goclickzetta

import (
	"fmt"
	"testing"
)

func TestDSNConfigToString(t *testing.T) {
	cfg := &Config{
		UserName:       "test",
		Password:       "test",
		Protocol:       "https",
		Service:        "dev.us-east-1",
		Schema:         "public",
		VirtualCluster: "test",
		Workspace:      "test",
		Instance:       "test",
	}
	dsn := DSN(cfg)
	if dsn != "test:test@https(dev.us-east-1)/public?virtualCluster=test&workspace=test&instance=test" {
		t.Errorf("Expected %s, got %s", "test:test@https(dev.us-east-1)?virtualCluster=test&schema=public&workspace=test&instance=test", dsn)
	}
	fmt.Println(dsn)
}

func TestDSNStringToConfig(t *testing.T) {
	dsn := "test:test@https(dev.us-east-1)/public1?virtualCluster=test&workspace=test&instance=test"
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Errorf("Expected no error, got %s", err)
	}
	if cfg.UserName != "test" {
		t.Errorf("Expected %s, got %s", "test", cfg.UserName)
	}
	if cfg.Password != "test" {
		t.Errorf("Expected %s, got %s", "test", cfg.Password)
	}
	if cfg.Protocol != "https" {
		t.Errorf("Expected %s, got %s", "https", cfg.Protocol)
	}
	if cfg.Service != "https://dev.us-east-1" {
		t.Errorf("Expected %s, got %s", "https://dev.us-east-1", cfg.Service)
	}
	if cfg.Schema != "public1" {
		t.Errorf("Expected %s, got %s", "public", cfg.Schema)
	}
	if cfg.VirtualCluster != "test" {
		t.Errorf("Expected %s, got %s", "test", cfg.VirtualCluster)
	}
	if cfg.Workspace != "test" {
		t.Errorf("Expected %s, got %s", "test", cfg.Workspace)
	}
	if cfg.Instance != "test" {
		t.Errorf("Expected %s, got %s", "test", cfg.Instance)
	}
}
