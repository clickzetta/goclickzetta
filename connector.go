package goclickzetta

import (
	"context"
	"database/sql/driver"
)

// InternalClickzettaDriver is the interface for an internal Clickzetta driver
type InternalClickzettaDriver interface {
	Open(dsn string) (driver.Conn, error)
	OpenWithConfig(ctx context.Context, config Config) (driver.Conn, error)
}

// Connector creates Driver with the specified Config
type Connector struct {
	driver InternalClickzettaDriver
	cfg    Config
}

// NewConnector creates a new connector with the given ClickzettaDriver and Config.
func NewConnector(driver InternalClickzettaDriver, config Config) Connector {
	return Connector{driver, config}
}

// Connect creates a new connection.
func (t Connector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg := t.cfg
	return t.driver.OpenWithConfig(ctx, cfg)
}

// Driver creates a new driver.
func (t Connector) Driver() driver.Driver {
	return t.driver
}
