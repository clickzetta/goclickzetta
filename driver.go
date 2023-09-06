package goclickzetta

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
)

var paramsMutex *sync.Mutex

// ClickzettaDriver is a context of Go Driver
type ClickzettaDriver struct{}

// Open creates a new connection.
func (d ClickzettaDriver) Open(dsn string) (driver.Conn, error) {
	logger.Info("Open")
	ctx := context.TODO()
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return d.OpenWithConfig(ctx, *cfg)
}

// OpenWithConfig creates a new connection with the given Config.
func (d ClickzettaDriver) OpenWithConfig(ctx context.Context, config Config) (driver.Conn, error) {
	logger.Info("OpenWithConfig")
	conn, err := buildClickzettaConn(ctx, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

var logger = CreateDefaultLogger()

func init() {
	sql.Register("clickzetta", &ClickzettaDriver{})
	logger.SetLogLevel("error")
	paramsMutex = &sync.Mutex{}
}
