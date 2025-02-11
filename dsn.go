package goclickzetta

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// ConfigBool is a type to represent true or false in the Config
type ConfigBool uint8

// Config is a set of configuration parameters
type Config struct {
	UserName       string // Username
	Password       string // Password (requires User)
	Schema         string // Schema
	Workspace      string // Workspace
	VirtualCluster string // VirtualCluster
	Service        string // Service
	Instance       string // Instance
	Protocol       string // Protocol
	Token          string
	InstanceId     int64

	Params map[string]*string // other connection parameters
}

var (
	errInvalidDSNUnescaped = errors.New("invalid DSN: did you forget to escape a param value?")
	errInvalidDSNAddr      = errors.New("invalid DSN: network address not terminated (missing closing brace)")
	errInvalidDSNNoSlash   = errors.New("invalid DSN: missing the slash separating the database name")
)

func (cfg *Config) normalize() error {
	if cfg.Service == "" {
		return errors.New("missing service name")
	}

	if cfg.VirtualCluster == "" {
		return errors.New("missing virtual cluster name")
	}

	if cfg.Workspace == "" {
		return errors.New("missing workspace name")
	}

	if cfg.Instance == "" {
		return errors.New("missing instance name")
	}

	if cfg.Protocol == "" {
		cfg.Protocol = "https"
	}

	if cfg.Schema == "" {
		cfg.Schema = "public"
	}

	if cfg.UserName == "" {
		return errors.New("missing username")
	}

	if cfg.Password == "" {
		return errors.New("missing password")
	}

	return nil
}

// DSN constructs a DSN for Clickzetta db.
func DSN(cfg *Config) (dsn string) {
	var buf bytes.Buffer

	// [username[:password]@]
	if len(cfg.UserName) > 0 {
		buf.WriteString(cfg.UserName)
		if len(cfg.Password) > 0 {
			buf.WriteByte(':')
			buf.WriteString(cfg.Password)
		}
		buf.WriteByte('@')
	}

	// [protocol[(address)]]
	if len(cfg.Protocol) > 0 {
		buf.WriteString(cfg.Protocol)
		if len(cfg.Service) > 0 {
			buf.WriteByte('(')
			buf.WriteString(cfg.Service)
			buf.WriteByte(')')
		}
	}

	// /schema is dbName in other drivers
	buf.WriteByte('/')
	buf.WriteString(url.PathEscape(cfg.Schema))

	hasParam := false
	if cfg.VirtualCluster != "" {
		writeDSNParam(&buf, &hasParam, "virtualCluster", cfg.VirtualCluster)
	}

	if cfg.Workspace != "" {
		writeDSNParam(&buf, &hasParam, "workspace", cfg.Workspace)
	}

	if cfg.Instance != "" {
		writeDSNParam(&buf, &hasParam, "instance", cfg.Instance)
	}

	if cfg.Params != nil {
		for k, v := range cfg.Params {
			writeDSNParam(&buf, &hasParam, k, *v)
		}
	}
	return buf.String()

}

func writeDSNParam(buf *bytes.Buffer, hasParam *bool, name, value string) {
	buf.Grow(1 + len(name) + 1 + len(value))
	if !*hasParam {
		*hasParam = true
		buf.WriteByte('?')
	} else {
		buf.WriteByte('&')
	}
	buf.WriteString(name)
	buf.WriteByte('=')
	buf.WriteString(value)
}

// ParseDSN parses the DSN string to a Config.
func ParseDSN(dsn string) (cfg *Config, err error) {
	// New config with some default values
	cfg = &Config{
		Params: make(map[string]*string),
	}

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								cfg.Password = dsn[k+1 : j]
								break
							}
						}
						cfg.UserName = dsn[:k]

						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, errInvalidDSNUnescaped
							}
							return nil, errInvalidDSNAddr
						}
						cfg.Service = dsn[k+1 : i-1]

						break
					}
				}
				cfg.Protocol = dsn[j+1 : k]
			}

			if cfg.Protocol == "http" || cfg.Protocol == "https" {
				cfg.Service = cfg.Protocol + "://" + cfg.Service
			}

			// schema[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(cfg, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}

			schema := dsn[i+1 : j]
			if cfg.Schema, err = url.PathUnescape(schema); err != nil {
				return nil, fmt.Errorf("invalid schema %q: %w", schema, err)
			}

			break
		}
	}

	if !foundSlash && len(dsn) > 0 {
		return nil, errInvalidDSNNoSlash
	}

	if err = cfg.normalize(); err != nil {
		return nil, err
	}
	return
}

// parseDSNParams parses the DSN "query string". Values must be url.QueryEscape'ed
func parseDSNParams(cfg *Config, params string) (err error) {
	logger.Infof("Query String: %v\n", params)
	for _, v := range strings.Split(params, "&") {
		param := strings.SplitN(v, "=", 2)
		if len(param) != 2 {
			continue
		}
		var value string
		value, err = url.QueryUnescape(param[1])
		if err != nil {
			return err
		}
		switch param[0] {
		case "virtualcluster", "virtualCluster", "vc", "VirtualCluster", "Virtualcluster", "VC":
			cfg.VirtualCluster = value
		case "instance":
			cfg.Instance = value
		case "workspace":
			cfg.Workspace = value
		default:
			if cfg.Params == nil {
				cfg.Params = make(map[string]*string)
			}
			cfg.Params[param[0]] = &value
		}
	}
	return
}
