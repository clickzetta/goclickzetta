package goclickzetta

import (
	"context"
)

// driverFlagsKeyType used to store driver flags in context
type driverFlagsKeyType string

const (
	// driverFlagsKey used to store driver flags in context
	driverFlagsKey driverFlagsKeyType = "goclickzetta.driver.flags"
)

// DriverFlags used to pass driver parameters in context
type DriverFlags map[string]string

// WithDriverFlags add driver flags to context
// example:
//
//	ctx := goclickzetta.WithDriverFlags(ctx, goclickzetta.DriverFlags{
//	    "separate_params": "true",
//	    "custom_flag": "value1",
//	})
//	db.WithContext(ctx).Raw("SELECT * FROM table").Scan(&results)
func WithDriverFlags(ctx context.Context, flags DriverFlags) context.Context {
	// if flags already exist in context, merge them
	if existing, ok := ctx.Value(driverFlagsKey).(DriverFlags); ok {
		merged := make(DriverFlags)
		// first copy existing flags
		for k, v := range existing {
			merged[k] = v
		}
		// then add new flags (new values will overwrite old values)
		for k, v := range flags {
			merged[k] = v
		}
		return context.WithValue(ctx, driverFlagsKey, merged)
	}
	return context.WithValue(ctx, driverFlagsKey, flags)
}

// GetDriverFlags get driver flags from context
// if there are no flags in context, return nil
func GetDriverFlags(ctx context.Context) DriverFlags {
	if flags, ok := ctx.Value(driverFlagsKey).(DriverFlags); ok {
		return flags
	}
	return nil
}

// GetDriverFlag get specified driver flag value from context
// if flag does not exist, return empty string and false
func GetDriverFlag(ctx context.Context, key string) (string, bool) {
	flags := GetDriverFlags(ctx)
	if flags == nil {
		return "", false
	}
	value, ok := flags[key]
	return value, ok
}
