package goclickzetta

import (
	"database/sql/driver"
	"sync/atomic"
	"time"
)

type noCopy struct{}

func (*noCopy) Lock() {}

func (*noCopy) Unlock() {}

// atomicError is a wrapper for atomically accessed error values
type atomicError struct {
	_     noCopy
	value atomic.Value
}

// Set sets the error value regardless of the previous value.
// The value must not be nil
func (ae *atomicError) Set(value error) {
	ae.value.Store(value)
}

// Value returns the current error value
func (ae *atomicError) Value() error {
	if v := ae.value.Load(); v != nil {
		// this will panic if the value doesn't implement the error interface
		return v.(error)
	}
	return nil
}

// integer min
func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// integer max
func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func int64Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func getMin(arr []int) int {
	if len(arr) == 0 {
		return -1
	}
	min := arr[0]
	for _, v := range arr {
		if v <= min {
			min = v
		}
	}
	return min
}

// time.Duration max
func durationMax(d1, d2 time.Duration) time.Duration {
	if d1-d2 > 0 {
		return d1
	}
	return d2
}

// time.Duration min
func durationMin(d1, d2 time.Duration) time.Duration {
	if d1-d2 < 0 {
		return d1
	}
	return d2
}

// toNamedValues converts a slice of driver.Value to a slice of driver.NamedValue for Go 1.8 SQL package
func toNamedValues(values []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for idx, value := range values {
		namedValues[idx] = driver.NamedValue{Name: "", Ordinal: idx + 1, Value: value}
	}
	return namedValues
}
