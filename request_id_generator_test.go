package goclickzetta

import (
	"sync"
	"testing"
	"time"
	"unicode"
)

func TestGenerateIDFormat(t *testing.T) {
	gen := getRequestIDGenerator()
	id := gen.generate()

	// ID should be around 30 characters (17 timestamp + ~13 base36)
	if len(id) < 20 || len(id) > 35 {
		t.Errorf("unexpected id length: %d, id: %s", len(id), id)
	}

	// first 17 characters should be digits (yyyyMMddHHmmssSSS)
	for i, c := range id[:17] {
		if !unicode.IsDigit(c) {
			t.Errorf("expected digit at position %d, got %c in id: %s", i, c, id)
		}
	}

	// timestamp prefix should be parseable and recent
	timeStr := id[:17]
	parsed, err := time.Parse("20060102150405", timeStr[:14])
	if err != nil {
		t.Errorf("failed to parse timestamp prefix: %v", err)
	}
	// should be within the last minute (accounting for timezone)
	if time.Since(parsed) > 24*time.Hour {
		t.Errorf("timestamp seems too old: %v", parsed)
	}

	// remaining part should be valid base36 (digits + lowercase letters)
	for i, c := range id[17:] {
		if !unicode.IsDigit(c) && !(c >= 'a' && c <= 'z') && c != '-' {
			t.Errorf("unexpected character at suffix position %d: %c in id: %s", i, c, id)
		}
	}
}

func TestGenerateIDUniqueness(t *testing.T) {
	gen := getRequestIDGenerator()
	count := 10000
	ids := make(map[string]bool, count)

	for i := 0; i < count; i++ {
		id := gen.generate()
		if ids[id] {
			t.Fatalf("duplicate id found: %s at iteration %d", id, i)
		}
		ids[id] = true
	}
}

func TestGenerateIDConcurrency(t *testing.T) {
	gen := getRequestIDGenerator()
	goroutines := 10
	perGoroutine := 1000

	var mu sync.Mutex
	ids := make(map[string]bool, goroutines*perGoroutine)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localIDs := make([]string, 0, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				localIDs = append(localIDs, gen.generate())
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range localIDs {
				if ids[id] {
					t.Errorf("duplicate id in concurrent test: %s", id)
					return
				}
				ids[id] = true
			}
		}()
	}
	wg.Wait()

	expected := goroutines * perGoroutine
	if len(ids) != expected {
		t.Errorf("expected %d unique ids, got %d", expected, len(ids))
	}
}

func TestGetMACAddressAsInt64(t *testing.T) {
	val := getMACAddressAsInt64()
	// should be a positive value within 48-bit range
	if val < 0 {
		t.Errorf("expected non-negative value, got %d", val)
	}
	if val >= (1 << 48) {
		t.Errorf("value exceeds 48-bit range: %d", val)
	}
}

func TestFormatJobId(t *testing.T) {
	id := formatJobId()
	if len(id) < 20 {
		t.Errorf("formatJobId returned too short id: %s", id)
	}
	// should not be empty
	if id == "" {
		t.Error("formatJobId returned empty string")
	}
}
