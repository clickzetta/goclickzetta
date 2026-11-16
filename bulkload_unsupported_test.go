package goclickzetta

import (
	"errors"
	"testing"
)

// This build has no Arrow dependency, so it cannot write the parquet files
// BulkLoad uploads. The entry points must refuse up front: the old code
// accepted rows, wrote no files and reported a successful commit, so every
// row was silently lost.
func TestBulkloadIsNotSupported(t *testing.T) {
	conn := newTestConn(nil)

	stream, err := conn.CreateBulkloadStream(BulkloadOptions{Table: "t", Operation: APPEND})
	if !errors.Is(err, ErrBulkloadNotSupported) || stream != nil {
		t.Errorf("CreateBulkloadStream = (%v, %v), want (nil, ErrBulkloadNotSupported)", stream, err)
	}

	stream, err = conn.GetDistributeBulkloadStream("stream-id", BulkloadOptions{Table: "t"})
	if !errors.Is(err, ErrBulkloadNotSupported) || stream != nil {
		t.Errorf("GetDistributeBulkloadStream = (%v, %v), want (nil, ErrBulkloadNotSupported)", stream, err)
	}
}
