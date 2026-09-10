package goclickzetta

import "testing"

// ClickzettaError.Error had no coverage at all, so nothing pinned the shape of
// the message a caller sees, or which fields make it in.
func TestClickzettaErrorMessage(t *testing.T) {
	tests := []struct {
		name string
		err  ClickzettaError
		want string
	}{
		{
			name: "number and message",
			err:  ClickzettaError{Number: 42, Message: "table not found"},
			want: "000042: table not found",
		},
		{
			name: "with sql state",
			err:  ClickzettaError{Number: 42, SQLState: "42S02", Message: "table not found"},
			want: "000042 (42S02): table not found",
		},
		{
			name: "with query id",
			err:  ClickzettaError{Number: 42, QueryID: "job-1", Message: "table not found", IncludeQueryID: true},
			want: "000042: job-1: table not found",
		},
		{
			name: "with sql state and query id",
			err:  ClickzettaError{Number: 42, SQLState: "42S02", QueryID: "job-1", Message: "table not found", IncludeQueryID: true},
			want: "000042 (42S02): job-1: table not found",
		},
		{
			name: "message args are interpolated",
			err:  ClickzettaError{Number: 7, Message: "column %s is %s", MessageArgs: []interface{}{"id", "missing"}},
			want: "000007: column id is missing",
		},
		{
			// Number is printed six digits wide, so a larger code is not
			// truncated to fit.
			name: "wide number",
			err:  ClickzettaError{Number: 1234567, Message: "boom"},
			want: "1234567: boom",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Error(); got != tc.want {
				t.Errorf("Error() = %q, want %q", got, tc.want)
			}
		})
	}
}
