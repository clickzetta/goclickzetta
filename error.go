package goclickzetta

import "fmt"

type ClickzettaError struct {
	Number         int
	SQLState       string
	QueryID        string
	Message        string
	MessageArgs    []interface{}
	IncludeQueryID bool // TODO: populate this in connection
}

func (ce *ClickzettaError) Error() string {
	message := ce.Message
	if len(ce.MessageArgs) > 0 {
		message = fmt.Sprintf(ce.Message, ce.MessageArgs...)
	}
	if ce.SQLState != "" {
		if ce.IncludeQueryID {
			return fmt.Sprintf("%06d (%s): %s: %s", ce.Number, ce.SQLState, ce.QueryID, message)
		}
		return fmt.Sprintf("%06d (%s): %s", ce.Number, ce.SQLState, message)
	}
	if ce.IncludeQueryID {
		return fmt.Sprintf("%06d: %s: %s", ce.Number, ce.QueryID, message)
	}
	return fmt.Sprintf("%06d: %s", ce.Number, message)
}
