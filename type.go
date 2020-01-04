package elasticsearch

import "fmt"

// Q the query arguments.
type Q map[string]interface{}

// Acknowledgement ...
type Acknowledgement struct {
	Acknowledged       bool `json:"acknowledged"`
	ShardsAcknowledged bool `json:"shards_acknowledged"`
}

// Error ...
type Error struct {
	Err ErrorError `json:"error"`
}

func (e *Error) Error() string {
	s := fmt.Sprintf("type: %s\n", e.Err.RootCause[0].Type)
	s += fmt.Sprintf("reason: %s\n", e.Err.RootCause[0].Reason)
	return s
}

// RootCause ...
type RootCause struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// ErrorError ...
type ErrorError struct {
	RootCause []RootCause `json:"root_cause"`
}
