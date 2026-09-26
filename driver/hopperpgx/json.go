package hopperpgx

import (
	"encoding/json"

	"github.com/parallelworks/hopper/driver"
)

// marshalAttemptError encodes an error record without its timestamp, which
// the finalize statement fills in from database time.
func marshalAttemptError(e *driver.AttemptError) ([]byte, error) {
	return json.Marshal(struct {
		Attempt int    `json:"attempt"`
		Error   string `json:"error"`
		Trace   string `json:"trace,omitempty"`
	}{e.Attempt, e.Error, e.Trace})
}
