package influxdb

import (
	"errors"
	"strings"
)

// ErrFieldTypeConflict is returned when a new field already exists with a
// different type.
var ErrFieldTypeConflict = errors.New("field type conflict")

// IsAuthorizationError indicates whether an error is due to an authorization failure
func IsAuthorizationError(err error) bool {
	e, ok := err.(interface {
		AuthorizationFailed() bool
	})
	return ok && e.AuthorizationFailed()
}

// IsClientError indicates whether an error is a known client error.
func IsClientError(err error) bool {
	if err == nil {
		return false
	}

	if strings.HasPrefix(err.Error(), ErrFieldTypeConflict.Error()) {
		return true
	}

	return false
}
