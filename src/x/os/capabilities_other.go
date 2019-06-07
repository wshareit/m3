// +build !linux

package xos

import (
	"errors"
	"runtime"
)

var (
	errIncompatibleOS = errors.New("cannot ensure capabilities on OS " + runtime.GOOS)
)

// CanEnsureCapSysResource returns true if it can attempt to raise process
// limits (Linux-only), or false otherwise.
func CanEnsureCapSysResource() (bool, error) {
	return false, errIncompatibleOS
}

// EnsureCapSysResource attempts to ensure the process has added SYS_RESOURCE to
// its capability set, returning an error if not possible.
func EnsureCapSysResource() error {
	return errIncompatibleOS
}
