package xos

import (
	"errors"
	"os"

	pkgerrors "github.com/pkg/errors"
	"github.com/syndtr/gocapability/capability"
	"go.uber.org/zap"
)

// CanEnsureCapSysResource returns true if it can attempt to raise process
// limits (Linux-only), or false otherwise.
func CanEnsureCapSysResource() (bool, error) {
	return true, nil
}

// EnsureCapSysResource attempts to ensure the process has added SYS_RESOURCE to
// its capability set, returning an error if not possible.
func EnsureCapSysResource(l *zap.Logger) error {
	pid := os.Getpid()
	cap, err := capability.NewPid2(pid)
	if err != nil {
		return pkgerrors.WithMessagef(err, "cannot create capability set for process %d", pid)
	}

	if err := cap.Load(); err != nil {
		return pkgerrors.WithMessagef(err, "cannot load capabilities for process %d", pid)
	}

	sugar := l.Sugar()
	sugar.Info("CHECKING EFFECTIVE | PERM")
	// Already effective, nothing to do.
	if cap.Get(capability.EFFECTIVE|capability.PERMITTED, capability.CAP_SYS_RESOURCE) {
		sugar.Info("ALREADY SET")
		return nil
	}

	sugar.Info("CHECKING BOUNDING")
	// If don't have it and can't get it, raise an error.
	if !cap.Get(capability.BOUNDING, capability.CAP_SYS_RESOURCE) {
		return errors.New("SYS_RESOURCE not in bounding capability set")
	}

	// Is permitted, not effective, so raise it.
	cap.Set(capability.EFFECTIVE|capability.PERMITTED, capability.CAP_SYS_RESOURCE)
	for _, c := range []capability.CapType{
		capability.EFFECTIVE,
		capability.PERMITTED,
		capability.AMBIENT,
	} {
		sugar.Info("SETTING")
		sugar.Info("APPLYING")
		if err := cap.Apply(c); err != nil {
			sugar.Info("RAISED LIMIT", c.String())
			return pkgerrors.WithMessagef(err, "error applying %s capabilities", c.String())
		}
	}

	if err := cap.Load(); err != nil {
		return err
	}

	sugar.Info(cap.String())

	return nil
}
