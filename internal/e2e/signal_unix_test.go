//go:build !windows

package e2e

import (
	"os/exec"
	"syscall"
)

// prepare has nothing to do on Unix.
func prepare(*exec.Cmd) {}

// interrupt asks the process to shut down the way a supervisor does.
func interrupt(cmd *exec.Cmd) error { return cmd.Process.Signal(syscall.SIGTERM) }
