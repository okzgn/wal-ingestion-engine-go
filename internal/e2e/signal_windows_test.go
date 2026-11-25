//go:build windows

package e2e

import (
	"os/exec"
	"syscall"
)

// prepare puts the child in its own process group, so a console control event
// can be sent to it alone and not to the test binary as well.
func prepare(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// interrupt asks the process to shut down. Windows has no SIGTERM: the nearest
// thing is CTRL_BREAK_EVENT, which the Go runtime delivers as os.Interrupt,
// the very signal the engine handles. It needs a console; without one (some
// CI runners) the call fails and the caller skips the test.
func interrupt(cmd *exec.Cmd) error {
	const ctrlBreakEvent = 1
	proc := syscall.NewLazyDLL("kernel32.dll").NewProc("GenerateConsoleCtrlEvent")
	if r, _, err := proc.Call(ctrlBreakEvent, uintptr(cmd.Process.Pid)); r == 0 {
		return err
	}
	return nil
}
