//go:build unix

package worker

import (
	"os"
	"os/exec"
	"syscall"
)

// configureProcessGroup makes cmd the leader of its own process group
// and makes cancelling it (on timeout) kill that whole group with
// SIGKILL instead of only the shell. Everything the command spawns
// stays in the group unless it deliberately moves itself out, so a
// timed-out task leaves nothing behind.
func configureProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		// A negative pid addresses the whole process group.
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if err == syscall.ESRCH {
			return os.ErrProcessDone // the group already exited
		}
		return err
	}
}
