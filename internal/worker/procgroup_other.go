//go:build !unix

package worker

import "os/exec"

// configureProcessGroup is a no-op where process groups aren't
// available. A timeout then kills only the process that was started;
// the executor's output-grace wait (see outputGrace) still stops a
// leftover child from hanging the worker indefinitely.
func configureProcessGroup(cmd *exec.Cmd) {}
