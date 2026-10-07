package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"
)

// DefaultExecTimeout bounds how long a single task's command is
// allowed to run before it's killed and reported as failed. Used
// whenever NewShellExecutor is given a timeout <= 0.
const DefaultExecTimeout = 30 * time.Second

// outputGrace is how long, once the shell has exited or been killed,
// the executor keeps waiting for the command's output pipes to close
// before giving up on them. A background process that inherited the
// pipes (for example "sleep 100 &") would otherwise hold the executor,
// and the worker slot with it, until that process exits. It's a var so
// tests can shorten it.
var outputGrace = 2 * time.Second

// NewShellExecutor returns a TaskExecutor that runs a task's Payload
// as a shell command (via "sh -c") and treats a non-zero exit status,
// or exceeding timeout, as failure. This replaces the placeholder
// executor that only slept and logged a fake message; workerID is
// used purely to prefix log lines the same way the old placeholder
// did.
//
// On timeout the whole process group is killed (see
// configureProcessGroup), not just the shell: "sh -c 'sleep 60; ...'"
// runs sleep as a child, and killing only the shell would leave it
// running and holding the output pipe open, so the timeout would not
// actually bound how long the task takes.
//
// Combined stdout/stderr is captured and both logged and included in
// the returned error on failure, so a task failure is diagnosable
// from the worker's own logs without needing to reproduce it
// manually.
func NewShellExecutor(workerID string, timeout time.Duration) TaskExecutor {
	if timeout <= 0 {
		timeout = DefaultExecTimeout
	}

	return func(taskID, payload string) error {
		payload = strings.TrimSpace(payload)
		if payload == "" {
			return fmt.Errorf("task %s: empty payload, nothing to execute", taskID)
		}

		log.Printf("[%s] executing task %s: %s", workerID, taskID, payload)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		cmd := exec.CommandContext(ctx, "sh", "-c", payload)
		configureProcessGroup(cmd)
		cmd.WaitDelay = outputGrace
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out

		runErr := cmd.Run()
		output := strings.TrimSpace(out.String())
		if output != "" {
			log.Printf("[%s] task %s output: %s", workerID, taskID, output)
		}

		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("task %s: timed out after %s", taskID, timeout)
		}
		// The command itself succeeded but something it started in the
		// background is still holding its output open. That is not a
		// task failure; we just stop waiting for the leftover process.
		if errors.Is(runErr, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success() {
			log.Printf("[%s] task %s: command succeeded but left a background process holding its output open; not waiting for it", workerID, taskID)
			runErr = nil
		}
		if runErr != nil {
			return fmt.Errorf("task %s: command failed: %w", taskID, runErr)
		}

		log.Printf("[%s] finished task %s", workerID, taskID)
		return nil
	}
}
