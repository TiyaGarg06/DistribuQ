package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// withOutputGrace sets the executor's output-grace wait for one test.
func withOutputGrace(t *testing.T, d time.Duration) {
	t.Helper()
	old := outputGrace
	outputGrace = d
	t.Cleanup(func() { outputGrace = old })
}

func TestShellExecutorSucceeds(t *testing.T) {
	exec := NewShellExecutor("w1", time.Second)
	if err := exec("t1", "exit 0"); err != nil {
		t.Errorf("expected success, got error: %v", err)
	}
}

func TestShellExecutorNonZeroExitFails(t *testing.T) {
	exec := NewShellExecutor("w1", time.Second)
	err := exec("t1", "exit 1")
	if err == nil {
		t.Fatal("expected error for non-zero exit, got nil")
	}
	if !strings.Contains(err.Error(), "t1") {
		t.Errorf("expected error to reference task id, got: %v", err)
	}
}

func TestShellExecutorTimeout(t *testing.T) {
	exec := NewShellExecutor("w1", 50*time.Millisecond)
	err := exec("t1", "sleep 1")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected timeout error, got: %v", err)
	}
}

func TestShellExecutorEmptyPayloadFails(t *testing.T) {
	exec := NewShellExecutor("w1", time.Second)
	err := exec("t1", "   ")
	if err == nil {
		t.Fatal("expected error for empty payload, got nil")
	}
	if !strings.Contains(err.Error(), "empty payload") {
		t.Errorf("expected empty-payload error, got: %v", err)
	}
}

func TestShellExecutorDefaultTimeoutAppliedWhenNonPositive(t *testing.T) {
	exec := NewShellExecutor("w1", 0)
	if err := exec("t1", "exit 0"); err != nil {
		t.Errorf("expected success with default timeout fallback, got: %v", err)
	}
}

// TestShellExecutorTimeoutKillsChildProcesses is the original bug: the
// payload runs sleep as a child of the shell, and the timeout used to
// kill only the shell, so the call took the child's full 6 seconds
// instead of the 300ms timeout. The output-grace wait is set very long
// here so that only a real kill of the child can return promptly.
func TestShellExecutorTimeoutKillsChildProcesses(t *testing.T) {
	withOutputGrace(t, 30*time.Second)
	exec := NewShellExecutor("w1", 300*time.Millisecond)

	start := time.Now()
	err := exec("t1", "sleep 6; echo done")
	elapsed := time.Since(start)

	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected a timeout error, got: %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("a 300ms timeout took %v; the child process was not killed", elapsed)
	}
}

// TestShellExecutorTimeoutKillsWholeProcessGroup checks that nothing
// the command started is left running: a background subshell is set to
// write a file one second in, long after the 200ms timeout, and must
// never get the chance.
func TestShellExecutorTimeoutKillsWholeProcessGroup(t *testing.T) {
	withOutputGrace(t, 100*time.Millisecond)
	survivor := filepath.Join(t.TempDir(), "survivor")
	exec := NewShellExecutor("w1", 200*time.Millisecond)

	err := exec("t1", fmt.Sprintf("(sleep 1; echo survived > '%s') & wait", survivor))
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected a timeout error, got: %v", err)
	}

	time.Sleep(1500 * time.Millisecond) // well past when a survivor would have written
	if _, statErr := os.Stat(survivor); statErr == nil {
		t.Error("a process started by the task survived the timeout and ran to completion")
	}
}

// TestShellExecutorDoesNotWaitForBackgroundProcess checks that a
// command which exits successfully but leaves a background process
// holding its output open is reported as a success promptly, rather
// than hanging until that process ends (3 seconds here).
func TestShellExecutorDoesNotWaitForBackgroundProcess(t *testing.T) {
	withOutputGrace(t, 100*time.Millisecond)
	exec := NewShellExecutor("w1", 30*time.Second)

	start := time.Now()
	err := exec("t1", "echo hi; sleep 3 &")
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("a command that exited 0 should succeed even with a lingering background process, got: %v", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("executor waited %v for a background process instead of returning", elapsed)
	}
}

// TestShellExecutorFailureStillReportedWithBackgroundProcess checks the
// other side of the same rule: leftover output holders must not turn a
// genuinely failing command into a success.
func TestShellExecutorFailureStillReportedWithBackgroundProcess(t *testing.T) {
	withOutputGrace(t, 100*time.Millisecond)
	exec := NewShellExecutor("w1", 30*time.Second)

	err := exec("t1", "sleep 3 & exit 7")
	if err == nil {
		t.Fatal("expected a failure for a non-zero exit, got nil")
	}
	if !strings.Contains(err.Error(), "command failed") {
		t.Errorf("expected a command-failed error, got: %v", err)
	}
}
