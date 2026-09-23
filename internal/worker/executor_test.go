package worker

import (
	"strings"
	"testing"
	"time"
)

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
