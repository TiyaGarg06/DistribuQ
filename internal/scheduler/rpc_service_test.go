package scheduler

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
)

// captureLog redirects the standard logger into a buffer for the
// duration of the test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	oldOut, oldFlags := log.Writer(), log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(oldOut)
		log.SetFlags(oldFlags)
	})
	return &buf
}

// TestFailTaskLogsCause checks that a worker-reported failure is
// logged with the task, the worker, and the cause the worker gave.
func TestFailTaskLogsCause(t *testing.T) {
	buf := captureLog(t)

	s := NewScheduler((&fakeDispatch{}).fn)
	s.RegisterWorker("w1", "localhost:9001")
	s.SubmitTask("t1", "payload")
	svc := &SchedulerService{S: s}

	var reply OKReply
	err := svc.FailTask(&FailArgs{TaskID: "t1", WorkerID: "w1", Cause: "exit status 1"}, &reply)
	if err != nil || !reply.OK {
		t.Fatalf("expected accepted failure report, got err=%v ok=%v", err, reply.OK)
	}

	got := buf.String()
	for _, want := range []string{"task t1 failed on worker w1", "exit status 1"} {
		if !strings.Contains(got, want) {
			t.Errorf("expected log to contain %q, got %q", want, got)
		}
	}
}

// TestFailTaskLogsStaleReport checks that a failure report ignored as
// stale is logged as ignored, not as a real failure.
func TestFailTaskLogsStaleReport(t *testing.T) {
	s, _ := newReassignedTask(t) // t1 now belongs to w2; w1 is a zombie
	buf := captureLog(t)
	svc := &SchedulerService{S: s}

	var reply OKReply
	err := svc.FailTask(&FailArgs{TaskID: "t1", WorkerID: "w1", Cause: "late boom"}, &reply)
	if !errors.Is(err, ErrStaleReport) || reply.OK {
		t.Fatalf("expected stale report to be rejected, got err=%v ok=%v", err, reply.OK)
	}

	got := buf.String()
	if !strings.Contains(got, "ignoring stale failure report for task t1 from worker w1") {
		t.Errorf("expected stale-report log line, got %q", got)
	}
	if strings.Contains(got, "failed on worker") {
		t.Errorf("a stale report must not be logged as a real failure, got %q", got)
	}
}

// TestFailTaskTruncatesHugeCause checks that very long causes are cut
// down before logging.
func TestFailTaskTruncatesHugeCause(t *testing.T) {
	buf := captureLog(t)

	s := NewScheduler((&fakeDispatch{}).fn)
	s.RegisterWorker("w1", "localhost:9001")
	s.SubmitTask("t1", "payload")
	svc := &SchedulerService{S: s}

	var reply OKReply
	huge := strings.Repeat("x", 5000)
	if err := svc.FailTask(&FailArgs{TaskID: "t1", WorkerID: "w1", Cause: huge}, &reply); err != nil {
		t.Fatalf("FailTask: %v", err)
	}

	got := buf.String()
	if len(got) > 1000 {
		t.Errorf("expected the logged cause to be truncated, log line was %d bytes", len(got))
	}
	if !strings.Contains(got, "(truncated)") {
		t.Errorf("expected a truncation marker, got %q", got)
	}
}
