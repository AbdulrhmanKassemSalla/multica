package daemon

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

// suspendableClock is a wall clock that can jump forward to simulate a host
// suspend: the bubble's fake time keeps advancing only while "awake", and
// sleep() adds wall time the process never ran for.
type suspendableClock struct{ offset atomic.Int64 }

func (c *suspendableClock) now() time.Time        { return time.Now().Add(time.Duration(c.offset.Load())) }
func (c *suspendableClock) sleep(d time.Duration) { c.offset.Add(int64(d)) }

func TestHostSuspendDetector(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	s := newHostSuspendDetector(time.Minute, base)

	if got := s.observe(base.Add(time.Minute + 5*time.Second)); got != 0 {
		t.Fatalf("normal tick with jitter reported suspend %s", got)
	}
	// Ticker fired one awake interval after the previous tick, but 3h of wall
	// time passed while the host was asleep.
	at := base.Add(time.Minute + 5*time.Second + 3*time.Hour + time.Minute)
	if got := s.observe(at); got != 3*time.Hour {
		t.Fatalf("suspend = %s, want 3h", got)
	}
	if got := s.observe(at.Add(-time.Hour)); got != 0 {
		t.Fatalf("backwards wall step reported suspend %s", got)
	}
}

func TestCreditSuspendedTimeClampsToNow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var last atomic.Int64

	// Activity before sleep: shifted by exactly the slept time.
	last.Store(now.Add(-4 * time.Hour).UnixNano())
	creditSuspendedTime(&last, 3*time.Hour, now)
	if got := now.Sub(time.Unix(0, last.Load())); got != time.Hour {
		t.Fatalf("idle after credit = %s, want 1h", got)
	}

	// Activity already after wake: never pushed into the future.
	last.Store(now.Add(-time.Second).UnixNano())
	creditSuspendedTime(&last, 3*time.Hour, now)
	if got := last.Load(); got != now.UnixNano() {
		t.Fatalf("credit pushed stamp past now: %d > %d", got, now.UnixNano())
	}
}

// GitHub #9024: a host that sleeps longer than the idle window must not have
// its in-flight runs force-stopped on wake, while a run that stays silent for
// a full window of awake time is still stopped.
func TestIdleWatchdogDoesNotCountHostSuspendAsSilence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tools int32
	}{
		{name: "no tool in flight", tools: 0},
		{name: "tool in flight", tools: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				clock := &suspendableClock{}
				var last, threshold atomic.Int64
				var fired atomic.Bool
				last.Store(clock.now().UnixNano())
				// Mirrors the Cursor native-tool callback: it republishes a
				// stale pre-sleep stamp, which must not undo the credit.
				staleToolStamp := last.Load()
				tools := func() int32 {
					for {
						previous := last.Load()
						if staleToolStamp <= previous || last.CompareAndSwap(previous, staleToolStamp) {
							break
						}
					}
					return tc.tools
				}

				// 1h window → 30m ticks.
				go new(Daemon).runIdleWatchdogWithClock(ctx, time.Hour, time.Hour, &last, tools, &fired, &threshold,
					cancel, make(chan agent.Message), nil, nil, slog.Default(), clock.now)

				synctest.Wait()
				time.Sleep(10 * time.Minute)
				clock.sleep(3 * time.Hour) // lid closed for 3h, well past the window
				time.Sleep(21 * time.Minute)
				synctest.Wait()
				if fired.Load() || ctx.Err() != nil {
					t.Fatal("host suspend was counted as agent silence and force-stopped the run")
				}

				// Genuinely silent for a full window of awake time: still stopped.
				time.Sleep(30 * time.Minute)
				synctest.Wait()
				if !fired.Load() || ctx.Err() == nil {
					t.Fatal("watchdog no longer stops a run silent for a full awake window")
				}
				if got := time.Duration(threshold.Load()); got != time.Hour {
					t.Fatalf("threshold = %s, want 1h", got)
				}
			})
		})
	}
}
