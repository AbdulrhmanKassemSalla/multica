package daemon

import (
	"sync/atomic"
	"time"
)

// hostSuspendDetector recognises a host sleep/suspend between two idle
// watchdog ticks so the slept time is not charged to the agent as silence
// (GitHub #9024).
//
// The watchdog stamps activity as wall-clock unix nanos, so a laptop that
// sleeps for longer than the idle window wakes up with every in-flight run
// already "silent" past budget and they are all force-stopped on the first
// tick. Nothing can run while the host is suspended — the agent cannot emit,
// and neither can the watchdog — so that interval says nothing about whether
// the agent is hung.
//
// Detection relies only on the wall clock and the ticker, which keeps it
// identical on every platform: the ticker fires roughly every `interval` of
// awake time, so a wall-clock gap between two ticks well beyond `interval`
// means the process was not running for the excess. That holds both where
// Go's monotonic clock stops during sleep (macOS, Linux: the next tick lands
// one interval after wake) and where it keeps counting (Windows: the overdue
// tick fires right after wake). A forward wall-clock step (NTP, manual change)
// is treated the same way, which is also correct: the wall-based idle reading
// would otherwise jump by that step.
type hostSuspendDetector struct {
	interval  time.Duration
	tolerance time.Duration
	prevWall  int64
}

func newHostSuspendDetector(interval time.Duration, now time.Time) *hostSuspendDetector {
	// One extra interval of tolerance absorbs scheduler/GC/load jitter. A
	// false positive would only delay a force stop slightly; a false negative
	// (a nap shorter than one tick) overcounts silence by at most one tick.
	return &hostSuspendDetector{interval: interval, tolerance: interval, prevWall: now.UnixNano()}
}

// observe records a tick at `now` and returns how much wall time elapsed
// since the previous tick without the process running. Zero means a normal
// tick. A backwards wall-clock step returns zero.
func (s *hostSuspendDetector) observe(now time.Time) time.Duration {
	wall := now.UnixNano()
	gap := time.Duration(wall - s.prevWall)
	s.prevWall = wall
	if gap <= s.interval+s.tolerance {
		return 0
	}
	return gap - s.interval
}

// creditSuspendedTime moves lastActivityAt forward by `suspended` so the idle
// reading only counts awake time, never past `now`. The clamp matters when
// activity already landed after wake (its stamp is fresh; shifting it would
// put it in the future and stretch the next budget). CAS keeps it monotone
// against concurrent drain-loop stamps.
func creditSuspendedTime(lastActivityAt *atomic.Int64, suspended time.Duration, now time.Time) {
	if suspended <= 0 {
		return
	}
	nowNanos := now.UnixNano()
	for {
		previous := lastActivityAt.Load()
		shifted := previous + int64(suspended)
		if shifted > nowNanos {
			shifted = nowNanos
		}
		if shifted <= previous || lastActivityAt.CompareAndSwap(previous, shifted) {
			return
		}
	}
}
