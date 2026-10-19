package memory

import (
	"sync"
	"time"
)

// FixedClock implements ports.Clock with a settable time, for deterministic
// tests of lease expiry.
type FixedClock struct {
	mu  sync.RWMutex
	now time.Time
}

// NewFixedClock constructs a FixedClock starting at now.
func NewFixedClock(now time.Time) *FixedClock {
	return &FixedClock{now: now}
}

func (c *FixedClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Set moves the clock to now.
func (c *FixedClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// Advance moves the clock forward by d.
func (c *FixedClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
