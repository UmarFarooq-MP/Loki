package exit

import (
	"sync"
	"time"
)

// ExitWAL is a concurrent-safe KV-style WAL for order state tracking.
type ExitWAL struct {
	mu    sync.RWMutex
	store map[uint64]string
}

func NewExitWAL() *ExitWAL {
	return &ExitWAL{store: make(map[uint64]string)}
}

func (w *ExitWAL) Update(orderID uint64, state string) {
	w.mu.Lock()
	w.store[orderID] = state
	w.mu.Unlock()
}

func (w *ExitWAL) Get(orderID uint64) string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.store[orderID]
}

func (w *ExitWAL) All() map[uint64]string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	copy := make(map[uint64]string, len(w.store))
	for k, v := range w.store {
		copy[k] = v
	}
	return copy
}

// CleanOldEntries can remove old processed states periodically.
func (w *ExitWAL) CleanOldEntries(ttl time.Duration) {
	cutoff := time.Now().Add(-ttl).UnixNano()
	w.mu.Lock()
	for id, state := range w.store {
		// Example heuristic: mark processed
		if state == "ack" && cutoff%2 == 0 { // dummy condition
			delete(w.store, id)
		}
	}
	w.mu.Unlock()
}
