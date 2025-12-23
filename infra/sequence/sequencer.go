package sequence

import "sync/atomic"

// Sequencer generates strictly monotonic sequence IDs.
// It is deterministic and replay-safe.
type Sequencer struct {
	next atomic.Uint64
}

// New creates a sequencer starting from a given value.
// On fresh start → start = 0
// On replay → start = last replayed seq
func New(start uint64) *Sequencer {
	s := &Sequencer{}
	s.next.Store(start)
	return s
}

// Next returns the next global sequence ID.
func (s *Sequencer) Next() uint64 {
	return s.next.Add(1)
}

// Current returns the last issued sequence.
func (s *Sequencer) Current() uint64 {
	return s.next.Load()
}

// Reset sets the sequencer to a specific value.
// This is ONLY used after WAL replay.
func (s *Sequencer) Reset(v uint64) {
	s.next.Store(v)
}
