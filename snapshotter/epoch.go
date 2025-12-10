package snapshotter

import "sync/atomic"

// globalEpoch tracks the current system-wide epoch.
// Each snapshot reader records the epoch value when it begins reading.
var globalEpoch atomic.Uint64

// Reader represents a snapshot reader (e.g., snapshotter or client).
// It holds its current epoch, so we can detect the oldest active reader.
type Reader struct {
	epoch uint64
}

// EnterRead marks the start of a read phase (snapshot).
func (r *Reader) EnterRead() {
	r.epoch = globalEpoch.Load()
}

// ExitRead marks the end of a read phase.
// It does not modify the global epoch; reclamation will check this.
func (r *Reader) ExitRead() {
	// no-op (we can add debug hooks later if needed)
}

// AdvanceEpoch increments the global epoch number.
// Typically called by the snapshot coordinator.
func AdvanceEpoch() uint64 {
	return globalEpoch.Add(1)
}

// MinReaderEpoch returns the smallest epoch across all active readers.
// Used to determine which retired orders are safe to reclaim.
func MinReaderEpoch(readers ...*Reader) uint64 {
	if len(readers) == 0 {
		return ^uint64(0) // max uint64 (no readers)
	}
	min := readers[0].epoch
	for _, r := range readers {
		if r.epoch < min {
			min = r.epoch
		}
	}
	return min
}
