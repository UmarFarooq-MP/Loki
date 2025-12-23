package memory

import "sync/atomic"

// GlobalEpoch monotonically increases.
var GlobalEpoch atomic.Uint64

const inactive = ^uint64(0)

// ReaderEpoch marks when a reader entered a read section.
type ReaderEpoch struct {
	epoch atomic.Uint64
}

func (r *ReaderEpoch) Enter() {
	r.epoch.Store(GlobalEpoch.Load())
}

func (r *ReaderEpoch) Exit() {
	r.epoch.Store(inactive)
}

func (r *ReaderEpoch) Value() uint64 {
	return r.epoch.Load()
}

// ReclaimablePool is the ONLY requirement for reclamation.
// It is intentionally type-erased.
type ReclaimablePool interface {
	PutAny(any)
}

// AdvanceEpochAndReclaim advances the epoch and reclaims
// retired objects that are safe.
func AdvanceEpochAndReclaim(
	ring *RetireRing,
	pool ReclaimablePool,
	readers ...*ReaderEpoch,
) {
	GlobalEpoch.Add(1)
	min := minReaderEpoch(readers...)

	for {
		obj := ring.Dequeue()
		if obj == nil {
			return
		}

		if min == inactive {
			pool.PutAny(obj)
			continue
		}

		// Not safe yet → FIFO guarantees newer ones aren't either
		_ = ring.Enqueue(obj)
		return
	}
}

func minReaderEpoch(rs ...*ReaderEpoch) uint64 {
	min := inactive
	for _, r := range rs {
		if r == nil {
			continue
		}
		v := r.Value()
		if v < min {
			min = v
		}
	}
	return min
}
