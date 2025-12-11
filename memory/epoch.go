package memory

import "sync/atomic"

// GlobalEpoch tracks global reclamation epochs across the system.
var GlobalEpoch atomic.Uint64

// ReaderEpoch tracks the read epoch of a snapshot reader.
type ReaderEpoch struct {
	value atomic.Uint64
}

// Enter marks the reader as active at the current epoch.
func (r *ReaderEpoch) Enter() {
	r.value.Store(GlobalEpoch.Load())
}

// Exit marks the reader as inactive.
func (r *ReaderEpoch) Exit() {
	r.value.Store(^uint64(0)) // max value = idle
}

// AdvanceEpochAndReclaim increments the epoch and reclaims safe retired objects.
func AdvanceEpochAndReclaim(rq *RetireRing, pool *OrderPool, readers ...*ReaderEpoch) {
	GlobalEpoch.Add(1)
	min := minReaderEpoch(readers...)
	for {
		ref := rq.Dequeue()
		if ref == nil {
			break
		}
		if min == ^uint64(0) {
			pool.Put(ref)
		} else {
			_ = rq.Enqueue(ref)
			break
		}
	}
}

func minReaderEpoch(rs ...*ReaderEpoch) uint64 {
	min := ^uint64(0)
	for _, r := range rs {
		v := r.value.Load()
		if v < min {
			min = v
		}
	}
	return min
}
