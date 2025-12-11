package snapshotter

import (
	"loki/memory"
	"loki/rcu"
)

// AdvanceEpochAndReclaim moves global epoch forward
// and reclaims orders safe to reuse.
func AdvanceEpochAndReclaim(rq *memory.RetireRing, pool *memory.OrderPool, readers ...*rcu.Reader) {
	rcu.AdvanceEpoch()
	min := rcu.MinReaderEpoch(readers...)

	for {
		o := rq.Dequeue()
		if o == nil {
			break
		}
		if min == ^uint64(0) || o.RetireEpoch() < min {
			pool.Put(o)
		} else {
			_ = rq.Enqueue(o)
			break
		}
	}
}
