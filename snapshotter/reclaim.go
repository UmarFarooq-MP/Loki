package snapshotter

import "loki/internal/orderbook"

// AdvanceEpochAndReclaim increments the global epoch and reclaims
// orders from the retire ring whose retireEpoch < MinReaderEpoch.
func AdvanceEpochAndReclaim(
	rq *orderbook.RetireRing,
	pool *orderbook.OrderPool,
	readers ...*Reader,
) {
	AdvanceEpoch()
	min := MinReaderEpoch(readers...)
	for {
		o := rq.Dequeue()
		if o == nil {
			break
		}
		if min == ^uint64(0) || o.retireEpoch < min {
			pool.Put(o)
		} else {
			_ = rq.Enqueue(o)
			break
		}
	}
}
