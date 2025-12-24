package service

import (
	"testing"

	"loki/domain/orderbook"
	"loki/infra/memory"
	"loki/infra/sequence"
	entrywal "loki/infra/wal/entry"
	exitwal "loki/infra/wal/exit"
	"loki/snapshot"
)

func BenchmarkPlaceOrder_Core(b *testing.B) {
	book := orderbook.NewOrderBook()

	pool := memory.NewPool(func() *orderbook.Order {
		return &orderbook.Order{}
	})
	ring := memory.NewRetireRing(4096)

	seq := sequence.New(0)
	reader := snapshot.NewReader()

	entryWAL, _ := entrywal.Open(entrywal.Config{
		Dir:         b.TempDir(),
		SegmentSize: 64 << 20,
	})
	exitWAL, _ := exitwal.Open(b.TempDir())

	svc := NewOrderService(
		book,
		pool,
		ring,
		reader,
		seq,
		entryWAL,
		exitWAL,
	)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			svc.PlaceOrder(
				orderbook.Bid,
				orderbook.Limit,
				100,
				1,
				1,
			)
		}
	})
}
