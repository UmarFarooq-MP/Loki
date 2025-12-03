package order_book

import (
	"loki"
	"testing"
)

func BenchmarkPlaceOrder(b *testing.B) {
	book := NewOrderBook()
	book.Log = nil
	pool := NewOrderPool(max(b.N, 1<<22))
	rq := main.newRetireRing(uint64(b.N) * 2)
	seq := uint64(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = book.placeOrder(Bid, Limit, 100, uint64(i), 1000, seq, pool, rq)
		seq++
	}
}

func BenchmarkCancelOrder(b *testing.B) {
	book := NewOrderBook()
	book.Log = nil
	pool := NewOrderPool(max(b.N, 1<<22))
	rq := main.newRetireRing(uint64(b.N) * 2)

	var orders []*Order
	for i := 0; i < b.N; i++ {
		o := book.placeOrder(Bid, Limit, 100, uint64(i), 1000, uint64(i+1), pool, rq)
		orders = append(orders, o)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		book.cancelOrder(100, orders[i], rq, Bid)
	}
}

// (all other benchmarks remain identical; just ensure `book.Log = nil`)
