package main

import (
	"sync/atomic"
	"testing"
	"time"
)

// ---------------- Basic Benchmarks ---------------- //

func BenchmarkPlaceOrder(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(max(b.N, 1<<22))
	rq := newRetireRing(uint64(b.N) * 2)
	seq := uint64(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = book.placeOrder(Bid, Limit, 100, uint64(i), 1000, seq, pool, rq)
		seq++
	}
}

func BenchmarkCancelOrder(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(max(b.N, 1<<22))
	rq := newRetireRing(uint64(b.N) * 2)

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

func BenchmarkSnapshot(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(1 << 22)
	rq := newRetireRing(1 << 18)

	// preload with non-crossing orders so snapshot is stable
	for i := 0; i < 50000; i++ {
		if i%2 == 0 {
			_ = book.placeOrder(Bid, Limit, 99, uint64(i), 1000, uint64(i+1), pool, rq)
		} else {
			_ = book.placeOrder(Ask, Limit, 101, uint64(i), 1000, uint64(i+1), pool, rq)
		}
	}
	reader := &Reader{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		book.SnapshotActiveIter(reader, func(p int64, o *Order) {
			count++
		})
		if count == 0 {
			b.Fatal("snapshot returned no orders")
		}
	}
}

func BenchmarkMixedPlaceCancel(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(max(b.N*2, 1<<22))
	rq := newRetireRing(uint64(b.N) * 2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := book.placeOrder(Bid, Limit, 100, uint64(i), 1000, uint64(i+1), pool, rq)
		if i%2 == 0 {
			book.cancelOrder(100, o, rq, Bid)
		}
	}
}

// ---------------- Parallel Versions (safe) ---------------- //

// Placement only (high throughput)
func BenchmarkParallelPlaceOnly(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(max(b.N*2, 1<<22))
	rq := newRetireRing(uint64(b.N) * 2)
	seq := uint64(1)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			localSeq := atomic.AddUint64(&seq, 1)
			_ = book.placeOrder(Bid, Limit, 100, localSeq, 1000, localSeq, pool, rq)
			// occasionally reclaim so retireQ doesn’t fill
			if localSeq%1000 == 0 {
				advanceEpochAndReclaim(rq, pool, &Reader{})
			}
		}
	})
}

// Cancel only (preloaded orders)
func BenchmarkParallelCancelOnly(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(max(b.N*2, 1<<22))
	rq := newRetireRing(uint64(b.N) * 2)

	orders := make([]*Order, b.N)
	for i := 0; i < b.N; i++ {
		orders[i] = book.placeOrder(Bid, Limit, 100, uint64(i), 1000, uint64(i+1), pool, rq)
	}
	idx := int64(0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&idx, 1)
			i := int(n) % len(orders)
			if orders[i].Status == Active {
				book.cancelOrder(100, orders[i], rq, Bid)
			}
			if n%1000 == 0 {
				advanceEpochAndReclaim(rq, pool, &Reader{})
			}
		}
	})
}

// ---------------- Stress Benchmarks ---------------- //

func BenchmarkThroughputStress(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(max(b.N*2, 1<<22))
	rq := newRetireRing(uint64(b.N) * 4)
	seq := uint64(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		side := Bid
		price := int64(100)
		if i%2 == 0 {
			side = Ask
			price = 99 // ensures crossing
		}
		_ = book.placeOrder(side, Limit, price, uint64(i), 1, seq, pool, rq)
		seq++
	}
}

func BenchmarkIOCOrders(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(max(b.N*2, 1<<22))
	rq := newRetireRing(uint64(b.N) * 4)
	seq := uint64(1)

	// preload asks so IOC can match
	for i := 0; i < 1000; i++ {
		_ = book.placeOrder(Ask, Limit, 100, uint64(i), 1, uint64(i+1), pool, rq)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = book.placeOrder(Bid, IOC, 100, uint64(i), 1, atomic.AddUint64(&seq, 1), pool, rq)
	}
}

func BenchmarkFOKOrders(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(max(b.N*2, 1<<22))
	rq := newRetireRing(uint64(b.N) * 4)
	seq := uint64(1)

	// preload limited depth
	for i := 0; i < 10; i++ {
		_ = book.placeOrder(Ask, Limit, 100, uint64(i), 1, uint64(i+1), pool, rq)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = book.placeOrder(Bid, FOK, 100, uint64(i), 20, atomic.AddUint64(&seq, 1), pool, rq)
	}
}

func BenchmarkPostOnlyOrders(b *testing.B) {
	book := NewOrderBook()
	pool := NewOrderPool(max(b.N*2, 1<<22))
	rq := newRetireRing(uint64(b.N) * 4)
	seq := uint64(1)

	// preload best ask
	_ = book.placeOrder(Ask, Limit, 100, 1, 1, 1, pool, rq)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		price := int64(101)
		if i%2 == 0 {
			price = 99 // crosses, should reject
		}
		_ = book.placeOrder(Bid, PostOnly, price, uint64(i), 1, atomic.AddUint64(&seq, 1), pool, rq)
	}
}

// ---------------- Concurrency Simulation ---------------- //

// Not a benchmark, but simulates production: one writer, one reader, one reclaimer
func TestConcurrentModel(t *testing.T) {
	book := NewOrderBook()
	pool := NewOrderPool(1 << 22)
	rq := newRetireRing(1 << 20)
	seq := uint64(1)

	// placement goroutine (writer)
	go func() {
		for i := 0; i < 1_000_000; i++ {
			_ = book.placeOrder(Bid, Limit, 100, uint64(i), 1000, atomic.AddUint64(&seq, 1), pool, rq)
		}
	}()

	// snapshot goroutine (reader)
	go func() {
		reader := &Reader{}
		for i := 0; i < 500; i++ {
			book.SnapshotActiveIter(reader, func(p int64, o *Order) {})
			time.Sleep(50 * time.Microsecond)
		}
	}()

	// reclaimer goroutine
	go func() {
		reader := &Reader{}
		for i := 0; i < 2000; i++ {
			advanceEpochAndReclaim(rq, pool, reader)
			time.Sleep(20 * time.Microsecond)
		}
	}()

	time.Sleep(2 * time.Second)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
