package main

import (
	"testing"
	"time"
)

func newTestEnv() (*OrderBook, *OrderPool, *retireRing, *Reader) {
	book := NewOrderBook()
	pool := NewOrderPool(1024)
	rq := newRetireRing(128)
	reader := &Reader{}
	return book, pool, rq, reader
}

func TestLimitOrderInsertAndMatch(t *testing.T) {
	book, pool, rq, _ := newTestEnv()
	book.placeOrder(Bid, LimitOrder, 100, 5, 1, 1, pool, rq)
	book.placeOrder(Ask, LimitOrder, 100, 5, 2, 2, pool, rq)

	if book.Bids.Size() != 0 || book.Asks.Size() != 0 {
		t.Error("orders should have matched and book emptied")
	}
}

func TestIOCOrder(t *testing.T) {
	book, pool, rq, _ := newTestEnv()
	book.placeOrder(Bid, IOCOrder, 100, 5, 1, 1, pool, rq)

	if book.Bids.Size() != 0 {
		t.Error("IOC order should not persist in the book")
	}
}

func TestFOKOrder(t *testing.T) {
	book, pool, rq, _ := newTestEnv()
	book.placeOrder(Bid, FOKOrder, 100, 5, 1, 1, pool, rq)

	if book.Bids.Size() != 0 {
		t.Error("FOK order without full fill should not persist")
	}
}

func TestPostOnlyOrder(t *testing.T) {
	book, pool, rq, _ := newTestEnv()
	book.placeOrder(Bid, PostOnlyOrder, 100, 5, 1, 1, pool, rq)

	if book.Bids.Size() != 1 {
		t.Error("post-only order should rest in the book")
	}
}

func TestBidAskseparation(t *testing.T) {
	book, pool, rq, _ := newTestEnv()
	book.placeOrder(Bid, LimitOrder, 100, 1, 1, 1, pool, rq)
	book.placeOrder(Ask, LimitOrder, 200, 1, 2, 2, pool, rq)

	if book.Bids.Size() != 1 || book.Asks.Size() != 1 {
		t.Error("Bids and Asks should be in separate trees")
	}
}

func TestCancelAndReclaim(t *testing.T) {
	book, pool, rq, _ := newTestEnv()
	o := book.placeOrder(Bid, LimitOrder, 100, 1, 1, 1, pool, rq)
	book.cancelOrder(1, o, rq, Bid)
	if book.Bids.Size() != 0 {
		t.Error("order should have been cancelled")
	}
}

func TestSnapshotIter(t *testing.T) {
	book, pool, rq, reader := newTestEnv()
	book.placeOrder(Bid, LimitOrder, 100, 1, 1, 1, pool, rq)
	book.placeOrder(Ask, LimitOrder, 101, 1, 2, 2, pool, rq)

	foundBid, foundAsk := false, false
	book.SnapshotActiveIter(reader, func(price int64, o *Order) {
		if o.Side == Bid {
			foundBid = true
		}
		if o.Side == Ask {
			foundAsk = true
		}
	})
	if !foundBid || !foundAsk {
		t.Error("snapshot did not visit all orders")
	}
}

func TestCancelNonexistentOrder(t *testing.T) {
	book, _, rq, _ := newTestEnv()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on cancelOrder(nil), but got none")
		}
	}()
	book.cancelOrder(123, nil, rq, Bid)
}

func TestSnapshotEmptyBook(t *testing.T) {
	book, _, _, reader := newTestEnv()
	called := false
	book.SnapshotActiveIter(reader, func(price int64, o *Order) {
		called = true
	})
	if called {
		t.Error("snapshot on empty book should not call callback")
	}
}

func TestOrderPoolExhaustion(t *testing.T) {
	book := NewOrderBook()
	pool := NewOrderPool(1) // deliberately tiny
	rq := newRetireRing(1)

	// first order works
	_ = book.placeOrder(Bid, LimitOrder, 100, 1, time.Now().UnixNano(), 1, pool, rq)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on pool exhaustion, but got none")
		}
	}()
	// second order must panic
	book.placeOrder(Bid, LimitOrder, 101, 1, time.Now().UnixNano(), 2, pool, rq)
}
