package orderbook

import (
	"fmt"
	"sync/atomic"
	"time"

	"loki/memory"
)

type OrderBook struct {
	Bids    *RBTree
	Asks    *RBTree
	LastSeq atomic.Uint64
}

// NewOrderBook creates a new empty order book.
func NewOrderBook() *OrderBook {
	return &OrderBook{
		Bids: NewRBTree(),
		Asks: NewRBTree(),
	}
}

// PlaceOrder inserts an order into the book or matches it immediately.
func (b *OrderBook) PlaceOrder(
	side Side,
	otype OrderType,
	price int64,
	qty int64,
	userID uint64,
	seq uint64,
	pool *memory.OrderPool,
	rq *memory.RetireRing,
) *Order {
	o := pool.Get()
	if o == nil {
		panic("order pool exhausted")
	}
	*o = Order{
		ID:     userID,
		Side:   side,
		Type:   otype,
		Price:  price,
		Qty:    qty,
		SeqID:  seq,
		Status: Active,
	}
	b.LastSeq.Store(seq)

	// Match
	filled := b.match(o, rq)
	o.Filled = filled

	// Handle leftovers
	switch o.Type {
	case Limit:
		if o.Qty > 0 {
			b.enqueue(o)
		}
	case PostOnly:
		if filled > 0 {
			o.Status = Inactive
			_ = rq.Enqueue(o)
		} else {
			b.enqueue(o)
		}
	default: // IOC, FOK, Market
		if o.Qty > 0 {
			o.Status = Inactive
			_ = rq.Enqueue(o)
		}
	}
	return o
}

// Match executes matching logic between opposite sides.
func (b *OrderBook) match(o *Order, rq *memory.RetireRing) int64 {
	filled := int64(0)
	if o.Side == Bid {
		for o.Qty > 0 {
			bestAsk := b.Asks.MinLevel()
			if bestAsk == nil || (o.Type != Market && bestAsk.Price > o.Price) {
				break
			}
			head := bestAsk.head
			trade := min(o.Qty, head.Qty)
			o.Qty -= trade
			head.Qty -= trade
			filled += trade

			if head.Qty == 0 {
				b.cancelOrder(bestAsk.Price, head, rq, Ask)
			}
		}
	} else {
		for o.Qty > 0 {
			bestBid := b.Bids.MaxLevel()
			if bestBid == nil || (o.Type != Market && bestBid.Price < o.Price) {
				break
			}
			head := bestBid.head
			trade := min(o.Qty, head.Qty)
			o.Qty -= trade
			head.Qty -= trade
			filled += trade

			if head.Qty == 0 {
				b.cancelOrder(bestBid.Price, head, rq, Bid)
			}
		}
	}
	return filled
}

// enqueue adds order to proper price level.
func (b *OrderBook) enqueue(o *Order) {
	if o.Side == Bid {
		level := b.Bids.UpsertLevel(o.Price)
		level.Enqueue(o)
	} else {
		level := b.Asks.UpsertLevel(o.Price)
		level.Enqueue(o)
	}
}

// cancelOrder removes order and recycles it.
func (b *OrderBook) cancelOrder(price int64, o *Order, rq *memory.RetireRing, side Side) {
	o.Status = Inactive
	o.retireEpoch = uint64(time.Now().UnixNano())

	var lvl *PriceLevel
	if side == Bid {
		lvl = b.Bids.FindLevel(price)
	} else {
		lvl = b.Asks.FindLevel(price)
	}

	if lvl != nil {
		lvl.unlinkAlreadyInactive(o)
		if lvl.head == nil {
			if side == Bid {
				b.Bids.DeleteLevel(price)
			} else {
				b.Asks.DeleteLevel(price)
			}
		}
	}
	if !rq.Enqueue(o) {
		panic("retire ring full")
	}
}

// SnapshotActiveIter iterates through all active orders safely.
func (b *OrderBook) SnapshotActiveIter(visit func(price int64, o *Order)) {
	b.Bids.ForEachDescending(func(lvl *PriceLevel) bool {
		for n := lvl.head; n != nil; n = n.next {
			if n.Status == Active {
				visit(lvl.Price, n)
			}
		}
		return true
	})
	b.Asks.ForEachAscending(func(lvl *PriceLevel) bool {
		for n := lvl.head; n != nil; n = n.next {
			if n.Status == Active {
				visit(lvl.Price, n)
			}
		}
		return true
	})
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
