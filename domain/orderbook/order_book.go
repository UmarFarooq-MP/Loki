package orderbook

import "sync/atomic"

// OrderBook is single-writer and deterministic.
type OrderBook struct {
	Bids *RBTree
	Asks *RBTree

	LastSeq atomic.Uint64
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		Bids: NewRBTree(),
		Asks: NewRBTree(),
	}
}

func (b *OrderBook) Place(o *Order) {
	b.LastSeq.Store(o.SeqID)

	if o.Side == Bid {
		b.matchBid(o)
		if o.Remaining() > 0 && o.Type == Limit {
			b.Bids.GetOrCreate(o.Price).Enqueue(o)
		}
	} else {
		b.matchAsk(o)
		if o.Remaining() > 0 && o.Type == Limit {
			b.Asks.GetOrCreate(o.Price).Enqueue(o)
		}
	}

	if o.Remaining() == 0 {
		o.Status = Inactive
	}
}

// ---- traversal helpers ----

func (b *OrderBook) BidsWalk(fn func(*PriceLevel)) {
	b.Bids.walkDesc(fn)
}

func (b *OrderBook) AsksWalk(fn func(*PriceLevel)) {
	b.Asks.walkAsc(fn)
}

// ---- matching ----

func (b *OrderBook) matchBid(o *Order) {
	for o.Remaining() > 0 {
		best := b.Asks.BestMin()
		if best == nil {
			return
		}
		if o.Type != Market && best.Price > o.Price {
			return
		}

		head := best.Head()
		trade := min(o.Remaining(), head.Remaining())

		o.Filled += trade
		head.Filled += trade

		if head.Remaining() == 0 {
			head.Status = Inactive
			best.PopHead()
		}
	}
}

func (b *OrderBook) matchAsk(o *Order) {
	for o.Remaining() > 0 {
		best := b.Bids.BestMax()
		if best == nil {
			return
		}
		if o.Type != Market && best.Price < o.Price {
			return
		}

		head := best.Head()
		trade := min(o.Remaining(), head.Remaining())

		o.Filled += trade
		head.Filled += trade

		if head.Remaining() == 0 {
			head.Status = Inactive
			best.PopHead()
		}
	}
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
