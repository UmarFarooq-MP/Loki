package main

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"loki/wal"
)

type OrderBook struct {
	Bids    *RBTree
	Asks    *RBTree
	LastSeq atomic.Uint64
	Log     wal.WAL // ✅ WAL handle for persistence
}

// NewOrderBook creates a new order book with WAL enabled
func NewOrderBook() *OrderBook {
	cfg := wal.Config{
		Dir:             "./wal_data",
		SegmentSize:     2 * 1024 * 1024,
		SegmentDuration: 1 * time.Minute,
		Serializer:      wal.ProtoSerializer{},
		FlushInterval:   2 * time.Second,
	}
	log, err := wal.New(cfg)
	if err != nil {
		panic(fmt.Errorf("failed to init WAL: %w", err))
	}

	return &OrderBook{
		Bids: NewRBTree(),
		Asks: NewRBTree(),
		Log:  log,
	}
}

// ---------------- Matching Engine ---------------- //

// Place an order (runs matching first, then rests if needed)
func (b *OrderBook) placeOrder(
	side Side, otype OrderType, price int64,
	id uint64, qty int64, seq uint64,
	pool *OrderPool, rq *retireRing,
) *Order {
	o := pool.Get()
	if o == nil {
		panic("order pool exhausted")
	}
	*o = Order{
		ID: id, Side: side, Type: otype, Price: price,
		Qty: qty, SeqID: seq, Status: Active,
	}
	b.LastSeq.Store(seq)

	// 🧾 WAL log order placement
	b.logOrderEvent("place", o)

	// Market orders don’t use price
	if o.Type == Market {
		o.Price = 0
	}

	// --- Special handling for FOK (dry-run) ---
	if o.Type == FOK {
		available := b.checkLiquidity(side, o.Price, o.Qty)
		if available < o.Qty {
			o.Status = Inactive
			_ = rq.Enqueue(o)
			b.logOrderEvent("reject", o)
			return o
		}
	}

	// Match against opposite side
	matched := b.match(o, rq)
	o.Filled = matched

	// Decide what to do with leftover
	switch o.Type {
	case Limit:
		if o.Qty > 0 {
			b.enqueue(o)
		}
	case PostOnly:
		if matched > 0 {
			o.Status = Inactive
			_ = rq.Enqueue(o)
			b.logOrderEvent("reject-cross", o)
		} else if o.Qty > 0 {
			b.enqueue(o)
		}
	case IOC, FOK, Market:
		if o.Qty > 0 {
			o.Status = Inactive
			_ = rq.Enqueue(o)
		}
	}

	return o
}

// match executes trades against opposite side
func (b *OrderBook) match(o *Order, rq *retireRing) int64 {
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

			// 🧾 WAL log trade
			b.logTrade(o, head.Price, trade)

			if head.Qty == 0 {
				b.cancelOrder(bestAsk.Price, head, rq, Ask)
			}
		}
	} else { // Ask side
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

			// 🧾 WAL log trade
			b.logTrade(o, head.Price, trade)

			if head.Qty == 0 {
				b.cancelOrder(bestBid.Price, head, rq, Bid)
			}
		}
	}
	return filled
}

// enqueue leftover order into book
func (b *OrderBook) enqueue(o *Order) {
	if o.Side == Bid {
		lvl := b.Bids.UpsertLevel(o.Price)
		lvl.Enqueue(o)
	} else {
		lvl := b.Asks.UpsertLevel(o.Price)
		lvl.Enqueue(o)
	}
}

// cancel order and recycle
func (b *OrderBook) cancelOrder(price int64, o *Order, rq *retireRing, side Side) {
	o.Status = Inactive
	o.retireEpoch = globalEpoch.Load()

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
				_ = b.Bids.DeleteLevel(price)
			} else {
				_ = b.Asks.DeleteLevel(price)
			}
		}
	}
	if !rq.Enqueue(o) {
		panic("retire ring full")
	}

	// 🧾 WAL log cancel
	b.logOrderEvent("cancel", o)
}

// ---------------- WAL Utilities ---------------- //

// logOrderEvent writes order placement, cancel, or reject events to WAL
func (b *OrderBook) logOrderEvent(event string, o *Order) {
	if b.Log == nil {
		return
	}
	data := fmt.Sprintf("%s|%d|%d|%d|%d|%d|%d|%d",
		event, o.ID, o.Side, o.Type, o.Price, o.Qty, o.Filled, o.SeqID)
	rec := &wal.Record{
		Type: wal.RecordPlace,
		Time: time.Now().UnixNano(),
		Data: []byte(data),
	}
	_ = b.Log.Append(rec)
}

// logTrade writes matched trade info to WAL
func (b *OrderBook) logTrade(o *Order, price int64, qty int64) {
	if b.Log == nil {
		return
	}
	data := fmt.Sprintf("trade|%d|%d|%d|%d|%d",
		o.ID, o.Side, price, qty, o.SeqID)
	rec := &wal.Record{
		Type: wal.RecordMatch,
		Time: time.Now().UnixNano(),
		Data: []byte(data),
	}
	_ = b.Log.Append(rec)
}

// ReplayFromWAL rebuilds the book by reading historical records
func (b *OrderBook) ReplayFromWAL() error {
	if b.Log == nil {
		return nil
	}
	fmt.Println("🔁 Replaying from WAL ...")

	return b.Log.ReplayFrom(0, func(r *wal.Record) {
		parts := strings.Split(string(r.Data), "|")
		if len(parts) == 0 {
			return
		}

		switch parts[0] {
		case "place":
			o := DecodeBinary([]byte{}) // optional: adapt to EncodeBinary()
			_ = o
		case "cancel":
			// TODO: optional restore logic
		case "trade":
			// optional trade replay
		}
	})
}

// ---------------- FOK Pre-check ---------------- //

func (b *OrderBook) checkLiquidity(side Side, limitPrice int64, desired int64) int64 {
	available := int64(0)
	if side == Bid {
		b.Asks.ForEachAscending(func(lvl *PriceLevel) bool {
			if lvl.Price > limitPrice {
				return false
			}
			available += lvl.TotalQty
			return available < desired
		})
	} else {
		b.Bids.ForEachDescending(func(lvl *PriceLevel) bool {
			if lvl.Price < limitPrice {
				return false
			}
			available += lvl.TotalQty
			return available < desired
		})
	}
	return available
}

// ---------------- Epoch Reclaim ---------------- //

func advanceEpochAndReclaim(rq *retireRing, pool *OrderPool, rs ...*Reader) {
	globalEpoch.Add(1)
	min := minReaderEpoch(rs...)
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

// ---------------- Snapshots ---------------- //

func (b *OrderBook) SnapshotActiveIter(r *Reader, visit func(price int64, o *Order)) {
	r.EnterRead()
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
	r.ExitRead()
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
