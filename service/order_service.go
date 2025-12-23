package service

import (
	"fmt"
	"time"

	"loki/domain/orderbook"
	"loki/infra/memory"
	"loki/infra/wal"
	"loki/snapshot"
)

/*
OrderService is the ONLY write entry point into the system.

All coordination between:
- domain (orderbook)
- infra (memory, wal)
- snapshot
happens here.
*/

type OrderService struct {
	book   *orderbook.OrderBook
	pool   *memory.Pool[orderbook.Order]
	ring   *memory.RetireRing
	reader *snapshot.Reader
	wal    *wal.WAL
}

// NewOrderService wires all dependencies.
// No globals. No magic.
func NewOrderService(
	book *orderbook.OrderBook,
	pool *memory.Pool[orderbook.Order],
	ring *memory.RetireRing,
	reader *snapshot.Reader,
	w *wal.WAL,
) *OrderService {
	return &OrderService{
		book:   book,
		pool:   pool,
		ring:   ring,
		reader: reader,
		wal:    w,
	}
}

//
// ──────────────────────────────────────────────────────────
// Commands
// ──────────────────────────────────────────────────────────
//

// PlaceOrder submits a new order into the engine.
// It returns the assigned sequence number.
func (s *OrderService) PlaceOrder(
	side orderbook.Side,
	otype orderbook.OrderType,
	price int64,
	qty int64,
	userID uint64,
) uint64 {
	seq := uint64(time.Now().UnixNano())

	// 1️⃣ Allocate domain object
	o := s.pool.Get()
	*o = orderbook.Order{
		ID:     userID,
		Side:   side,
		Type:   otype,
		Price:  price,
		Qty:    qty,
		Filled: 0,
		SeqID:  seq,
		Status: orderbook.Active,
	}

	// 2️⃣ Write WAL intent (best-effort for now)
	_ = s.wal.Append(
		wal.NewRecord(
			wal.RecordPlace,
			[]byte(fmt.Sprintf(
				"%d|%d|%d|%d|%d",
				userID,
				side,
				otype,
				price,
				qty,
			)),
		),
	)

	// 3️⃣ Execute deterministic domain logic
	s.book.Place(o)

	// 4️⃣ Retire immediately if fully filled
	if o.Remaining() == 0 {
		s.retire(o)
	}

	return seq
}

//
// ──────────────────────────────────────────────────────────
// Queries
// ──────────────────────────────────────────────────────────
//

// Snapshot returns a consistent view of all ACTIVE orders.
// Caller must treat returned orders as read-only.
func (s *OrderService) Snapshot() []*orderbook.Order {
	s.reader.Begin()
	defer s.reader.End()

	out := make([]*orderbook.Order, 0, 1024)

	// Walk bids (best → worst)
	s.book.BidsWalk(func(lvl *orderbook.PriceLevel) {
		for o := lvl.Head(); o != nil; o = o.Next() {
			if o.Status == orderbook.Active {
				out = append(out, o)
			}
		}
	})

	// Walk asks (best → worst)
	s.book.AsksWalk(func(lvl *orderbook.PriceLevel) {
		for o := lvl.Head(); o != nil; o = o.Next() {
			if o.Status == orderbook.Active {
				out = append(out, o)
			}
		}
	})

	return out
}

//
// ──────────────────────────────────────────────────────────
// Reclamation
// ──────────────────────────────────────────────────────────
//

// AdvanceEpoch performs safe reclamation.
// Intended to be called periodically by a background job.
func (s *OrderService) AdvanceEpoch() {
	memory.AdvanceEpochAndReclaim(
		s.ring,
		s.pool, // satisfies ReclaimablePool via PutAny
		s.reader.Epoch(),
	)
}

func (s *OrderService) retire(o *orderbook.Order) {
	o.Status = orderbook.Inactive
	_ = s.ring.Enqueue(o)
}
