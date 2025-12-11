package service

import (
	"fmt"
	"time"

	"loki/memory"
	"loki/orderbook"
	"loki/rcu"
	"loki/wal/entry"
	"loki/wal/exit"
)

type OrderService struct {
	Book     *orderbook.OrderBook
	Pool     *memory.OrderPool
	Ring     *memory.RetireRing
	Reader   *rcu.Reader
	EntryWAL *entry.WAL
	ExitWAL  *exit.ExitWAL
}

// NewOrderService constructs the full order service stack.
func NewOrderService(
	book *orderbook.OrderBook,
	pool *memory.OrderPool,
	ring *memory.RetireRing,
	reader *rcu.Reader,
	entryWAL *entry.WAL,
	exitWAL *exit.ExitWAL,
) *OrderService {
	return &OrderService{
		Book:     book,
		Pool:     pool,
		Ring:     ring,
		Reader:   reader,
		EntryWAL: entryWAL,
		ExitWAL:  exitWAL,
	}
}

// PlaceOrder handles new incoming orders.
func (s *OrderService) PlaceOrder(side orderbook.Side, otype orderbook.OrderType, price int64, qty int64, userID uint64) {
	seq := uint64(time.Now().UnixNano())

	// Log into entry WAL
	data := fmt.Sprintf("place|%d|%d|%d|%d|%d", userID, side, otype, price, qty)
	rec := entry.NewRecord(entry.RecordPlace, []byte(data))
	_ = s.EntryWAL.Append(rec)

	// Process order
	o := s.Book.PlaceOrder(side, otype, price, qty, userID, seq, s.Pool, s.Ring)

	// Store state in exit WAL
	state := "ready"
	if o.Status == orderbook.Inactive {
		state = "inactive"
	}
	s.ExitWAL.Update(o.ID, state)
}

// Snapshot returns the current view of the order book.
func (s *OrderService) Snapshot() []*orderbook.Order {
	var orders []*orderbook.Order
	s.Book.SnapshotActiveIter(s.Reader, func(price int64, o *orderbook.Order) {
		orders = append(orders, o)
	})
	return orders
}
