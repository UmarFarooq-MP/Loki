package service

import (
	_ "sync/atomic"

	"loki/memory"
	"loki/orderbook"
	"loki/snapshotter"
)

// OrderService coordinates the core order book, memory pools, and snapshot readers.
type OrderService struct {
	Book   *orderbook.OrderBook
	Pool   *memory.OrderPool
	Retire *memory.RetireRing
	Reader *snapshotter.Reader
}

// NewOrderService initializes the in-memory matching engine service.
func NewOrderService() *OrderService {
	return &OrderService{
		Book:   orderbook.NewOrderBook(),
		Pool:   memory.NewOrderPool(1 << 20),  // 1M capacity
		Retire: memory.NewRetireRing(1 << 16), // 64K retire ring
		Reader: snapshotter.NewReader(),
	}
}

// PlaceOrder inserts a new order into the book and updates sequence.
func (s *OrderService) PlaceOrder(
	side orderbook.Side,
	otype orderbook.OrderType,
	price int64,
	qty uint64,
	userID uint64,
) {
	seq := s.Book.LastSeq.Add(1) // use atomic.Uint64 method
	s.Book.PlaceOrder(side, otype, price, uint64(qty), int64(userID), seq, s.Pool, s.Retire)
}

// Snapshot returns a list of active orders for clients.
func (s *OrderService) Snapshot() []orderbook.Order {
	var result []orderbook.Order

	// Ensure your OrderBook actually defines SnapshotActiveIter
	// Signature: SnapshotActiveIter(reader *snapshotter.Reader, visit func(price int64, o *Order))
	s.Book.SnapshotActiveIter(s.Reader, func(price int64, o *orderbook.Order) {
		result = append(result, *o)
	})
	return result
}

// LastSeq returns the last used sequence number.
func (s *OrderService) LastSeq() uint64 {
	return s.Book.LastSeq.Load()
}
