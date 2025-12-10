package orderbook

import "sync/atomic"

type Side uint8
type OrderType uint8
type OrderStatus uint8

const (
	Bid Side = iota
	Ask
)

const (
	Limit OrderType = iota
	Market
	IOC
	FOK
	PostOnly
)

const (
	Active OrderStatus = iota
	Inactive
)

type Order struct {
	ID          uint64
	Side        Side
	Type        OrderType
	Price       int64
	Qty         int64
	Filled      int64
	SeqID       uint64
	Status      OrderStatus
	retireEpoch uint64
	prev, next  *Order
	CreatedAt   int64
	UpdatedAt   int64
}

type OrderPool struct {
	buf   []*Order
	index atomic.Uint64
	size  uint64
}

func NewOrderPool(size int) *OrderPool {
	return &OrderPool{buf: make([]*Order, size), size: uint64(size)}
}

func (p *OrderPool) Get() *Order {
	i := p.index.Load()
	if i >= p.size {
		return nil
	}
	o := p.buf[i]
	if o == nil {
		o = &Order{}
		p.buf[i] = o
	}
	p.index.Add(1)
	return o
}

func (p *OrderPool) Put(o *Order) {
	if o == nil {
		return
	}
	*o = Order{}
}
