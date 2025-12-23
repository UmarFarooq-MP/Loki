package orderbook

type Side int
type OrderType int
type Status int

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
	Active Status = iota
	Inactive
)

// Order is a pure domain entity.
type Order struct {
	ID     uint64
	Price  int64
	Qty    int64
	Filled int64
	SeqID  uint64

	Side   Side
	Type   OrderType
	Status Status

	next *Order
	prev *Order
}

func (o *Order) Remaining() int64 {
	return o.Qty - o.Filled
}

// Read-only traversal helpers
func (o *Order) Next() *Order {
	return o.next
}
