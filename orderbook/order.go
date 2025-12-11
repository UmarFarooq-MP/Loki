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
	FOK
	IOC
	PostOnly
)

const (
	Active Status = iota
	Inactive
)

// Order represents a single limit/market order.
type Order struct {
	ID          uint64
	Price       int64
	Qty         int64
	Filled      int64
	SeqID       uint64
	Side        Side
	Type        OrderType
	Status      Status
	retireEpoch uint64
	next        *Order
	prev        *Order
}
