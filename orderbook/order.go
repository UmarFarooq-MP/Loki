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

// Implement memory.Order interface.
func (o *Order) Reset()                  { *o = Order{} }
func (o *Order) RetireEpoch() uint64     { return o.retireEpoch }
func (o *Order) SetRetireEpoch(v uint64) { o.retireEpoch = v }
