package orderbook

// PriceLevel is a FIFO queue at a single price.
type PriceLevel struct {
	Price int64

	head *Order
	tail *Order

	TotalQty   int64
	OrderCount int
}

func (p *PriceLevel) Enqueue(o *Order) {
	if p.head == nil {
		p.head = o
		p.tail = o
	} else {
		p.tail.next = o
		o.prev = p.tail
		p.tail = o
	}
	p.TotalQty += o.Remaining()
	p.OrderCount++
}

func (p *PriceLevel) PopHead() *Order {
	o := p.head
	if o == nil {
		return nil
	}

	p.head = o.next
	if p.head != nil {
		p.head.prev = nil
	} else {
		p.tail = nil
	}

	o.next = nil
	o.prev = nil

	p.TotalQty -= o.Remaining()
	p.OrderCount--

	return o
}

func (p *PriceLevel) Empty() bool {
	return p.head == nil
}

// Read-only helper
func (p *PriceLevel) Head() *Order {
	return p.head
}
