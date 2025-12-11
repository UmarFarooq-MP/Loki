package orderbook

type PriceLevel struct {
	Price      int64
	head       *Order
	tail       *Order
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
	p.TotalQty += o.Qty
	p.OrderCount++
}

func (p *PriceLevel) unlinkAlreadyInactive(o *Order) {
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		p.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		p.tail = o.prev
	}
	p.TotalQty -= o.Qty
	p.OrderCount--
	if p.TotalQty < 0 {
		p.TotalQty = 0
	}
}
