package orderbook

type PriceLevel struct {
	Price      int64
	TotalQty   int64
	head, tail *Order
}

func (pl *PriceLevel) Enqueue(o *Order) {
	if pl.tail == nil {
		pl.head = o
		pl.tail = o
	} else {
		pl.tail.next = o
		o.prev = pl.tail
		pl.tail = o
	}
	pl.TotalQty += o.Qty
}

func (pl *PriceLevel) unlinkAlreadyInactive(o *Order) {
	if o == nil {
		return
	}
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		pl.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		pl.tail = o.prev
	}
	pl.TotalQty -= o.Qty
	o.prev = nil
	o.next = nil
}
