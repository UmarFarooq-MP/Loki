package main

import "fmt"

// PriceLevel holds all active orders at a given price.
type PriceLevel struct {
	Price    int64
	head     *Order
	tail     *Order
	TotalQty int64
}

// Enqueue appends a new order at the end of this price level (FIFO order book behavior).
func (lvl *PriceLevel) Enqueue(o *Order) {
	if lvl.tail != nil {
		lvl.tail.next = o
		o.prev = lvl.tail
	} else {
		lvl.head = o
	}
	lvl.tail = o
	lvl.TotalQty += o.Qty
}

// Dequeue removes and returns the oldest order (head) from this price level.
func (lvl *PriceLevel) Dequeue() *Order {
	if lvl.head == nil {
		return nil
	}
	o := lvl.head
	lvl.head = o.next
	if lvl.head != nil {
		lvl.head.prev = nil
	} else {
		lvl.tail = nil
	}
	o.next, o.prev = nil, nil
	lvl.TotalQty -= o.Qty
	return o
}

// unlinkAlreadyInactive safely removes an order from the linked list.
func (lvl *PriceLevel) unlinkAlreadyInactive(o *Order) {
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		lvl.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		lvl.tail = o.prev
	}
	lvl.TotalQty -= o.Qty
	o.next, o.prev = nil, nil
}

// Snapshot returns a slice of active orders (useful for debugging or replay).
func (lvl *PriceLevel) Snapshot() []*Order {
	var list []*Order
	for n := lvl.head; n != nil; n = n.next {
		list = append(list, n)
	}
	return list
}

// String formats this price level for debugging/logging.
func (lvl *PriceLevel) String() string {
	count := 0
	for n := lvl.head; n != nil; n = n.next {
		count++
	}
	return fmt.Sprintf("PriceLevel{Price=%d, Orders=%d, TotalQty=%d}", lvl.Price, count, lvl.TotalQty)
}
