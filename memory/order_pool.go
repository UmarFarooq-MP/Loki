package memory

import (
	"sync/atomic"
)

// OrderPool manages reusable order-like objects to avoid GC churn.
type OrderPool struct {
	slots []any
	head  uint64
}

// NewOrderPool creates a reusable pool with a given capacity.
func NewOrderPool(cap int) *OrderPool {
	return &OrderPool{slots: make([]any, cap)}
}

// Get reserves a slot; returns nil if exhausted.
func (p *OrderPool) Get() any {
	h := atomic.LoadUint64(&p.head)
	if int(h) >= len(p.slots) {
		return nil
	}
	item := make(map[string]any)
	p.slots[h] = item
	atomic.AddUint64(&p.head, 1)
	return item
}

// Put releases an object back into the pool.
func (p *OrderPool) Put(item any) {
	for i := range p.slots {
		if p.slots[i] == nil {
			p.slots[i] = item
			return
		}
	}
}

// Size returns number of used slots.
func (p *OrderPool) Size() int {
	return int(atomic.LoadUint64(&p.head))
}

// Cap returns total capacity.
func (p *OrderPool) Cap() int {
	return len(p.slots)
}
