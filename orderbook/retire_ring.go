package orderbook

import (
	"fmt"
	"sync/atomic"
)

type RetireRing struct {
	head  uint64
	_pad1 [56]byte
	tail  uint64
	_pad2 [56]byte
	buf   []*Order
	mask  uint64
}

func NewRetireRing(size uint64) *RetireRing {
	return &RetireRing{buf: make([]*Order, size), mask: size - 1}
}

func (r *RetireRing) Enqueue(o *Order) bool {
	h := r.head
	t := atomic.LoadUint64(&r.tail)
	if h-t == uint64(len(r.buf)) {
		return false
	}
	r.buf[h&r.mask] = o
	r.head = h + 1
	return true
}

func (r *RetireRing) Dequeue() *Order {
	t := r.tail
	h := atomic.LoadUint64(&r.head)
	if t == h {
		return nil
	}
	o := r.buf[t&r.mask]
	r.buf[t&r.mask] = nil
	r.tail = t + 1
	return o
}

func (r *RetireRing) Dump() {
	fmt.Printf("RetireRing{len=%d, cap=%d, head=%d, tail=%d}\n",
		r.Len(), r.Cap(), atomic.LoadUint64(&r.head), atomic.LoadUint64(&r.tail))
}

func (r *RetireRing) Len() int {
	h := atomic.LoadUint64(&r.head)
	t := atomic.LoadUint64(&r.tail)
	return int(h - t)
}

func (r *RetireRing) Cap() int {
	return len(r.buf)
}
