package rbq

import (
	"fmt"
	"loki/order_book"
	"sync/atomic"
)

// SPSC ring for retired orders (matcher → reclaimer).
type retireRing struct {
	// align head/tail to separate cache lines
	head  uint64
	_pad1 [56]byte
	tail  uint64
	_pad2 [56]byte

	buf  []*order_book.Order
	mask uint64
}

// newRetireRing allocates a fixed-size circular buffer (power-of-2 length).
func newRetireRing(pow2 uint64) *retireRing {
	return &retireRing{buf: make([]*order_book.Order, pow2), mask: pow2 - 1}
}

// Enqueue pushes an order into the ring.
// Returns false if the buffer is full.
func (q *retireRing) Enqueue(o *order_book.Order) bool {
	h := q.head
	t := atomic.LoadUint64(&q.tail)
	if h-t == uint64(len(q.buf)) {
		return false // full
	}
	q.buf[h&q.mask] = o
	q.head = h + 1
	return true
}

// Dequeue pops the next order from the ring.
// Returns nil if the buffer is empty.
func (q *retireRing) Dequeue() *order_book.Order {
	t := q.tail
	h := atomic.LoadUint64(&q.head)
	if t == h {
		return nil
	}
	o := q.buf[t&q.mask]
	q.buf[t&q.mask] = nil
	q.tail = t + 1
	return o
}

// ---------------- Optional Diagnostics ---------------- //

// Len returns the number of orders currently stored.
func (q *retireRing) Len() int {
	h := atomic.LoadUint64(&q.head)
	t := atomic.LoadUint64(&q.tail)
	return int(h - t)
}

// Cap returns the total capacity of the ring.
func (q *retireRing) Cap() int {
	return len(q.buf)
}

// IsFull reports whether the ring is full.
func (q *retireRing) IsFull() bool {
	h := atomic.LoadUint64(&q.head)
	t := atomic.LoadUint64(&q.tail)
	return h-t == uint64(len(q.buf))
}

// IsEmpty reports whether the ring is empty.
func (q *retireRing) IsEmpty() bool {
	return atomic.LoadUint64(&q.head) == atomic.LoadUint64(&q.tail)
}

// Dump prints a short summary for debugging / monitoring.
func (q *retireRing) Dump() {
	fmt.Printf("retireRing{len=%d, cap=%d, head=%d, tail=%d}\n",
		q.Len(), q.Cap(), atomic.LoadUint64(&q.head), atomic.LoadUint64(&q.tail))
}
