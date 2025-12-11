package memory

import (
	"fmt"
	"sync/atomic"
)

// RetireRing is a lock-free SPSC ring used for retired objects.
type RetireRing struct {
	head  uint64
	_pad1 [56]byte
	tail  uint64
	_pad2 [56]byte
	buf   []any
	mask  uint64
}

// NewRetireRing allocates a ring with power-of-two size.
func NewRetireRing(pow2 uint64) *RetireRing {
	return &RetireRing{buf: make([]any, pow2), mask: pow2 - 1}
}

// Enqueue adds an element; returns false if full.
func (r *RetireRing) Enqueue(item any) bool {
	h := r.head
	t := atomic.LoadUint64(&r.tail)
	if h-t == uint64(len(r.buf)) {
		return false
	}
	r.buf[h&r.mask] = item
	r.head = h + 1
	return true
}

// Dequeue removes one element; returns nil if empty.
func (r *RetireRing) Dequeue() any {
	t := r.tail
	h := atomic.LoadUint64(&r.head)
	if t == h {
		return nil
	}
	item := r.buf[t&r.mask]
	r.buf[t&r.mask] = nil
	r.tail = t + 1
	return item
}

// Diagnostic helpers
func (r *RetireRing) Len() int { return int(atomic.LoadUint64(&r.head) - atomic.LoadUint64(&r.tail)) }
func (r *RetireRing) Cap() int { return len(r.buf) }
func (r *RetireRing) IsFull() bool {
	h := atomic.LoadUint64(&r.head)
	t := atomic.LoadUint64(&r.tail)
	return h-t == uint64(len(r.buf))
}
func (r *RetireRing) IsEmpty() bool { return atomic.LoadUint64(&r.head) == atomic.LoadUint64(&r.tail) }

func (r *RetireRing) Dump() {
	fmt.Printf("RetireRing{len=%d, cap=%d, head=%d, tail=%d}\n",
		r.Len(), r.Cap(), atomic.LoadUint64(&r.head), atomic.LoadUint64(&r.tail))
}
