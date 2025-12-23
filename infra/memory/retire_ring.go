package memory

import "sync/atomic"

// RetireRing is a lock-free SPSC ring buffer for retired objects.
type RetireRing struct {
	head  uint64
	_pad1 [56]byte
	tail  uint64
	_pad2 [56]byte
	buf   []any
	mask  uint64
}

func NewRetireRing(size uint64) *RetireRing {
	if size&(size-1) != 0 {
		panic("RetireRing size must be power of two")
	}
	return &RetireRing{
		buf:  make([]any, size),
		mask: size - 1,
	}
}

func (r *RetireRing) Enqueue(v any) bool {
	h := r.head
	t := atomic.LoadUint64(&r.tail)
	if h-t == uint64(len(r.buf)) {
		return false
	}
	r.buf[h&r.mask] = v
	r.head = h + 1
	return true
}

func (r *RetireRing) Dequeue() any {
	t := r.tail
	h := atomic.LoadUint64(&r.head)
	if t == h {
		return nil
	}
	v := r.buf[t&r.mask]
	r.buf[t&r.mask] = nil
	r.tail = t + 1
	return v
}
