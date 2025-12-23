package memory

import "sync"

// Pool is a typed object pool.
// It is type-safe for normal use, but can also participate
// in epoch-based reclamation via PutAny.
type Pool[T any] struct {
	p *sync.Pool
}

func NewPool[T any](ctor func() *T) *Pool[T] {
	return &Pool[T]{
		p: &sync.Pool{
			New: func() any { return ctor() },
		},
	}
}

func (p *Pool[T]) Get() *T {
	return p.p.Get().(*T)
}

func (p *Pool[T]) Put(v *T) {
	p.p.Put(v)
}

// PutAny allows Pool[T] to satisfy ReclaimablePool.
// This is an explicit, safe adapter between typed and erased worlds.
func (p *Pool[T]) PutAny(v any) {
	obj, ok := v.(*T)
	if !ok {
		panic("memory.Pool: PutAny received wrong type")
	}
	p.Put(obj)
}
