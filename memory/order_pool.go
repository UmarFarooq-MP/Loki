package memory

import "sync"

// GenericPool is a reusable, type-agnostic object pool.
// It avoids cyclic dependencies by using 'any' instead of importing orderbook directly.
type GenericPool[T any] struct {
	pool *sync.Pool
}

// NewGenericPool creates a new generic pool with a given constructor.
func NewGenericPool[T any](constructor func() *T) *GenericPool[T] {
	return &GenericPool[T]{
		pool: &sync.Pool{
			New: func() any { return constructor() },
		},
	}
}

// Get retrieves an object from the pool.
func (p *GenericPool[T]) Get() *T {
	return p.pool.Get().(*T)
}

// Put returns an object to the pool.
func (p *GenericPool[T]) Put(obj *T) {
	p.pool.Put(obj)
}
