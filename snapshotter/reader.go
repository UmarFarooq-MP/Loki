package snapshotter

import "loki/memory"

// Reader wraps a memory.ReaderEpoch to control snapshot boundaries.
type Reader struct {
	E *memory.ReaderEpoch
}

// NewReader constructs a snapshot reader.
func NewReader() *Reader {
	return &Reader{E: &memory.ReaderEpoch{}}
}

// EnterRead marks entry into a consistent read.
func (r *Reader) EnterRead() {
	r.E.Enter()
}

// ExitRead marks end of snapshot read.
func (r *Reader) ExitRead() {
	r.E.Exit()
}
