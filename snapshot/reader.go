package snapshot

import "loki/infra/memory"

/*
Snapshot Reader

This is a thin adapter over memory.ReaderEpoch.
Its only responsibility is to clearly mark:
- when a snapshot begins
- when it ends

Everything else (epoching, reclamation) is handled elsewhere.
*/

type Reader struct {
	epoch *memory.ReaderEpoch
}

func NewReader() *Reader {
	return &Reader{
		epoch: &memory.ReaderEpoch{},
	}
}

// Begin marks the start of a consistent snapshot.
func (r *Reader) Begin() {
	r.epoch.Enter()
}

// End marks the end of a snapshot.
func (r *Reader) End() {
	r.epoch.Exit()
}

// Epoch exposes the underlying epoch for reclaimers.
func (r *Reader) Epoch() *memory.ReaderEpoch {
	return r.epoch
}
