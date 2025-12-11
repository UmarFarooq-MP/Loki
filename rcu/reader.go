package rcu

import "sync/atomic"

var globalEpoch atomic.Uint64

type Reader struct {
	epoch uint64
}

func (r *Reader) EnterRead() {
	r.epoch = globalEpoch.Load()
}

func (r *Reader) ExitRead() {}

func AdvanceEpoch() {
	globalEpoch.Add(1)
}

func MinReaderEpoch(readers ...*Reader) uint64 {
	min := ^uint64(0)
	for _, r := range readers {
		if r.epoch < min {
			min = r.epoch
		}
	}
	return min
}

func GlobalEpoch() uint64 {
	return globalEpoch.Load()
}
