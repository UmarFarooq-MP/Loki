package wal

import (
	"fmt"
	"time"
)

// Config defines configuration for a WAL instance.
type Config struct {
	Dir             string
	SegmentSize     uint64
	SegmentDuration time.Duration
	Serializer      Serializer
	FlushInterval   time.Duration
}

// WAL is the public interface users interact with.
type WAL interface {
	Append(*Record) error
	Sync() error
	Close() error
	ReplayFrom(snapshotSeq uint64, apply func(*Record)) error
}

// New creates a new WAL instance from Config.
func New(cfg Config) (WAL, error) {
	if cfg.Dir == "" {
		cfg.Dir = "./wal_data"
	}
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = 2 * 1024 * 1024
	}
	if cfg.SegmentDuration == 0 {
		cfg.SegmentDuration = 5 * time.Minute
	}
	if cfg.Serializer == nil {
		cfg.Serializer = BinarySerializer{}
	}

	core, err := NewWALIntegration(WALConfig{
		Dir:             cfg.Dir,
		SegmentSize:     cfg.SegmentSize,
		SegmentDuration: cfg.SegmentDuration,
		Serializer:      cfg.Serializer,
		FlushInterval:   cfg.FlushInterval,
	})
	if err != nil {
		return nil, fmt.Errorf("create wal: %w", err)
	}

	return &walWrapper{core: core}, nil
}

type walWrapper struct {
	core *WALIntegration
}

func (w *walWrapper) Append(rec *Record) error {
	return w.core.AppendRecord(rec)
}

func (w *walWrapper) Sync() error {
	return w.core.wal.Sync()
}

func (w *walWrapper) Close() error {
	return w.core.Close()
}

func (w *walWrapper) ReplayFrom(seq uint64, apply func(*Record)) error {
	return w.core.ReplayFromSnapshot(seq, apply)
}
