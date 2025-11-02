package wal

import (
	"fmt"
	"path/filepath"
	"time"
)

type WALIntegration struct {
	wal WALInterface
	cfg WALConfig
}

type WALInterface interface {
	Append(*Record) error
	Sync() error
	Close() error
}

func NewWALIntegration(cfg WALConfig) (*WALIntegration, error) {
	w, err := NewCoreWAL(cfg)
	if err != nil {
		return nil, fmt.Errorf("init wal: %w", err)
	}
	i := &WALIntegration{wal: w, cfg: cfg}

	if cfg.FlushInterval > 0 {
		go i.autoFlush()
	}
	return i, nil
}

func (i *WALIntegration) autoFlush() {
	ticker := time.NewTicker(i.cfg.FlushInterval)
	defer ticker.Stop()
	for range ticker.C {
		_ = i.wal.Sync()
	}
}

func (i *WALIntegration) AppendRecord(rec *Record) error {
	return i.wal.Append(rec)
}

func (i *WALIntegration) ReplayFromSnapshot(snapshotSeq uint64, apply func(*Record)) error {
	index, err := LoadAllIndex(i.cfg.Dir)
	if err != nil {
		return fmt.Errorf("load index: %w", err)
	}

	for _, seg := range index {
		if seg.LastSeq <= snapshotSeq {
			continue
		}
		path := filepath.Join(i.cfg.Dir, seg.File)
		r, err := OpenReader(path, i.cfg.Serializer)
		if err != nil {
			return err
		}
		for r.Next() {
			rec := r.Record()
			if rec.Seq <= snapshotSeq {
				continue
			}
			apply(rec)
		}
		r.Close()
	}
	return nil
}

func (i *WALIntegration) Close() error {
	_ = i.wal.Sync()
	_ = i.wal.Close()
	fmt.Println("✅ WAL integration closed")
	return nil
}
