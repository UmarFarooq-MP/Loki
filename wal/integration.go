package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	sort.Slice(index, func(a, b int) bool {
		return index[a].FirstSeq < index[b].FirstSeq
	})

	for _, seg := range index {
		if seg.LastSeq <= snapshotSeq {
			continue
		}
		path := filepath.Join(i.cfg.Dir, seg.File)
		if err := i.replayFile(path, snapshotSeq, apply); err != nil {
			return err
		}
	}

	current := filepath.Join(i.cfg.Dir, "current.wal")
	if _, err := os.Stat(current); err == nil {
		if err := i.replayFile(current, snapshotSeq, apply); err != nil {
			return err
		}
	}
	return nil
}

func (i *WALIntegration) Close() error {
	_ = i.wal.Sync()
	_ = i.wal.Close()
	fmt.Println("WAL integration closed")
	return nil
}

func (i *WALIntegration) replayFile(path string, snapshotSeq uint64, apply func(*Record)) error {
	r, err := OpenReader(path, i.cfg.Serializer)
	if err != nil {
		return err
	}
	defer r.Close()
	for r.Next() {
		rec := r.Record()
		if rec.Seq <= snapshotSeq {
			continue
		}
		apply(rec)
	}
	if err := r.Err(); err != nil {
		return err
	}
	return nil
}
