package main

import (
	"fmt"
	"path/filepath"
	"time"
)

// WALIntegration handles lifecycle of WAL inside your system (init, replay, close)
type WALIntegration struct {
	wal WALInterface
	cfg WALConfig
}

// NewWALIntegration creates and configures a WAL instance
func NewWALIntegration(cfg WALConfig) (*WALIntegration, error) {
	w, err := NewWAL(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	i := &WALIntegration{
		wal: w,
		cfg: cfg,
	}

	// Start background auto-flush if configured
	if cfg.FlushInterval > 0 {
		go i.startAutoFlush()
	}

	return i, nil
}

// startAutoFlush runs in background and periodically flushes+syncs data
func (i *WALIntegration) startAutoFlush() {
	ticker := time.NewTicker(i.cfg.FlushInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := i.wal.Sync(); err != nil {
			fmt.Printf("⚠️  WAL auto-sync failed: %v\n", err)
		}
	}
}

// AppendRecord is a simple wrapper to append a record to WAL
func (i *WALIntegration) AppendRecord(rec *Record) error {
	return i.wal.Append(rec)
}

// ReplayFromSnapshot replays records newer than a given snapshot sequence
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
			return fmt.Errorf("open reader: %w", err)
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

	fmt.Printf("🔁 Replay complete from snapshot seq=%d\n", snapshotSeq)
	return nil
}

// Close finalizes WAL safely and prints stats if available
func (i *WALIntegration) Close() error {
	if err := i.wal.Sync(); err != nil {
		return err
	}
	if err := i.wal.Close(); err != nil {
		return err
	}

	//// If WAL implements PrintStats, call it
	//if w, ok := i.wal.(*WAL); ok {
	//	//w.PrintStats()
	//}

	fmt.Println("✅ WAL integration closed successfully")
	return nil
}
