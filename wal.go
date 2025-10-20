package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type RecordType byte

const (
	RecordPlace    RecordType = 1
	RecordCancel   RecordType = 2
	RecordMatch    RecordType = 3
	RecordSnapshot RecordType = 4
)

type Record struct {
	Type RecordType
	Seq  uint64
	Time int64
	Data []byte
}

type WalIndexEntry struct {
	File      string `json:"file"`
	FirstSeq  uint64 `json:"first_seq"`
	LastSeq   uint64 `json:"last_seq"`
	Timestamp string `json:"timestamp"`
}

type WALInterface interface {
	Append(rec *Record) error
	Sync() error
	Close() error
}

type WALConfig struct {
	Dir             string        // e.g. "./wal_data"
	SegmentSize     uint64        // max size in bytes before rotation
	SegmentDuration time.Duration // max time before rotation
	Serializer      Serializer    // pluggable serializer
	FlushInterval   time.Duration // optional: background flush interval
}

type WAL struct {
	cfg             WALConfig
	file            *os.File
	writer          *bufio.Writer
	seq             uint64
	segmentID       int
	segmentStartSeq uint64
	bytesWritten    uint64
	lastRotationAt  time.Time
}

func NewWAL(cfg WALConfig) (*WAL, error) {
	// Apply defaults if missing
	if cfg.Dir == "" {
		cfg.Dir = "./wal_data"
	}
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = 2 * 1024 * 1024 // 2 MB
	}
	if cfg.SegmentDuration == 0 {
		cfg.SegmentDuration = 5 * time.Minute
	}
	if cfg.Serializer == nil {
		cfg.Serializer = BinarySerializer{}
	}

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	last, _ := LoadLastIndex(cfg.Dir)
	var segID int
	var seq uint64

	if last != nil {
		id, _ := strconv.Atoi(strings.TrimSuffix(filepath.Base(last.File), ".wal"))
		segID = id
		seq = last.LastSeq
	} else {
		fmt.Println("Starting new WAL (no index found)")
	}

	path := filepath.Join(cfg.Dir, "current.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		cfg:             cfg,
		file:            f,
		writer:          bufio.NewWriterSize(f, 1<<20),
		segmentID:       segID,
		segmentStartSeq: seq + 1,
		seq:             seq,
		lastRotationAt:  time.Now(),
	}

	// finalize leftover current.wal if previous run crashed mid-write
	if info, err := os.Stat(path); err == nil && info.Size() > 0 && segID > 0 {
		_ = w.rotate()
	}

	return w, nil
}

func (w *WAL) Append(rec *Record) error {
	data, err := w.cfg.Serializer.Encode(rec)
	if err != nil {
		return err
	}

	if w.shouldRotate(len(data)) {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	rec.Seq = w.seq + 1
	w.seq++

	n, err := w.writer.Write(data)
	w.bytesWritten += uint64(n)
	return err
}

func (w *WAL) shouldRotate(nextSize int) bool {
	if w.bytesWritten+uint64(nextSize) >= w.cfg.SegmentSize {
		return true
	}
	if time.Since(w.lastRotationAt) >= w.cfg.SegmentDuration {
		return true
	}
	return false
}

func (w *WAL) rotate() error {
	_ = w.writer.Flush()
	_ = w.file.Sync()
	_ = w.file.Close()

	newID := w.segmentID + 1
	newFile := fmt.Sprintf("%06d.wal", newID)
	oldPath := filepath.Join(w.cfg.Dir, "current.wal")
	newPath := filepath.Join(w.cfg.Dir, newFile)

	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	entry := WalIndexEntry{
		File:      newFile,
		FirstSeq:  w.segmentStartSeq,
		LastSeq:   w.seq,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	_ = AppendIndexEntry(w.cfg.Dir, entry)

	f, err := os.OpenFile(oldPath, os.O_CREATE|os.O_RDWR|os.O_APPEND|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w.file = f
	w.writer = bufio.NewWriterSize(f, 1<<20)
	w.segmentID = newID
	w.segmentStartSeq = w.seq + 1
	w.bytesWritten = 0
	w.lastRotationAt = time.Now()

	fmt.Printf("Rotated WAL → %s (seq %d–%d)\n", newFile, entry.FirstSeq, entry.LastSeq)
	return nil
}

func (w *WAL) Sync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	_ = w.writer.Flush()
	_ = w.file.Sync()
	_ = w.file.Close()

	newID := w.segmentID + 1
	newFile := fmt.Sprintf("%06d.wal", newID)
	oldPath := filepath.Join(w.cfg.Dir, "current.wal")
	newPath := filepath.Join(w.cfg.Dir, newFile)
	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	entry := WalIndexEntry{
		File:      newFile,
		FirstSeq:  w.segmentStartSeq,
		LastSeq:   w.seq,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	_ = AppendIndexEntry(w.cfg.Dir, entry)

	fmt.Printf("Finalized WAL → %s (seq %d–%d)\n", newFile, w.segmentStartSeq, w.seq)
	return nil
}
