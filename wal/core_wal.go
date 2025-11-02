package wal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type CoreWAL struct {
	cfg             WALConfig
	file            *os.File
	writer          *bufio.Writer
	seq             uint64
	segmentID       int
	segmentStartSeq uint64
	bytesWritten    uint64
	lastRotationAt  time.Time
}

type WALConfig struct {
	Dir             string
	SegmentSize     uint64
	SegmentDuration time.Duration
	Serializer      Serializer
	FlushInterval   time.Duration
}

func NewCoreWAL(cfg WALConfig) (*CoreWAL, error) {
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
	}

	path := filepath.Join(cfg.Dir, "current.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	w := &CoreWAL{
		cfg:             cfg,
		file:            f,
		writer:          bufio.NewWriterSize(f, 1<<20),
		segmentID:       segID,
		segmentStartSeq: seq + 1,
		seq:             seq,
		lastRotationAt:  time.Now(),
	}

	return w, nil
}

func (w *CoreWAL) Append(rec *Record) error {
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

func (w *CoreWAL) shouldRotate(nextSize int) bool {
	return w.bytesWritten+uint64(nextSize) >= w.cfg.SegmentSize ||
		time.Since(w.lastRotationAt) >= w.cfg.SegmentDuration
}

func (w *CoreWAL) rotate() error {
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

	f, err := os.OpenFile(oldPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
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

func (w *CoreWAL) Sync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *CoreWAL) Close() error {
	_ = w.writer.Flush()
	_ = w.file.Sync()
	_ = w.file.Close()

	newFile := fmt.Sprintf("%06d.wal", w.segmentID+1)
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
