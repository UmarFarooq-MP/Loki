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

const (
	defaultSegmentSize     = 2 * 1024 * 1024 // 2 MB for testing (increase in prod)
	defaultSegmentDuration = 5 * time.Minute
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

type WAL struct {
	dir             string
	file            *os.File
	writer          *bufio.Writer
	seq             uint64
	segmentID       int
	segmentStartSeq uint64
	bytesWritten    uint64
	maxSize         uint64
	lastRotationAt  time.Time
	ser             BinarySerializer
}

func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	last, _ := LoadLastIndex(dir)
	var segID int
	var seq uint64

	if last != nil {
		id, _ := strconv.Atoi(strings.TrimSuffix(filepath.Base(last.File), ".wal"))
		segID = id
		seq = last.LastSeq
		fmt.Printf("Resuming WAL from segment %06d (seq=%d)\n", segID, seq)
	} else {
		fmt.Println("Starting new WAL (no index found)")
	}

	path := filepath.Join(dir, "current.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		dir:             dir,
		file:            f,
		writer:          bufio.NewWriterSize(f, 1<<20),
		ser:             BinarySerializer{},
		segmentID:       segID,
		segmentStartSeq: seq + 1,
		seq:             seq,
		maxSize:         defaultSegmentSize,
		lastRotationAt:  time.Now(),
	}

	// finalize leftover current.wal if previous run crashed mid-write
	if info, err := os.Stat(path); err == nil && info.Size() > 0 && segID > 0 {
		_ = w.rotate()
	}

	return w, nil
}

func (w *WAL) Append(rec *Record) error {
	data, err := w.ser.Encode(rec)
	if err != nil {
		return err
	}

	if w.shouldRotate(len(data)) {
		//TODO :: prefered to do it on a separate routine
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

// ---------- Rotation trigger ----------
func (w *WAL) shouldRotate(nextSize int) bool {
	if w.bytesWritten+uint64(nextSize) >= w.maxSize {
		return true
	}
	if time.Since(w.lastRotationAt) >= defaultSegmentDuration {
		return true
	}
	return false
}

// ---------- Rotate segment ----------
func (w *WAL) rotate() error {
	_ = w.writer.Flush()
	_ = w.file.Sync()
	_ = w.file.Close()

	newID := w.segmentID + 1
	newFile := fmt.Sprintf("%06d.wal", newID)
	oldPath := filepath.Join(w.dir, "current.wal")
	newPath := filepath.Join(w.dir, newFile)

	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	entry := WalIndexEntry{
		File:      newFile,
		FirstSeq:  w.segmentStartSeq,
		LastSeq:   w.seq,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	_ = AppendIndexEntry(w.dir, entry)

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
	//
	_ = w.writer.Flush()
	_ = w.file.Sync()
	_ = w.file.Close()

	// finalize current.wal into next numbered segment
	newID := w.segmentID + 1
	newFile := fmt.Sprintf("%06d.wal", newID)
	oldPath := filepath.Join(w.dir, "current.wal")
	newPath := filepath.Join(w.dir, newFile)
	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	entry := WalIndexEntry{
		File:      newFile,
		FirstSeq:  w.segmentStartSeq,
		LastSeq:   w.seq,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	_ = AppendIndexEntry(w.dir, entry)

	fmt.Printf("Finalized WAL → %s (seq %d–%d)\n", newFile, w.segmentStartSeq, w.seq)
	return nil
}
