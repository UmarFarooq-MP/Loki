package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const frameHeaderSize = 8

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
		segmentID:       segID,
		segmentStartSeq: seq + 1,
		seq:             seq,
		lastRotationAt:  time.Now(),
	}

	if err := w.recoverCurrentState(); err != nil {
		return nil, err
	}
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}
	w.writer = bufio.NewWriterSize(f, 1<<20)

	return w, nil
}

func (w *CoreWAL) Append(rec *Record) error {
	// TODO:: change sequencer to get updated via request --- update when sequencer is ready
	rec.Seq = w.seq + 1
	data, err := w.cfg.Serializer.Encode(rec)
	if err != nil {
		return err
	}

	//frameHeader = length(4) + CRC(4)
	recordSize := frameHeaderSize + len(data)

	//TODO :: a good update will be if we can say create a file and make it available in advance instead
	// Creating at run time , may be a good R&D to later look into
	if w.shouldRotate(recordSize) {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	w.seq++
	if err := writeFrame(w.writer, data); err != nil {
		return err
	}
	w.bytesWritten += uint64(recordSize)
	return nil
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

func (w *CoreWAL) recoverCurrentState() error {
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		w.bytesWritten = 0
		return nil
	}
	path := filepath.Join(w.cfg.Dir, "current.wal")
	r, err := os.Open(path)
	if err != nil {
		return err
	}
	defer r.Close()
	var (
		validBytes int64
		header     [frameHeaderSize]byte
	)
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if err == io.EOF {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return w.truncateCurrent(validBytes)
			}
			return err
		}
		payloadLen := binary.LittleEndian.Uint32(header[:4])
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
				return w.truncateCurrent(validBytes)
			}
			return err
		}
		checksum := binary.LittleEndian.Uint32(header[4:])
		if crc32.ChecksumIEEE(payload) != checksum {
			return w.truncateCurrent(validBytes)
		}
		rec, err := w.cfg.Serializer.Decode(payload)
		if err != nil {
			return err
		}
		w.seq = rec.Seq
		validBytes += int64(frameHeaderSize + len(payload))
	}
	w.bytesWritten = uint64(validBytes)
	return nil
}

func (w *CoreWAL) truncateCurrent(validBytes int64) error {
	if err := w.file.Truncate(validBytes); err != nil {
		return err
	}
	if _, err := w.file.Seek(validBytes, io.SeekStart); err != nil {
		return err
	}
	w.bytesWritten = uint64(validBytes)
	return nil
}

func writeFrame(wr io.Writer, payload []byte) error {
	var header [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
	if _, err := wr.Write(header[:]); err != nil {
		return err
	}
	_, err := wr.Write(payload)
	return err
}
