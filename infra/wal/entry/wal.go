package entry

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	Dir             string
	SegmentSize     int64
	SegmentDuration time.Duration
}

type WAL struct {
	dir        string
	segSize    int64
	current    *segment
	segIndex   int
	lastRotate time.Time
}

func Open(cfg Config) (*WAL, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}

	seg, err := openSegment(cfg.Dir, 0)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:        cfg.Dir,
		segSize:    cfg.SegmentSize,
		current:    seg,
		lastRotate: time.Now(),
	}, nil
}

func (w *WAL) Append(r *Record) error {
	payloadLen := uint32(len(r.Data))

	// Frame:
	// [type:1][seq:8][time:8][len:4][payload][crc:4]
	buf := make([]byte, 1+8+8+4+payloadLen+4)

	buf[0] = byte(r.Type)
	binary.BigEndian.PutUint64(buf[1:9], r.Seq)
	binary.BigEndian.PutUint64(buf[9:17], uint64(r.Time))
	binary.BigEndian.PutUint32(buf[17:21], payloadLen)
	copy(buf[21:], r.Data)

	crc := CRC32(buf[:21+payloadLen])
	binary.BigEndian.PutUint32(buf[21+payloadLen:], crc)

	if err := w.current.append(buf); err != nil {
		return err
	}

	if w.current.offset >= w.segSize {
		return w.rotate()
	}
	return nil
}

func (w *WAL) rotate() error {
	_ = w.current.close()
	w.segIndex++

	seg, err := openSegment(w.dir, w.segIndex)
	if err != nil {
		return err
	}

	w.current = seg
	w.lastRotate = time.Now()
	return nil
}

func (w *WAL) TruncateBefore(seq uint64) error {
	files, err := filepath.Glob(filepath.Join(w.dir, "segment-*.wal"))
	if err != nil {
		return err
	}

	for _, path := range files {
		maxSeq, err := maxSeqInSegment(path)
		if err != nil {
			continue
		}
		if maxSeq <= seq {
			_ = os.Remove(path)
		}
	}
	return nil
}
