package entry

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type WAL struct {
	dir             string
	segmentSize     int64
	segmentDuration time.Duration
	current         *segment
	nextIndex       int
	lastRotation    time.Time
}

type Config struct {
	Dir             string
	SegmentSize     int64
	SegmentDuration time.Duration
}

func New(cfg Config) (*WAL, error) {
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, err
	}
	seg, err := openSegment(cfg.Dir, 0)
	if err != nil {
		return nil, err
	}
	return &WAL{
		dir:             cfg.Dir,
		segmentSize:     cfg.SegmentSize,
		segmentDuration: cfg.SegmentDuration,
		current:         seg,
		lastRotation:    time.Now(),
	}, nil
}

func (w *WAL) Append(r *Record) error {
	data := fmt.Sprintf("%d|%d|%s\n", r.Type, r.Time, string(r.Data))
	if err := w.current.append([]byte(data)); err != nil {
		return err
	}
	if w.needsRotation() {
		return w.rotate()
	}
	return nil
}

func (w *WAL) needsRotation() bool {
	return w.current.offset >= w.segmentSize ||
		time.Since(w.lastRotation) > w.segmentDuration
}

func (w *WAL) rotate() error {
	_ = w.current.close()
	w.nextIndex++
	seg, err := openSegment(w.dir, w.nextIndex)
	if err != nil {
		return err
	}
	w.current = seg
	w.lastRotation = time.Now()
	return nil
}

func (w *WAL) ReplayFrom(offset int64, fn func(*Record)) error {
	files, err := filepath.Glob(filepath.Join(w.dir, "*.wal"))
	if err != nil {
		return err
	}
	for _, path := range files {
		data, _ := os.ReadFile(path)
		lines := string(data)
		for _, line := range SplitLines(lines) {
			if line == "" {
				continue
			}
			var rt int
			var ts int64
			var payload string
			_, _ = fmt.Sscanf(line, "%d|%d|%s", &rt, &ts, &payload)
			fn(&Record{Type: RecordType(rt), Time: ts, Data: []byte(payload)})
		}
	}
	return nil
}

func SplitLines(s string) []string {
	out := []string{}
	start := 0
	for i, c := range s {
		if c == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
