package wal

import (
	"os"
	"path/filepath"
	"sync"
	"time"
)

type WAL interface {
	Append(rec *Record) error
	ReplayFrom(offset int64, fn func(*Record)) error
	Close() error
}

type Config struct {
	Dir             string
	SegmentSize     int64
	SegmentDuration time.Duration
	Serializer      Serializer
	FlushInterval   time.Duration
}

type walImpl struct {
	cfg   Config
	file  *os.File
	mu    sync.Mutex
	bytes int64
	start time.Time
}

func New(cfg Config) (WAL, error) {
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(cfg.Dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w := &walImpl{
		cfg:   cfg,
		file:  f,
		start: time.Now(),
	}
	go w.autoFlush()
	return w, nil
}

func (w *walImpl) Append(rec *Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := w.cfg.Serializer.Encode(rec)
	if err != nil {
		return err
	}
	n, err := w.file.Write(data)
	if err != nil {
		return err
	}
	w.bytes += int64(n)
	if w.bytes > w.cfg.SegmentSize || time.Since(w.start) > w.cfg.SegmentDuration {
		_ = w.rotate()
	}
	return nil
}

func (w *walImpl) ReplayFrom(_ int64, fn func(*Record)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	path := filepath.Join(w.cfg.Dir, "wal.log")
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := w.cfg.Serializer.Decoder(f)
	for {
		rec, err := dec()
		if err != nil {
			break
		}
		fn(rec)
	}
	return nil
}

func (w *walImpl) rotate() error {
	_ = w.file.Close()
	old := filepath.Join(w.cfg.Dir, "wal.log")
	new := filepath.Join(w.cfg.Dir, time.Now().Format("20060102_150405")+".log")
	_ = os.Rename(old, new)
	f, err := os.OpenFile(old, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.file = f
	w.bytes = 0
	w.start = time.Now()
	return nil
}

func (w *walImpl) autoFlush() {
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()
	for range ticker.C {
		w.mu.Lock()
		_ = w.file.Sync()
		w.mu.Unlock()
	}
}

func (w *walImpl) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
