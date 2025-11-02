package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()

	// --- write phase ---
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	const n = 100
	for i := 0; i < n; i++ {
		rec := &Record{
			Type: RecordPlace,
			Time: time.Now().UnixNano(),
			Data: []byte(fmt.Sprintf("order-%d", i)),
		}
		if err := w.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
		if i%20 == 0 {
			_ = w.Sync()
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// --- replay phase ---
	r, err := OpenReader(dir)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	count := 0
	for r.Next() {
		rec := r.Record()
		if rec.Type != RecordPlace {
			t.Fatalf("unexpected record type: %v", rec.Type)
		}
		count++
	}
	if r.Err() != nil && r.Err().Error() != "EOF" {
		t.Errorf("reader error: %v", r.Err())
	}
	if count != n {
		t.Fatalf("expected %d records, got %d", n, count)
	}
	_ = r.Close()
}

func TestWAL_Rotation(t *testing.T) {
	dir := t.TempDir()

	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	w.file.Truncate(defaultSegmentSize) // simulate full segment
	if err := w.Append(&Record{Type: RecordCancel, Data: []byte("rotate")}); err != nil {
		t.Fatalf("append after rotation: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	files, _ := os.ReadDir(dir)
	if len(files) < 2 {
		t.Fatalf("expected rotated file + current.wal, found %d", len(files))
	}
}

func TestWAL_CRCIntegrity(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec := &Record{
		Type: RecordPlace,
		Time: time.Now().UnixNano(),
		Data: []byte("valid-record"),
	}
	_ = w.Append(rec)
	_ = w.Sync()
	_ = w.Close()

	path := filepath.Join(dir, "current.wal")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// corrupt the first few bytes to break CRC
	_, _ = f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, 4)
	f.Close()

	r, err := OpenReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	ok := r.Next()
	if ok {
		t.Fatal("expected corruption detection, but got record")
	}
	if r.Err() == nil || r.Err().Error() != "wal: crc mismatch" {
		t.Fatalf("expected crc mismatch, got %v", r.Err())
	}
}

func TestDurableBookHooks(t *testing.T) {
	dir := t.TempDir()
	db := NewDurableBook(dir)

	db.LogPlace(0, 100, 10, 1)
	db.LogCancel(0, 1)
	db.Close()

	r, _ := OpenReader(dir)
	foundPlace, foundCancel := false, false
	for r.Next() {
		switch r.Record().Type {
		case RecordPlace:
			foundPlace = true
		case RecordCancel:
			foundCancel = true
		}
	}
	if !foundPlace || !foundCancel {
		t.Fatalf("expected both place and cancel records, got place=%v cancel=%v", foundPlace, foundCancel)
	}
}
