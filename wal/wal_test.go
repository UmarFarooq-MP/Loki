package wal

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func testConfig(dir string) WALConfig {
	return WALConfig{
		Dir:             dir,
		SegmentSize:     4 * 1024 * 1024,
		SegmentDuration: time.Hour,
		Serializer:      ProtoSerializer{},
	}
}

func TestReplayIncludesCurrentSegment(t *testing.T) {
	dir := t.TempDir()
	core, err := NewCoreWAL(testConfig(dir))
	if err != nil {
		t.Fatalf("create wal: %v", err)
	}
	for i := 0; i < 3; i++ {
		rec := &Record{Type: RecordPlace, Time: int64(i), Data: []byte(fmt.Sprintf("rec-%d", i))}
		if err := core.Append(rec); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := core.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	_ = core.file.Close()

	integ, err := NewWALIntegration(testConfig(dir))
	if err != nil {
		t.Fatalf("restart wal: %v", err)
	}
	defer integ.Close()
	var got []string
	if err := integ.ReplayFromSnapshot(0, func(r *Record) {
		got = append(got, string(r.Data))
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	want := []string{"rec-0", "rec-1", "rec-2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected replay data: got %v want %v", got, want)
	}
}

func TestWALRecoversSequenceAfterCrash(t *testing.T) {
	dir := t.TempDir()
	core, err := NewCoreWAL(testConfig(dir))
	if err != nil {
		t.Fatalf("create wal: %v", err)
	}
	for i := 0; i < 3; i++ {
		rec := &Record{Type: RecordPlace, Time: int64(i), Data: []byte(fmt.Sprintf("run1-%d", i))}
		if err := core.Append(rec); err != nil {
			t.Fatalf("append run1: %v", err)
		}
	}
	if err := core.Sync(); err != nil {
		t.Fatalf("sync run1: %v", err)
	}
	_ = core.file.Close()

	restarted, err := NewCoreWAL(testConfig(dir))
	if err != nil {
		t.Fatalf("restart wal: %v", err)
	}
	for i := 0; i < 2; i++ {
		rec := &Record{Type: RecordPlace, Time: int64(10 + i), Data: []byte(fmt.Sprintf("run2-%d", i))}
		if err := restarted.Append(rec); err != nil {
			t.Fatalf("append run2: %v", err)
		}
	}
	if err := restarted.Sync(); err != nil {
		t.Fatalf("sync run2: %v", err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatalf("close run2: %v", err)
	}

	integ, err := NewWALIntegration(testConfig(dir))
	if err != nil {
		t.Fatalf("final restart: %v", err)
	}
	defer integ.Close()
	var seqs []uint64
	if err := integ.ReplayFromSnapshot(0, func(r *Record) {
		seqs = append(seqs, r.Seq)
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	wantSeq := []uint64{1, 2, 3, 4, 5}
	if !reflect.DeepEqual(seqs, wantSeq) {
		t.Fatalf("unexpected seq list: got %v want %v", seqs, wantSeq)
	}
}
