package main

import (
	"fmt"
	"path/filepath"
	"time"
)

func main() {
	cfg := WALConfig{
		Dir:             "./wal_data",
		SegmentSize:     8 * 1024 * 1024,   // 8 MB
		SegmentDuration: 10 * time.Minute,  // rotate every 10 min
		Serializer:      ProtoSerializer{}, // or BinarySerializer{}
		FlushInterval:   3 * time.Second,   // auto flush every 3 sec
	}

	walInt, err := NewWALIntegration(cfg)
	if err != nil {
		panic(err)
	}
	defer walInt.Close()

	// Replay previous records since last snapshot
	lastSnapshotSeq := uint64(5000)
	err = walInt.ReplayFromSnapshot(lastSnapshotSeq, func(rec *Record) {
		fmt.Printf("🔁 Replaying record #%d, type=%v\n", rec.Seq, rec.Type)
	})
	if err != nil {
		panic(err)
	}

	// Example appending new records
	for i := 1; i <= 5; i++ {
		rec := &Record{
			Type: RecordPlace,
			Time: time.Now().UnixNano(),
			Data: []byte(fmt.Sprintf("order-%d", i)),
		}
		_ = walInt.AppendRecord(rec)
	}

	fmt.Println("🧾 Done appending records.")
}

// ReplayFrom replays all WAL entries after a given sequence number.
func ReplayFrom(dir string, startSeq uint64, apply func(*Record)) error {
	index, err := LoadAllIndex(dir)
	if err != nil {
		return err
	}
	for _, seg := range index {
		if seg.LastSeq <= startSeq {
			continue
		}
		path := filepath.Join(dir, seg.File)
		r, err := OpenReader(path, BinarySerializer{})
		if err != nil {
			return err
		}
		for r.Next() {
			rec := r.Record()
			if rec.Seq <= startSeq {
				continue
			}
			apply(rec)
		}
		r.Close()
	}
	return nil
}
