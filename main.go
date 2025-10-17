package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	dir := "./wal_data"
	_ = os.MkdirAll(dir, 0o755)
	fmt.Println("📂 WAL directory:", dir)

	// --- Create WAL ---
	w, err := OpenWAL(dir)
	if err != nil {
		panic(fmt.Errorf("failed to open WAL: %w", err))
	}

	fmt.Println("🧾 Appending records with large payloads to trigger rotation...")

	for i := 1; i <= 5000; i++ {
		// Create ~128KB payload per record to hit rotation fast
		largePayload := strings.Repeat(fmt.Sprintf("order-%d;", i), 8192)

		rec := &Record{
			Type: RecordPlace,
			Time: time.Now().UnixNano(),
			Data: []byte(largePayload),
		}
		if err := w.Append(rec); err != nil {
			panic(fmt.Errorf("append: %w", err))
		}

		if i%50 == 0 {
			_ = w.Sync()
			fmt.Printf("   ➕ appended %d records (~%.1f MB)\n", i, float64(i)*0.128)
		}
	}

	// TODO :: look in details Sync
	_ = w.Sync()
	_ = w.Close()
	fmt.Println("✅ WAL write complete and segment rotation finished")

	// --- Print WAL index ---
	index, err := LoadAllIndex(dir)
	if err != nil {
		panic(fmt.Errorf("read index: %w", err))
	}

	fmt.Println("\n📘 WAL Index Entries:")
	for _, e := range index {
		b, _ := json.MarshalIndent(e, "  ", "  ")
		fmt.Println("  ", string(b))
	}

	// --- Simulate recovery ---
	lastSeq := uint64(4900) // pretend snapshot covers up to seq=4900
	fmt.Printf("\n🔁 Replaying records > seq=%d ...\n", lastSeq)

	err = ReplayFrom(dir, lastSeq, func(rec *Record) {
		fmt.Printf("  ➕ Replayed Record #%d | size=%d KB\n", rec.Seq, len(rec.Data)/1024)
	})
	if err != nil {
		panic(fmt.Errorf("replay: %w", err))
	}

	fmt.Println("\n✅ Replay complete")
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
