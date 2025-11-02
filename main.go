package main

import (
	"fmt"
	"time"

	"loki/wal"
)

func main() {
	// --- 1️⃣ Configure WAL ---
	cfg := wal.Config{
		Dir:             "./wal_data",          // WAL directory
		SegmentSize:     2 * 1024 * 1024,       // 2 MB before rotation
		SegmentDuration: 1 * time.Minute,       // rotate every 1 minute
		Serializer:      wal.ProtoSerializer{}, // using Protobuf serializer
		FlushInterval:   2 * time.Second,       // auto flush every 2 seconds
	}

	// --- 2️⃣ Create WAL instance ---
	w, err := wal.New(cfg)
	if err != nil {
		panic(fmt.Errorf("failed to open WAL: %w", err))
	}
	defer w.Close()

	fmt.Println("📂 WAL initialized at:", cfg.Dir)

	// --- 3️⃣ Append sample records ---
	for i := 1; i <= 5; i++ {
		rec := &wal.Record{
			Type: wal.RecordPlace,
			Time: time.Now().UnixNano(),
			Data: []byte(fmt.Sprintf("order-%d", i)),
		}
		if err := w.Append(rec); err != nil {
			panic(fmt.Errorf("append: %w", err))
		}
	}

	// --- 4️⃣ Sync to disk for durability ---
	if err := w.Sync(); err != nil {
		panic(fmt.Errorf("sync: %w", err))
	}
	fmt.Println("✅ WAL write complete.")

	// --- 5️⃣ Replay all records (simulate recovery) ---
	fmt.Println("\n🔁 Replaying records:")
	if err := w.ReplayFrom(0, func(r *wal.Record) {
		fmt.Printf("  ➕ Seq=%d | Type=%d | Data=%s\n", r.Seq, r.Type, string(r.Data))
	}); err != nil {
		panic(fmt.Errorf("replay: %w", err))
	}

	fmt.Println("\n🎉 WAL test completed successfully.")
}
