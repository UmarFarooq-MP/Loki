package exit

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

type ExitState uint8

const (
	ExitNew ExitState = iota
	ExitSent
	ExitAcked
)

type ExitRecord struct {
	Seq       uint64    `json:"seq"`
	Payload   []byte    `json:"payload"`
	State     ExitState `json:"state"`
	Timestamp int64     `json:"ts"`
}

type ExitWAL struct {
	db *pebble.DB
}

func Open(path string) (*ExitWAL, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &ExitWAL{db: db}, nil
}

func (w *ExitWAL) Close() error {
	return w.db.Close()
}

func key(seq uint64) []byte {
	return []byte(fmt.Sprintf("exit/%020d", seq))
}

// =====================================================
// WRITE PATH
// =====================================================

func (w *ExitWAL) PutNew(seq uint64, payload []byte) error {
	rec := ExitRecord{
		Seq:       seq,
		Payload:   payload,
		State:     ExitNew,
		Timestamp: time.Now().UnixNano(),
	}
	data, _ := json.Marshal(rec)
	return w.db.Set(key(seq), data, pebble.Sync)
}

func (w *ExitWAL) MarkSent(seq uint64) error {
	return w.updateState(seq, ExitSent)
}

func (w *ExitWAL) MarkAcked(seq uint64) error {
	return w.updateState(seq, ExitAcked)
}

func (w *ExitWAL) updateState(seq uint64, st ExitState) error {
	k := key(seq)

	val, closer, err := w.db.Get(k)
	if err != nil {
		return nil // idempotent
	}
	defer closer.Close()

	var rec ExitRecord
	_ = json.Unmarshal(val, &rec)

	if rec.State >= st {
		return nil
	}

	rec.State = st
	data, _ := json.Marshal(rec)
	return w.db.Set(k, data, pebble.Sync)
}

// =====================================================
// SCAN
// =====================================================

func (w *ExitWAL) ScanPending(fn func(*ExitRecord) error) error {
	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("exit/"),
		UpperBound: []byte("exit/~"),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var rec ExitRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			continue
		}
		if rec.State != ExitAcked {
			if err := fn(&rec); err != nil {
				return err
			}
		}
	}
	return nil
}

// =====================================================
// TRUNCATION
// =====================================================

func (w *ExitWAL) TruncateAckedUpTo(seq uint64) error {
	batch := w.db.NewBatch()
	defer batch.Close()

	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("exit/"),
		UpperBound: []byte(fmt.Sprintf("exit/%020d", seq)),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var rec ExitRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			continue
		}
		if rec.State == ExitAcked {
			_ = batch.Delete(iter.Key(), nil)
		}
	}
	return batch.Commit(pebble.Sync)
}
