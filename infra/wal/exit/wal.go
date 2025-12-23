package exit

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

// -------------------- State --------------------

type ExitState uint8

const (
	StateNew ExitState = iota
	StateSent
	StateAcked
	StateFailed
)

func (s ExitState) String() string {
	switch s {
	case StateNew:
		return "NEW"
	case StateSent:
		return "SENT"
	case StateAcked:
		return "ACKED"
	case StateFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// -------------------- Record --------------------

type ExitRecord struct {
	State       ExitState
	Retries     uint32
	LastAttempt int64
}

// binary encoding: [state:1][retries:4][lastAttempt:8]
func encodeRecord(r ExitRecord) []byte {
	buf := make([]byte, 1+4+8)
	buf[0] = byte(r.State)
	binary.BigEndian.PutUint32(buf[1:5], r.Retries)
	binary.BigEndian.PutUint64(buf[5:13], uint64(r.LastAttempt))
	return buf
}

func decodeRecord(b []byte) (ExitRecord, error) {
	if len(b) != 13 {
		return ExitRecord{}, errors.New("invalid exit record length")
	}
	return ExitRecord{
		State:       ExitState(b[0]),
		Retries:     binary.BigEndian.Uint32(b[1:5]),
		LastAttempt: int64(binary.BigEndian.Uint64(b[5:13])),
	}, nil
}

// -------------------- WAL --------------------

type ExitWAL struct {
	db *pebble.DB
}

func Open(dir string) (*ExitWAL, error) {
	db, err := pebble.Open(dir, &pebble.Options{
		DisableWAL: false, // we WANT durability
	})
	if err != nil {
		return nil, err
	}
	return &ExitWAL{db: db}, nil
}

func (w *ExitWAL) Close() error {
	return w.db.Close()
}

// -------------------- API --------------------

// PutNew inserts a new outbox entry (called by OrderService).
func (w *ExitWAL) PutNew(orderID uint64) error {
	key := keyFor(orderID)
	rec := ExitRecord{
		State:       StateNew,
		Retries:     0,
		LastAttempt: 0,
	}
	return w.db.Set(key, encodeRecord(rec), pebble.Sync)
}

// UpdateState updates state after send / ack / failure.
func (w *ExitWAL) UpdateState(
	orderID uint64,
	state ExitState,
	retries uint32,
) error {
	key := keyFor(orderID)
	rec := ExitRecord{
		State:       state,
		Retries:     retries,
		LastAttempt: time.Now().UnixNano(),
	}
	return w.db.Set(key, encodeRecord(rec), pebble.Sync)
}

// Delete removes ACKED records (cleanup).
func (w *ExitWAL) Delete(orderID uint64) error {
	return w.db.Delete(keyFor(orderID), pebble.Sync)
}

// Get returns the current record for an order.
func (w *ExitWAL) Get(orderID uint64) (ExitRecord, error) {
	val, closer, err := w.db.Get(keyFor(orderID))
	if err != nil {
		return ExitRecord{}, err
	}
	defer closer.Close()

	return decodeRecord(val)
}

// -------------------- Scan --------------------

// ScanByState iterates all records in the given state.
// This is used by the Broadcaster.
func (w *ExitWAL) ScanByState(
	state ExitState,
	fn func(orderID uint64, rec ExitRecord) error,
) error {
	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("order/"),
		UpperBound: []byte("order/~"),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		val := iter.Value()

		rec, err := decodeRecord(val)
		if err != nil {
			return err
		}

		if rec.State != state {
			continue
		}

		id, err := parseKey(key)
		if err != nil {
			return err
		}

		if err := fn(id, rec); err != nil {
			return err
		}
	}
	return iter.Error()
}

// -------------------- Helpers --------------------

func keyFor(orderID uint64) []byte {
	return []byte(fmt.Sprintf("order/%020d", orderID))
}

func parseKey(b []byte) (uint64, error) {
	var id uint64
	_, err := fmt.Sscanf(string(bytes.TrimPrefix(b, []byte("order/"))), "%d", &id)
	return id, err
}
