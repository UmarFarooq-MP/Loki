package main

import (
	"fmt"
	"time"
)

// DurableBook is an example “hook layer” for integrating WAL into an OrderBook.
type DurableBook struct {
	WAL *WAL
}

func NewDurableBook(walDir string) *DurableBook {
	w, err := OpenWAL(walDir)
	if err != nil {
		panic(fmt.Errorf("open wal: %w", err))
	}
	return &DurableBook{WAL: w}
}

// LogPlace logs a limit/market order placement.
func (d *DurableBook) LogPlace(side int, price int64, qty uint64, uid uint64) {
	if d.WAL == nil {
		return
	}
	payload := fmt.Sprintf("%d,%d,%d,%d", side, price, qty, uid)
	rec := &Record{
		Type: RecordPlace,
		Time: time.Now().UnixNano(),
		Data: []byte(payload),
	}
	_ = d.WAL.Append(rec)
}

// LogCancel logs order cancellation.
func (d *DurableBook) LogCancel(side int, uid uint64) {
	if d.WAL == nil {
		return
	}
	payload := fmt.Sprintf("%d,%d", side, uid)
	rec := &Record{
		Type: RecordCancel,
		Time: time.Now().UnixNano(),
		Data: []byte(payload),
	}
	_ = d.WAL.Append(rec)
}

// Close closes WAL safely.
func (d *DurableBook) Close() error {
	if d.WAL != nil {
		return d.WAL.Close()
	}
	return nil
}
