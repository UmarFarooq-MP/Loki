package snapshot

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"time"

	"loki/domain/orderbook"
)

type Writer struct {
	Dir string
}

func (w *Writer) Write(seq uint64, book *orderbook.OrderBook) error {
	if err := os.MkdirAll(w.Dir, 0755); err != nil {
		return err
	}

	path := filepath.Join(w.Dir, "snapshot.bin")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	s := Snapshot{
		Seq:     seq,
		Created: time.Now(),
		Orders:  make([]OrderEntry, 0, 1024),
	}

	book.BidsWalk(func(lvl *orderbook.PriceLevel) {
		for o := lvl.Head(); o != nil; o = o.Next() {
			if o.Status == orderbook.Active {
				s.Orders = append(s.Orders, OrderEntry{
					ID: o.ID, Side: int(o.Side),
					Type: int(o.Type), Price: o.Price, Qty: o.Qty,
				})
			}
		}
	})

	book.AsksWalk(func(lvl *orderbook.PriceLevel) {
		for o := lvl.Head(); o != nil; o = o.Next() {
			if o.Status == orderbook.Active {
				s.Orders = append(s.Orders, OrderEntry{
					ID: o.ID, Side: int(o.Side),
					Type: int(o.Type), Price: o.Price, Qty: o.Qty,
				})
			}
		}
	})

	return gob.NewEncoder(f).Encode(&s)
}
