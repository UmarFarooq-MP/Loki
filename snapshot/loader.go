package snapshot

import (
	"encoding/gob"
	"os"

	"loki/domain/orderbook"
	"loki/infra/memory"
)

func Load(
	path string,
	book *orderbook.OrderBook,
	pool *memory.Pool[orderbook.Order],
) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, nil // snapshot optional
	}
	defer f.Close()

	var s Snapshot
	if err := gob.NewDecoder(f).Decode(&s); err != nil {
		return 0, err
	}

	for _, e := range s.Orders {
		o := pool.Get()
		*o = orderbook.Order{
			ID:     e.ID,
			Side:   orderbook.Side(e.Side),
			Type:   orderbook.OrderType(e.Type),
			Price:  e.Price,
			Qty:    e.Qty,
			SeqID:  e.ID,
			Status: orderbook.Active,
		}
		book.Place(o)
	}

	return s.Seq, nil
}
