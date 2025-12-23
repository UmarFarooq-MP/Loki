package snapshot

import "time"

type Snapshot struct {
	Seq     uint64
	Created time.Time
	Orders  []OrderEntry
}

type OrderEntry struct {
	ID    uint64
	Side  int
	Type  int
	Price int64
	Qty   int64
}
