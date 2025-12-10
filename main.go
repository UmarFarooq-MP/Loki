package main

import (
	"encoding/json"
	"fmt"
	"loki/order_book"
	"loki/rbq"
	"loki/snapshots"
	"os"
	"path/filepath"
	"time"

	_ "loki/wal"
)

// Snapshot represents a lightweight persisted view of the current book state.
type Snapshot struct {
	LastSeq uint64                   `json:"last_seq"`
	Bids    map[int64][]OrderSummary `json:"bids"`
	Asks    map[int64][]OrderSummary `json:"asks"`
	Time    time.Time                `json:"time"`
}

// OrderSummary is a small serializable version of an order (for snapshots only).
type OrderSummary struct {
	ID     uint64 `json:"id"`
	Side   string `json:"side"`
	Price  int64  `json:"price"`
	Qty    int64  `json:"qty"`
	Filled int64  `json:"filled"`
	Status string `json:"status"`
}

// -------------------- Snapshotter --------------------

type Snapshotter struct {
	Dir  string
	Book *order_book.OrderBook
}

// SaveSnapshot writes the current order book to a JSON snapshot.
func (s *Snapshotter) SaveSnapshot() error {
	snap := Snapshot{
		LastSeq: s.Book.LastSeq.Load(),
		Bids:    make(map[int64][]OrderSummary),
		Asks:    make(map[int64][]OrderSummary),
		Time:    time.Now(),
	}

	// Iterate all active orders
	s.Book.SnapshotActiveIter(&snapshotter.Reader{}, func(price int64, o *order_book.Order) {
		entry := OrderSummary{
			ID:     o.ID,
			Side:   sideToString(o.Side),
			Price:  price,
			Qty:    o.Qty,
			Filled: o.Filled,
			Status: statusToString(o.Status),
		}
		if o.Side == order_book.Bid {
			snap.Bids[price] = append(snap.Bids[price], entry)
		} else {
			snap.Asks[price] = append(snap.Asks[price], entry)
		}
	})

	path := filepath.Join(s.Dir, fmt.Sprintf("snapshot_%d.json", snap.LastSeq))
	data, _ := json.MarshalIndent(snap, "", "  ")
	return os.WriteFile(path, data, 0o644)
}

// LoadLatestSnapshot finds and loads the newest snapshot file.
func (s *Snapshotter) LoadLatestSnapshot() (*Snapshot, error) {
	files, err := os.ReadDir(s.Dir)
	if err != nil {
		return nil, err
	}

	var latestFile string
	var latestSeq uint64
	for _, f := range files {
		var seq uint64
		n, _ := fmt.Sscanf(f.Name(), "snapshot_%d.json", &seq)
		if n == 1 && seq > latestSeq {
			latestSeq = seq
			latestFile = f.Name()
		}
	}
	if latestFile == "" {
		return nil, nil // no snapshot yet
	}

	path := filepath.Join(s.Dir, latestFile)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

// -------------------- Utilities --------------------

func sideToString(s order_book.Side) string {
	if s == order_book.Bid {
		return "bid"
	}
	return "ask"
}

func statusToString(st order_book.OrderStatus) string {
	if st == order_book.Active {
		return "active"
	}
	return "inactive"
}

// -------------------- Main --------------------

func main() {
	fmt.Println("Starting order book engine with WAL + Snapshotter")

	book := order_book.NewOrderBook()
	defer book.Log.Close()

	snapper := &Snapshotter{Dir: "./snapshots", Book: book}
	_ = os.MkdirAll(snapper.Dir, 0o755)

	// Attempt to load last snapshot
	snap, err := snapper.LoadLatestSnapshot()
	if err != nil {
		panic(fmt.Errorf("failed to load snapshot: %w", err))
	}
	if snap != nil {
		fmt.Printf("Loaded snapshot seq=%d (%s)\n", snap.LastSeq, snap.Time.Format(time.RFC3339))
		book.ReplayFromWAL()
	} else {
		fmt.Println("No snapshot found, building fresh order book")
	}

	//Simulate order flow
	fmt.Println("Placing sample orders...")
	pool := order_book.NewOrderPool(1024)
	ring := rbq.newRetireRing(1024)

	book.placeOrder(order_book.Bid, order_book.Limit, 100, 1, 5, 1, pool, ring)
	book.placeOrder(order_book.Ask, order_book.Limit, 105, 2, 3, 2, pool, ring)
	book.placeOrder(order_book.Bid, order_book.Market, 0, 3, 10, 3, pool, ring)

	//Write snapshot periodically
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := snapper.SaveSnapshot(); err != nil {
				fmt.Println("Snapshot failed:", err)
			} else {
				fmt.Println("Snapshot saved successfully.")
			}
		}
	}()

	//Run engine for demo
	time.Sleep(12 * time.Second)
	fmt.Println("Engine stopped. WAL and snapshots written successfully.")
}
