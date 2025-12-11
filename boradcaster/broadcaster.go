package broadcaster

import (
	"fmt"
	"time"

	"loki/wal/exit"
)

type Broadcaster struct {
	WAL *exit.ExitWAL
}

func NewBroadcaster(w *exit.ExitWAL) *Broadcaster {
	return &Broadcaster{WAL: w}
}

// StartCron starts a periodic job to flush broadcast events.
func (b *Broadcaster) StartCron(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			b.flush()
		}
	}()
}

func (b *Broadcaster) flush() {
	states := b.WAL.All()
	for id, state := range states {
		if state == "ready" {
			// Simulate sending to Kafka or another stream.
			fmt.Printf("[Broadcaster] Sent order %d to Kafka\n", id)
			b.WAL.Update(id, "ack")
		}
	}
}
