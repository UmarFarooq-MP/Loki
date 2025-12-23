package broadcaster

import (
	"context"
	"log"
	"time"

	exitwal "loki/infra/wal/exit"
)

/*
Broadcaster implements the Outbox Pattern.

It guarantees:
- eventual delivery
- retry safety
- non-blocking matching
*/

type Broadcaster struct {
	wal      *exitwal.ExitWAL
	interval time.Duration
}

func New(wal *exitwal.ExitWAL, interval time.Duration) *Broadcaster {
	return &Broadcaster{
		wal:      wal,
		interval: interval,
	}
}

func (b *Broadcaster) Run(ctx context.Context) {
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	log.Println("[broadcaster] started")

	for {
		select {
		case <-ctx.Done():
			log.Println("[broadcaster] stopped")
			return
		case <-ticker.C:
			b.flush()
		}
	}
}

func (b *Broadcaster) flush() {
	b.process(exitwal.StateNew)
	b.process(exitwal.StateFailed)
}

func (b *Broadcaster) process(state exitwal.ExitState) {
	err := b.wal.ScanByState(state, func(orderID uint64, rec exitwal.ExitRecord) error {
		return b.send(orderID, rec)
	})
	if err != nil {
		log.Printf("[broadcaster] scan %s failed: %v", state, err)
	}
}

func (b *Broadcaster) send(orderID uint64, rec exitwal.ExitRecord) error {
	log.Printf("[broadcaster] sending order %d (retry=%d)", orderID, rec.Retries)

	// ---- Kafka send goes here ----
	// Must be idempotent by key=orderID

	success := true // simulate success

	if success {
		log.Printf("[broadcaster] ACK order %d", orderID)
		return b.wal.UpdateState(orderID, exitwal.StateAcked, rec.Retries)
	}

	log.Printf("[broadcaster] FAILED order %d", orderID)
	return b.wal.UpdateState(orderID, exitwal.StateFailed, rec.Retries+1)
}
