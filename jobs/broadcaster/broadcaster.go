package broadcaster

import (
	"context"
	_ "encoding/json"
	"log"
	"time"

	exitwal "loki/infra/wal/exit"

	"github.com/IBM/sarama"
)

type Broadcaster struct {
	exitWAL  *exitwal.ExitWAL
	producer sarama.SyncProducer
	topic    string
}

type Event struct {
	V    int    `json:"v"`
	Type string `json:"type"`
	ID   uint64 `json:"id"`
	Seq  uint64 `json:"seq"`
}

// ------------------------------------------------
// CONSTRUCTOR
// ------------------------------------------------

func New(
	exitWAL *exitwal.ExitWAL,
	brokers []string,
	topic string,
) (*Broadcaster, error) {

	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}

	return &Broadcaster{
		exitWAL:  exitWAL,
		producer: producer,
		topic:    topic,
	}, nil
}

// ------------------------------------------------
// START LOOP
// ------------------------------------------------

func (b *Broadcaster) Start(ctx context.Context) {
	log.Println("[broadcaster] started")

	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				b.replayOnce()
			}
		}
	}()
}

// ------------------------------------------------
// REPLAY LOGIC (CRITICAL)
// ------------------------------------------------

func (b *Broadcaster) replayOnce() {
	_ = b.exitWAL.ScanPending(func(rec *exitwal.ExitRecord) error {

		// 1️⃣ Mark SENT (idempotent)
		_ = b.exitWAL.MarkSent(rec.Seq)

		// 2️⃣ Publish to Kafka
		msg := &sarama.ProducerMessage{
			Topic: b.topic,
			Value: sarama.ByteEncoder(rec.Payload),
		}

		_, _, err := b.producer.SendMessage(msg)
		if err != nil {
			return nil // retry later
		}

		// 3️⃣ Mark ACKED
		_ = b.exitWAL.MarkAcked(rec.Seq)

		return nil
	})
}

// ------------------------------------------------
// SHUTDOWN
// ------------------------------------------------

func (b *Broadcaster) Close() error {
	return b.producer.Close()
}
