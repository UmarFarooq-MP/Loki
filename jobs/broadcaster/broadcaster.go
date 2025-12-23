package broadcaster

import (
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

func New(
	exitWAL *exitwal.ExitWAL,
	brokers []string,
	topic string,
) (*Broadcaster, error) {

	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Producer.Retry.Max = 10
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Return.Successes = true

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

func (b *Broadcaster) Run() {
	log.Println("[broadcaster] started")

	for {
		_ = b.exitWAL.ScanPending(func(rec *exitwal.ExitRecord) error {

			// mark SENT (idempotent)
			_ = b.exitWAL.MarkSent(rec.Seq)

			msg := &sarama.ProducerMessage{
				Topic: b.topic,
				Key:   sarama.StringEncoder(rec.Seq),
				Value: sarama.ByteEncoder(rec.Payload),
			}

			_, _, err := b.producer.SendMessage(msg)
			if err != nil {
				return nil // retry later
			}

			// mark ACKED
			_ = b.exitWAL.MarkAcked(rec.Seq)
			return nil
		})

		time.Sleep(5 * time.Millisecond)
	}
}
