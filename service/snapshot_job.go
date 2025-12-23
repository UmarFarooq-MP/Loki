package service

import (
	"time"

	"loki/snapshot"
)

func (s *OrderService) StartSnapshotJob(
	dir string,
	interval time.Duration,
) {
	w := &snapshot.Writer{Dir: dir}

	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()

		for range t.C {
			seq := s.seqGen.Current()
			if err := w.Write(seq, s.book); err != nil {
				continue
			}

			// truncate WAL AFTER snapshot
			_ = s.entryWAL.TruncateBefore(seq)
		}
	}()
}
